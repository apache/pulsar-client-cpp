/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "StProducerImpl.h"

#include <pulsar/MessageBuilder.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "MessageIdImpl.h"
#include "lib/ExecutorService.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar::st {

StProducerImpl::StProducerImpl(pulsar::ClientImplPtr classic, ProducerConfig config)
    : classic_(std::move(classic)),
      config_(std::move(config)),
      topic_(config_.topic),
      producerName_(config_.producerName.value_or(std::string{})),
      currentLayout_(std::make_shared<SegmentLayout>()) {}

Future<void> StProducerImpl::start() {
    dagWatch_ = std::make_shared<DagWatchSession>(classic_, config_.topic, /*createIfMissing*/ true);
    std::weak_ptr<StProducerImpl> weak = weak_from_this();
    // Register the layout listener BEFORE start() so the very first layout is delivered to it.
    dagWatch_->setLayoutChangeListener(
        [weak](const SegmentLayout& newLayout, const SegmentLayout& oldLayout) {
            if (auto self = weak.lock()) self->onLayoutChange(newLayout, oldLayout);
        });
    dagWatch_->start().addListener([weak](const Expected<SegmentLayout>& result) {
        if (auto self = weak.lock()) self->onStartResult(result);
    });
    return startPromise_.getFuture();
}

void StProducerImpl::onStartResult(const Expected<SegmentLayout>& result) {
    // Only the failure path is handled here: the DagWatchSession fails start()'s future (without
    // invoking the layout listener) when the lookup fails before the first layout arrives. On
    // success the listener runs immediately after and drives completeStart().
    if (!result) {
        startPromise_.setError(result.error());
    }
}

void StProducerImpl::onLayoutChange(const SegmentLayout& newLayout, const SegmentLayout& /*oldLayout*/) {
    std::vector<Future<pulsar::Producer>> retired;
    bool first = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        first = !sawFirstLayout_;
        sawFirstLayout_ = true;
        currentLayout_ = std::make_shared<SegmentLayout>(newLayout);

        std::unordered_set<std::uint64_t> active;
        for (const auto& segment : newLayout.activeSegments()) active.insert(segment.segmentId);
        for (auto it = segmentProducers_.begin(); it != segmentProducers_.end();) {
            if (active.find(it->first) == active.end()) {
                retired.push_back(std::move(it->second));
                it = segmentProducers_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Close producers for segments that left the layout, fire-and-forget.
    for (auto& future : retired) {
        future.addListener([](const Expected<pulsar::Producer>& result) {
            if (result) {
                pulsar::Producer producer = *result;
                producer.closeAsync([](pulsar::Result) {});
            }
        });
    }

    if (first) {
        completeStart();
    } else if (requiresExclusiveAttach(config_.accessMode)) {
        // Exclusive modes claim newly-active segments eagerly (best-effort; failures are logged).
        for (const auto& segment : newLayout.activeSegments()) {
            getOrCreateSegmentProducerAsync(segment.segmentId);
        }
    }
}

void StProducerImpl::completeStart() {
    if (!requiresExclusiveAttach(config_.accessMode)) {
        startPromise_.setSuccess();  // Shared: segment producers are created lazily on first send.
        return;
    }
    auto layout = snapshotLayout();
    const auto& segments = layout->activeSegments();
    if (segments.empty()) {
        startPromise_.setSuccess();
        return;
    }
    // Exclusive: eagerly attach every active segment; the create fails up front if the exclusive
    // claim is refused, exactly like the Java strict eager attach.
    auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(segments.size()));
    for (const auto& segment : segments) {
        getOrCreateSegmentProducerAsync(segment.segmentId)
            .addListener([self = shared_from_this(), remaining](const Expected<pulsar::Producer>& result) {
                if (!result) {
                    self->startPromise_.setError(
                        result.error());  // first error wins (complete is idempotent)
                    return;
                }
                if (remaining->fetch_sub(1) == 1) self->startPromise_.setSuccess();
            });
    }
}

pulsar::ProducerConfiguration StProducerImpl::buildSegmentConfiguration(const Segment& segment) const {
    // Build a FRESH config every time: pulsar::ProducerConfiguration's copy constructor shares its
    // impl, so copy-then-mutate would clobber every other segment's config.
    pulsar::ProducerConfiguration conf;
    conf.setSchema(config_.schema);
    conf.setAccessMode(toClassicAccessMode(config_.accessMode));
    if (config_.sendTimeoutMs) conf.setSendTimeout(static_cast<int>(*config_.sendTimeoutMs));
    conf.setBlockIfQueueFull(config_.blockIfQueueFull);
    if (config_.initialSequenceId) conf.setInitialSequenceId(*config_.initialSequenceId);
    for (const auto& [key, value] : config_.properties) conf.setProperty(key, value);
    if (config_.producerName) {
        conf.setProducerName(*config_.producerName + "-seg-" + std::to_string(segment.segmentId));
    }
    // Legacy segments wrap an externally-managed persistent:// topic; mark the connection so the
    // regular->scalable migration precheck (PIP-475) recognizes it. Real segment:// topics aren't.
    if (segment.isLegacy()) {
        conf.setProperty("__pulsar.v5.managed", "true");
    }
    // PIP-486 entry-bucketing: only the encryption-disables-batching branch is portable today; the
    // EntryBucketBatcherBuilder branch has no C++ seam yet (TODO when a batcher-builder lands).
    if (conf.isEncryptionEnabled()) {
        conf.setBatchingEnabled(false);
    }
    return conf;
}

Future<pulsar::Producer> StProducerImpl::getOrCreateSegmentProducerAsync(std::uint64_t segmentId) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (auto it = segmentProducers_.find(segmentId); it != segmentProducers_.end()) {
        return it->second;  // concurrent cold senders share the one creation attempt
    }

    const Segment* segment = nullptr;
    for (const auto& candidate : currentLayout_->activeSegments()) {
        if (candidate.segmentId == segmentId) {
            segment = &candidate;
            break;
        }
    }
    detail::Promise<pulsar::Producer> promise;
    if (segment == nullptr) {
        // Transient — don't cache, so a later routing attempt re-resolves against a fresh layout.
        promise.setError(Error{ResultUnknownError,
                               "segment " + std::to_string(segmentId) + " is not in the active layout"});
        return promise.getFuture();
    }

    pulsar::ProducerConfiguration conf = buildSegmentConfiguration(*segment);
    const std::string attachTopic = segment->attachTopicName();
    std::optional<std::string> assignedBrokerUrl;
    if (const std::string* url = currentLayout_->brokerUrl(segmentId)) {
        assignedBrokerUrl = *url;  // pin to the DAG-provided owner broker
    }

    auto future = promise.getFuture();
    segmentProducers_.insert_or_assign(segmentId, future);
    classic_->createSegmentProducerAsync(
        attachTopic, conf,
        [promise](std::variant<pulsar::Error, pulsar::Producer> result) {
            if (auto* producer = std::get_if<pulsar::Producer>(&result)) {
                promise.setValue(std::move(*producer));
            } else {
                promise.setError(std::get<pulsar::Error>(result));
            }
        },
        assignedBrokerUrl);
    return future;
}

Future<MessageId> StProducerImpl::sendAsync(OutgoingMessage message) {
    detail::Promise<MessageId> userPromise;
    auto userFuture = userPromise.getFuture();

    if (closed_.load()) {
        userPromise.setError(Error{ResultAlreadyClosed, "producer is closed"});
        return userFuture;
    }
    if (message.transaction) {
        userPromise.setError(Error{ResultOperationNotSupported,
                                   "transactions are not implemented yet in the scalable-topics client"});
        return userFuture;
    }

    // Register in the in-flight set (for flush) BEFORE dispatching: a warm send can complete
    // synchronously, which would otherwise run the removal listener before insertion.
    std::uint64_t inFlightId = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        inFlightId = nextInFlightId_++;
        inFlight_.insert_or_assign(inFlightId, userFuture);
    }
    std::weak_ptr<StProducerImpl> weak = weak_from_this();
    userFuture.addListener([weak, inFlightId](const Expected<MessageId>&) {
        if (auto self = weak.lock()) {
            std::lock_guard<std::mutex> lock(self->mutex_);
            self->inFlight_.erase(inFlightId);
        }
    });

    auto layout = snapshotLayout();
    Expected<std::uint64_t> route =
        message.key.has_value() ? router_.route(*message.key, *layout) : router_.routeRoundRobin(*layout);
    if (!route) {
        // Route failure (no active segment yet / uncovered hash) is terminal, not retried.
        userPromise.setError(route.error());
        return userFuture;
    }
    dispatchSend(std::move(message), *route, /*attempt*/ 0, userPromise);
    return userFuture;
}

void StProducerImpl::dispatchSend(OutgoingMessage message, std::uint64_t segmentId, int attempt,
                                  detail::Promise<MessageId> userPromise) {
    auto self = shared_from_this();
    getOrCreateSegmentProducerAsync(segmentId).addListener(
        [self, message = std::move(message), segmentId, attempt,
         userPromise](const Expected<pulsar::Producer>& created) mutable {
            if (!created) {
                self->handleSegmentFailure(created.error(), std::move(message), segmentId, attempt,
                                           userPromise);
                return;
            }
            pulsar::Producer producer = *created;
            pulsar::Message classicMessage = self->buildClassicMessage(message);
            producer.sendAsync(
                classicMessage, [self, message = std::move(message), segmentId, attempt, userPromise](
                                    pulsar::Result result, const pulsar::MessageId& v4Id) mutable {
                    if (result == pulsar::ResultOk) {
                        userPromise.setValue(
                            MessageIdFactory::create(v4Id, static_cast<std::int64_t>(segmentId)));
                    } else {
                        self->handleSegmentFailure(Error{result, ""}, std::move(message), segmentId, attempt,
                                                   userPromise);
                    }
                });
        });
}

void StProducerImpl::handleSegmentFailure(Error error, OutgoingMessage message, std::uint64_t segmentId,
                                          int attempt, detail::Promise<MessageId> userPromise) {
    if (!isSegmentGoneError(error.result, error.message) || attempt >= kSendRetryMaxAttempts ||
        closed_.load()) {
        userPromise.setError(std::move(error));
        return;
    }
    // The segment was sealed/terminated under us. Drop its cached producer so the retry recreates it
    // against the layout the DAG watch is about to deliver, then re-route after a short backoff.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        segmentProducers_.erase(segmentId);
    }
    LOG_INFO("[" << topic_ << "] segment " << segmentId << " is gone; retrying send, attempt "
                 << (attempt + 1) << " of " << kSendRetryMaxAttempts);
    auto timer = classic_->getIOExecutorProvider()->get()->createDeadlineTimer();
    const std::int64_t delayMs = std::min<std::int64_t>(100 * (attempt + 1), kSendRetryMaxBackoffMs);
    timer->expires_from_now(std::chrono::milliseconds(delayMs));
    auto self = shared_from_this();
    timer->async_wait([self, message = std::move(message), attempt, userPromise,
                       timer](const ASIO_ERROR& ec) mutable {
        if (ec || self->closed_.load()) {
            userPromise.setError(Error{ResultAlreadyClosed, "producer closed during send retry"});
            return;
        }
        auto layout = self->snapshotLayout();
        Expected<std::uint64_t> route = message.key.has_value() ? self->router_.route(*message.key, *layout)
                                                                : self->router_.routeRoundRobin(*layout);
        if (!route) {
            userPromise.setError(route.error());
            return;
        }
        self->dispatchSend(std::move(message), *route, attempt + 1, userPromise);
    });
}

pulsar::Message StProducerImpl::buildClassicMessage(const OutgoingMessage& message) const {
    pulsar::MessageBuilder builder;
    // The classic client copies the content synchronously, so publishing the borrowed view here
    // still honors the zero-copy (BytesView) lifetime contract.
    if (message.usesView) {
        builder.setContent(message.payloadView.data(), message.payloadView.size());
    } else {
        builder.setContent(message.payload.data(), message.payload.size());
    }
    if (message.key) builder.setPartitionKey(*message.key);
    for (const auto& [key, value] : message.properties) builder.setProperty(key, value);
    if (message.eventTime) {
        builder.setEventTimestamp(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(message.eventTime->time_since_epoch())
                .count()));
    }
    if (message.sequenceId) builder.setSequenceId(*message.sequenceId);
    if (message.deliverAt) {
        builder.setDeliverAt(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(message.deliverAt->time_since_epoch())
                .count()));
    }
    if (!message.replicationClusters.empty()) builder.setReplicationClusters(message.replicationClusters);
    return builder.build();
}

std::optional<std::int64_t> StProducerImpl::lastSequenceId() const {
    std::int64_t max = config_.initialSequenceId.value_or(-1);
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [segmentId, future] : segmentProducers_) {
        if (future.isReady()) {
            auto result = future.get();
            if (result) {
                max = std::max(max, result->getLastSequenceId());
            }
        }
    }
    return max < 0 ? std::nullopt : std::optional<std::int64_t>(max);
}

Future<void> StProducerImpl::flushAsync() {
    std::vector<Future<MessageId>> pending;
    std::vector<Future<pulsar::Producer>> producers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending.reserve(inFlight_.size());
        for (const auto& [id, future] : inFlight_) pending.push_back(future);
        producers.reserve(segmentProducers_.size());
        for (const auto& [id, future] : segmentProducers_) producers.push_back(future);
    }

    detail::Promise<void> promise;
    // +1 base count so an empty snapshot completes immediately.
    auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(pending.size()) + 1);
    auto reportedError = std::make_shared<std::atomic<bool>>(false);
    auto finishOne = [promise, remaining]() {
        if (remaining->fetch_sub(1) == 1) promise.setSuccess();  // no-op if already errored
    };

    // Nudge the ready classic producers so any batched sends complete; the actual completion is
    // tracked by the in-flight user futures below.
    for (auto& producerFuture : producers) {
        if (producerFuture.isReady()) {
            auto result = producerFuture.get();
            if (result) {
                pulsar::Producer producer = *result;
                producer.flushAsync([](pulsar::Result) {});
            }
        }
    }
    for (auto& future : pending) {
        future.addListener([promise, reportedError, finishOne](const Expected<MessageId>& result) {
            if (!result && !reportedError->exchange(true)) promise.setError(result.error());
            finishOne();
        });
    }
    finishOne();
    return promise.getFuture();
}

Future<void> StProducerImpl::closeAsync() {
    if (closed_.exchange(true)) {
        detail::Promise<void> promise;
        promise.setSuccess();  // idempotent: already closed
        return promise.getFuture();
    }
    if (dagWatch_) dagWatch_->close();

    std::vector<Future<pulsar::Producer>> producers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        producers.reserve(segmentProducers_.size());
        for (auto& [id, future] : segmentProducers_) producers.push_back(future);
        segmentProducers_.clear();
    }

    detail::Promise<void> promise;
    auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(producers.size()) + 1);
    auto finishOne = [promise, remaining]() {
        if (remaining->fetch_sub(1) == 1) promise.setSuccess();
    };
    for (auto& producerFuture : producers) {
        producerFuture.addListener([finishOne](const Expected<pulsar::Producer>& result) {
            if (result) {
                pulsar::Producer producer = *result;
                producer.closeAsync(
                    [finishOne](pulsar::Result) { finishOne(); });  // swallow per-producer errors
            } else {
                finishOne();  // creation itself failed — nothing to close
            }
        });
    }
    finishOne();
    return promise.getFuture();
}

std::shared_ptr<const SegmentLayout> StProducerImpl::snapshotLayout() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return currentLayout_;
}

bool StProducerImpl::isSegmentGoneError(pulsar::Result result, const std::string& message) {
    if (result == pulsar::ResultTopicTerminated || result == pulsar::ResultAlreadyClosed) return true;
    // Some paths surface a sealed segment as a generic error carrying the terminated text.
    return message.find("TopicTerminated") != std::string::npos ||
           message.find("terminated") != std::string::npos;
}

bool StProducerImpl::requiresExclusiveAttach(ProducerAccessMode mode) {
    return mode != ProducerAccessMode::Shared;
}

pulsar::ProducerConfiguration::ProducerAccessMode StProducerImpl::toClassicAccessMode(
    ProducerAccessMode mode) {
    // The st and classic enums do NOT share numeric values — map by name.
    switch (mode) {
        case ProducerAccessMode::Shared:
            return pulsar::ProducerConfiguration::Shared;
        case ProducerAccessMode::Exclusive:
            return pulsar::ProducerConfiguration::Exclusive;
        case ProducerAccessMode::ExclusiveWithFencing:
            return pulsar::ProducerConfiguration::ExclusiveWithFencing;
        case ProducerAccessMode::WaitForExclusive:
            return pulsar::ProducerConfiguration::WaitForExclusive;
    }
    return pulsar::ProducerConfiguration::Shared;
}

}  // namespace pulsar::st

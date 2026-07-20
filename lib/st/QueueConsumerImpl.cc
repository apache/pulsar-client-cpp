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
#include "QueueConsumerImpl.h"

#include <cstdint>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "MessageIdImpl.h"
#include "MessageImpl.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar::st {

namespace {

pulsar::InitialPosition toClassicInitialPosition(SubscriptionInitialPosition position) {
    return position == SubscriptionInitialPosition::Earliest ? pulsar::InitialPositionEarliest
                                                             : pulsar::InitialPositionLatest;
}

}  // namespace

QueueConsumerImpl::QueueConsumerImpl(pulsar::ClientImplPtr classic, QueueConsumerConfig config)
    : classic_(std::move(classic)),
      config_(std::move(config)),
      topic_(config_.topic),
      subscription_(config_.subscriptionName),
      consumerName_(config_.consumerName.value_or(std::string{})),
      receiveQueue_(
          std::make_shared<ReceiveQueue>(classic_->getIOExecutorProvider()->get(), kReceiveQueueCapacity)),
      currentLayout_(std::make_shared<SegmentLayout>()) {}

Future<void> QueueConsumerImpl::start() {
    dagWatch_ = std::make_shared<DagWatchSession>(classic_, config_.topic, /*createIfMissing*/ true);
    std::weak_ptr<QueueConsumerImpl> weak = weak_from_this();
    dagWatch_->setLayoutChangeListener(
        [weak](const SegmentLayout& newLayout, const SegmentLayout& oldLayout) {
            if (auto self = weak.lock()) self->onLayoutChange(newLayout, oldLayout);
        });
    dagWatch_->start().addListener([weak](const Expected<SegmentLayout>& result) {
        if (auto self = weak.lock()) self->onStartResult(result);
    });
    return startPromise_.getFuture();
}

void QueueConsumerImpl::onStartResult(const Expected<SegmentLayout>& result) {
    // Only the failure path (see StProducerImpl::onStartResult): the layout listener drives the
    // success path — subscribe the initial segments and complete startPromise_.
    if (!result) startPromise_.setError(result.error());
}

void QueueConsumerImpl::onLayoutChange(const SegmentLayout& newLayout, const SegmentLayout& /*oldLayout*/) {
    // Subscribe active AND sealed segments: a sealed segment may still hold undrained messages.
    std::vector<Segment> target;
    target.reserve(newLayout.activeSegments().size() + newLayout.sealedSegments().size());
    for (const auto& segment : newLayout.activeSegments()) target.push_back(segment);
    for (const auto& segment : newLayout.sealedSegments()) target.push_back(segment);

    std::vector<Future<pulsar::Consumer>> retired;
    std::vector<Segment> toAdd;
    bool first = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        first = !sawFirstLayout_;
        sawFirstLayout_ = true;
        currentLayout_ = std::make_shared<SegmentLayout>(newLayout);

        std::unordered_set<std::uint64_t> targetIds;
        for (const auto& segment : target) targetIds.insert(segment.segmentId);
        for (auto it = segmentConsumers_.begin(); it != segmentConsumers_.end();) {
            if (targetIds.find(it->first) == targetIds.end()) {
                retired.push_back(std::move(it->second));
                it = segmentConsumers_.erase(it);
            } else {
                ++it;
            }
        }
        // Forget segments that have left the DAG so a future segment id can never be mistaken for a
        // previously-drained one.
        for (auto it = drainedSegments_.begin(); it != drainedSegments_.end();) {
            it = targetIds.count(*it) ? std::next(it) : drainedSegments_.erase(it);
        }
        for (const auto& segment : target) {
            if (segmentConsumers_.find(segment.segmentId) == segmentConsumers_.end() &&
                drainedSegments_.find(segment.segmentId) == drainedSegments_.end())
                toAdd.push_back(segment);
        }
    }

    for (auto& future : retired) {
        future.addListener([](const Expected<pulsar::Consumer>& result) {
            if (result) {
                pulsar::Consumer consumer = *result;
                consumer.closeAsync([](pulsar::Result) {});
            }
        });
    }

    if (first) {
        if (toAdd.empty()) {
            startPromise_.setSuccess();
            return;
        }
        auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(toAdd.size()));
        for (const auto& segment : toAdd) {
            getOrCreateSegmentConsumerAsync(segment).addListener(
                [self = shared_from_this(), remaining](const Expected<pulsar::Consumer>& result) {
                    if (!result) {
                        self->startPromise_.setError(result.error());  // first error wins (idempotent)
                        return;
                    }
                    if (remaining->fetch_sub(1) == 1) self->startPromise_.setSuccess();
                });
        }
    } else {
        for (const auto& segment : toAdd) getOrCreateSegmentConsumerAsync(segment);  // best-effort
    }
}

pulsar::ConsumerConfiguration QueueConsumerImpl::buildSegmentConfiguration(const Segment& segment) const {
    // Build a FRESH config every time (pulsar::ConsumerConfiguration's copy ctor shares its impl).
    pulsar::ConsumerConfiguration conf;
    conf.setConsumerType(pulsar::ConsumerShared);
    conf.setSchema(config_.schema);
    conf.setSubscriptionInitialPosition(toClassicInitialPosition(config_.initialPosition));
    if (config_.consumerName) {
        conf.setConsumerName(*config_.consumerName + "-seg-" + std::to_string(segment.segmentId));
    }
    if (config_.ackPolicy.groupTime) {
        conf.setAckGroupingTimeMs(static_cast<long>(config_.ackPolicy.groupTime->count()));
    }
    if (config_.ackPolicy.negativeAckRedeliveryDelay) {
        conf.setNegativeAckRedeliveryDelayMs(
            static_cast<long>(config_.ackPolicy.negativeAckRedeliveryDelay->count()));
    }
    for (const auto& [key, value] : config_.properties) conf.setProperty(key, value);
    if (segment.isLegacy()) conf.setProperty("__pulsar.v5.managed", "true");
    return conf;
}

Future<pulsar::Consumer> QueueConsumerImpl::getOrCreateSegmentConsumerAsync(const Segment& segment) {
    detail::Promise<pulsar::Consumer> promise;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto it = segmentConsumers_.find(segment.segmentId); it != segmentConsumers_.end()) {
            return it->second;
        }
        segmentConsumers_.insert_or_assign(segment.segmentId, promise.getFuture());
    }

    const pulsar::ConsumerConfiguration conf = buildSegmentConfiguration(segment);
    const std::string attachTopic = segment.attachTopicName();
    const std::uint64_t segmentId = segment.segmentId;
    auto self = shared_from_this();
    classic_->subscribeSegmentAsync(
        attachTopic, config_.subscriptionName, conf,
        [self, promise, segmentId](std::variant<pulsar::Error, pulsar::Consumer> result) {
            if (auto* consumer = std::get_if<pulsar::Consumer>(&result)) {
                // pulsar::Consumer is a copyable handle (its virtual dtor suppresses the move ctor),
                // so this is a shared-impl copy, not a deep copy.
                pulsar::Consumer c = *consumer;
                self->startReceiveLoop(c, segmentId);
                promise.setValue(c);
            } else {
                // Evict the failed subscribe so a later reconcile retries this segment.
                {
                    std::lock_guard<std::mutex> lock(self->mutex_);
                    self->segmentConsumers_.erase(segmentId);
                }
                promise.setError(std::get<pulsar::Error>(result));
            }
        });
    return promise.getFuture();
}

void QueueConsumerImpl::startReceiveLoop(pulsar::Consumer consumer, std::uint64_t segmentId) {
    if (closed_.load()) return;
    auto self = shared_from_this();
    consumer.receiveAsync([self, consumer, segmentId](pulsar::Result result, const pulsar::Message& message) {
        if (result != pulsar::ResultOk) {
            if (result == pulsar::ResultTopicTerminated) {
                // A sealed segment fully drained: close its consumer, drop it from the cache, and
                // remember it drained so a later reconcile does not re-subscribe the still-in-DAG
                // sealed segment (which would redeliver its unacked messages as duplicates).
                {
                    std::lock_guard<std::mutex> lock(self->mutex_);
                    self->segmentConsumers_.erase(segmentId);
                    self->drainedSegments_.insert(segmentId);
                }
                pulsar::Consumer done = consumer;
                done.closeAsync([](pulsar::Result) {});
            }
            // Otherwise (AlreadyClosed / consumer closing) just stop the loop.
            return;
        }
        MessageId id = MessageIdFactory::create(message.getMessageId(), static_cast<std::int64_t>(segmentId));
        auto messageImpl = std::make_shared<MessageImpl>(message, std::move(id));
        // Re-arm only once the fan-in queue has room, so a slow consumer throttles this segment.
        self->receiveQueue_->offer(std::move(messageImpl))
            .addListener([self, consumer, segmentId](const Expected<void>&) {
                self->startReceiveLoop(consumer, segmentId);
            });
    });
}

Future<MessageImplPtr> QueueConsumerImpl::receiveAsync() { return receiveQueue_->receiveAsync(); }

Future<MessageImplPtr> QueueConsumerImpl::receiveAsync(std::chrono::milliseconds timeout) {
    return receiveQueue_->receiveAsync(timeout);
}

Future<pulsar::Consumer> QueueConsumerImpl::segmentConsumerFor(const MessageId& id) const {
    const auto& impl = MessageIdFactory::impl(id);
    if (impl) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto it = segmentConsumers_.find(static_cast<std::uint64_t>(impl->segmentId));
            it != segmentConsumers_.end()) {
            return it->second;
        }
    }
    detail::Promise<pulsar::Consumer> promise;
    promise.setError(Error{ResultUnknownError, "no consumer for the message's segment"});
    return promise.getFuture();
}

void QueueConsumerImpl::acknowledge(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    if (!impl) return;
    const pulsar::MessageId v4 = impl->v4MessageId;
    segmentConsumerFor(id).addListener([v4](const Expected<pulsar::Consumer>& result) {
        if (result) {
            pulsar::Consumer consumer = *result;
            consumer.acknowledgeAsync(v4, [](pulsar::Result) {});
        }
    });
}

void QueueConsumerImpl::acknowledge(const MessageId& /*id*/, const Transaction& /*txn*/) {
    // Transactions are not implemented yet in the scalable-topics client, and an ack is
    // fire-and-forget void (no error channel). Drop it — the message is simply redelivered.
    LOG_WARN("[" << topic_ << "] transactional acknowledge is not implemented yet; dropping the ack");
}

void QueueConsumerImpl::negativeAcknowledge(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    if (!impl) return;
    const pulsar::MessageId v4 = impl->v4MessageId;
    segmentConsumerFor(id).addListener([v4](const Expected<pulsar::Consumer>& result) {
        if (result) {
            pulsar::Consumer consumer = *result;
            consumer.negativeAcknowledge(v4);
        }
    });
}

Future<void> QueueConsumerImpl::closeAsync() {
    if (closed_.exchange(true)) {
        detail::Promise<void> promise;
        promise.setSuccess();  // idempotent
        return promise.getFuture();
    }
    if (dagWatch_) dagWatch_->close();
    if (receiveQueue_) receiveQueue_->close();  // fail pending receives

    std::vector<Future<pulsar::Consumer>> consumers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        consumers.reserve(segmentConsumers_.size());
        for (auto& [segmentId, future] : segmentConsumers_) consumers.push_back(future);
        segmentConsumers_.clear();
    }

    detail::Promise<void> promise;
    auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(consumers.size()) + 1);
    auto finishOne = [promise, remaining]() {
        if (remaining->fetch_sub(1) == 1) promise.setSuccess();
    };
    for (auto& future : consumers) {
        future.addListener([finishOne](const Expected<pulsar::Consumer>& result) {
            if (result) {
                pulsar::Consumer consumer = *result;
                consumer.closeAsync([finishOne](pulsar::Result) { finishOne(); });  // swallow errors
            } else {
                finishOne();
            }
        });
    }
    finishOne();
    return promise.getFuture();
}

}  // namespace pulsar::st

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
#include "StreamConsumerImpl.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <random>
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

// Close a segment consumer once its creation future resolves (a no-op if creation failed).
void closeWhenReady(Future<pulsar::Consumer> future) {
    future.addListener([](const Expected<pulsar::Consumer>& result) {
        if (result) {
            pulsar::Consumer consumer = *result;
            consumer.closeAsync([](pulsar::Result) {});
        }
    });
}

// The consumer name is the controller's registration key, so one is always needed:
// default to "v5-stream-" + 8 random hex chars when unset (Java parity).
std::string defaultConsumerName() {
    static const char kHex[] = "0123456789abcdef";
    std::random_device rd;
    std::string suffix(8, '0');
    for (auto& c : suffix) {
        c = kHex[rd() % 16];
    }
    return "v5-stream-" + suffix;
}

// Rebalance handoffs collide briefly by design: the previous Exclusive owner has not
// released the segment yet (ConsumerBusy), or sticky ranges still overlap
// (ConsumerAssignError). Only these are worth retrying; anything else is a real
// error that retrying would only hide.
bool isRebalanceCollision(Result result) {
    return result == ResultConsumerBusy || result == ResultConsumerAssignError;
}

}  // namespace

StreamConsumerImpl::StreamConsumerImpl(pulsar::ClientImplPtr classic, StreamConsumerConfig config)
    : classic_(std::move(classic)),
      config_(std::move(config)),
      topic_(config_.topic),
      subscription_(config_.subscriptionName),
      consumerName_(config_.consumerName.value_or(defaultConsumerName())),
      executor_(classic_->getIOExecutorProvider()->get()),
      receiveQueue_(std::make_shared<ReceiveQueue>(executor_, kReceiveQueueCapacity)) {}

Future<void> StreamConsumerImpl::start() {
    if (config_.useNamespace) {
        startPromise_.setError(Error{ResultOperationNotSupported,
                                     "namespace-mode subscriptions are not implemented yet in the "
                                     "scalable-topics client"});
        return startPromise_.getFuture();
    }
    session_ =
        std::make_shared<ConsumerAssignmentSession>(classic_, config_.topic, config_.subscriptionName,
                                                    consumerName_, pulsar::ScalableConsumerType_STREAM);
    // Same order as the Java client: subscribe the initial assignment from start()'s
    // own result, and register the update listener only afterwards — so nothing but
    // the initial subscribes can complete startPromise_, and their failures (or the
    // PIP-486 gate) reach the caller.
    std::weak_ptr<StreamConsumerImpl> weak = weak_from_this();
    session_->start().addListener([weak](const Expected<std::vector<AssignedSegment>>& result) {
        auto self = weak.lock();
        if (!self) return;
        if (!result) {
            self->startPromise_.setError(result.error());
            return;
        }
        self->applyInitialAssignment(*result);
    });
    return startPromise_.getFuture();
}

void StreamConsumerImpl::applyInitialAssignment(const std::vector<AssignedSegment>& segments) {
    if (closed_.load()) {
        startPromise_.setError(Error{ResultAlreadyClosed, "consumer is closed"});
        return;
    }
    for (const auto& assigned : segments) {
        if (!assigned.ownedBucketRanges.empty()) {
            // PIP-486 bucket-sharing (consumers outnumbering segments) is not supported yet.
            startPromise_.setError(
                Error{ResultOperationNotSupported,
                      "bucket-shared segment assignments (PIP-486) are not supported yet in the "
                      "scalable-topics client; use at most one stream consumer per segment"});
            return;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        currentAssignment_ = segments;
    }
    if (segments.empty()) {
        startPromise_.setSuccess();
    } else {
        auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(segments.size()));
        for (const auto& assigned : segments) {
            getOrCreateSegmentConsumerAsync(assigned).addListener(
                [self = shared_from_this(), remaining](const Expected<pulsar::Consumer>& result) {
                    if (!result) {
                        self->startPromise_.setError(result.error());  // first error wins (idempotent)
                        return;
                    }
                    if (remaining->fetch_sub(1) == 1) self->startPromise_.setSuccess();
                });
        }
    }
    // Now that every initial segment has its entry, updates (and the session's replay
    // of the initial assignment) reconcile against them — a no-op for the same set.
    std::weak_ptr<StreamConsumerImpl> weak = weak_from_this();
    session_->setListener([weak](const std::vector<AssignedSegment>& newSegments,
                                 const std::vector<AssignedSegment>& oldSegments) {
        if (auto self = weak.lock()) self->onAssignmentChange(newSegments, oldSegments);
    });
}

void StreamConsumerImpl::onAssignmentChange(const std::vector<AssignedSegment>& newSegments,
                                            const std::vector<AssignedSegment>& /*oldSegments*/) {
    std::vector<Future<pulsar::Consumer>> retired;
    std::vector<AssignedSegment> toAdd;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // An update racing closeAsync() must not create consumers nobody will close.
        if (closed_.load()) return;
        currentAssignment_ = newSegments;

        std::unordered_set<std::uint64_t> targetIds;
        for (const auto& assigned : newSegments) targetIds.insert(assigned.segment.segmentId);
        for (auto it = segmentConsumers_.begin(); it != segmentConsumers_.end();) {
            if (targetIds.find(it->first) == targetIds.end()) {
                // Released by a rebalance: close immediately. Unacked in-flight messages
                // are redelivered to the segment's next owner from the shared cursor.
                retired.push_back(std::move(it->second.future));
                latestDelivered_.erase(static_cast<std::int64_t>(it->first));
                it = segmentConsumers_.erase(it);
            } else {
                ++it;
            }
        }
        for (const auto& assigned : newSegments) {
            if (segmentConsumers_.find(assigned.segment.segmentId) != segmentConsumers_.end()) {
                continue;
            }
            if (!assigned.ownedBucketRanges.empty()) {
                // PIP-486 bucket-sharing (consumers outnumbering segments) is not supported yet.
                LOG_ERROR("[" << topic_ << "] segment " << assigned.segment.segmentId
                              << " was assigned bucket-shared (PIP-486), which is not supported yet; "
                                 "skipping it");
                continue;
            }
            toAdd.push_back(assigned);
        }
    }

    for (auto& future : retired) closeWhenReady(future);
    for (const auto& assigned : toAdd) subscribeSegmentWithRetry(assigned, /*attempt*/ 0);
}

pulsar::ConsumerConfiguration StreamConsumerImpl::buildSegmentConfiguration(
    const AssignedSegment& assigned) const {
    // Build a FRESH config every time (pulsar::ConsumerConfiguration's copy ctor shares its impl).
    pulsar::ConsumerConfiguration conf;
    conf.setConsumerType(pulsar::ConsumerExclusive);
    conf.setSchema(config_.schema);
    conf.setSubscriptionInitialPosition(toClassicInitialPosition(config_.initialPosition));
    conf.setConsumerName(consumerName_ + "-seg-" + std::to_string(assigned.segment.segmentId));
    if (config_.ackPolicy.groupTime) {
        conf.setAckGroupingTimeMs(static_cast<long>(config_.ackPolicy.groupTime->count()));
    }
    // AckPolicy::negativeAckRedeliveryDelay is deliberately not wired: a stream
    // consumer has no negative-ack path (documented on the config field).
    if (config_.readCompacted) {
        conf.setReadCompacted(*config_.readCompacted);
    }
    if (config_.replicateSubscriptionState) {
        conf.setReplicateSubscriptionStateEnabled(*config_.replicateSubscriptionState);
    }
    if (!config_.subscriptionProperties.empty()) {
        conf.setSubscriptionProperties(config_.subscriptionProperties);
    }
    for (const auto& [key, value] : config_.properties) conf.setProperty(key, value);
    if (assigned.segment.isLegacy()) conf.setProperty("__pulsar.v5.managed", "true");
    return conf;
}

Future<pulsar::Consumer> StreamConsumerImpl::getOrCreateSegmentConsumerAsync(
    const AssignedSegment& assigned) {
    detail::Promise<pulsar::Consumer> promise;
    const std::uint64_t segmentId = assigned.segment.segmentId;
    std::uint64_t generation = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // closeAsync() has already snapshotted (or will never see) this entry: refuse
        // rather than create a consumer that nothing closes.
        if (closed_.load()) {
            promise.setError(Error{ResultAlreadyClosed, "consumer is closed"});
            return promise.getFuture();
        }
        if (auto it = segmentConsumers_.find(segmentId); it != segmentConsumers_.end()) {
            return it->second.future;
        }
        generation = nextSegmentGeneration_++;
        segmentConsumers_.insert_or_assign(segmentId, SegmentEntry{promise.getFuture(), generation});
    }

    const pulsar::ConsumerConfiguration conf = buildSegmentConfiguration(assigned);
    const std::string attachTopic = assigned.segment.attachTopicName();
    auto self = shared_from_this();
    classic_->subscribeSegmentAsync(
        attachTopic, config_.subscriptionName, conf,
        [self, promise, segmentId, generation](std::variant<pulsar::Error, pulsar::Consumer> result) {
            if (auto* consumer = std::get_if<pulsar::Consumer>(&result)) {
                // pulsar::Consumer is a copyable handle (its virtual dtor suppresses the
                // move ctor), so this is a shared-impl copy, not a deep copy.
                pulsar::Consumer c = *consumer;
                self->startReceiveLoop(c, segmentId, generation);
                promise.setValue(c);
            } else {
                // Evict the failed subscribe so a later attempt (retry or assignment
                // push) can re-create it.
                {
                    std::lock_guard<std::mutex> lock(self->mutex_);
                    self->dropSegmentLocked(segmentId, generation);
                }
                promise.setError(std::get<pulsar::Error>(result));
            }
        });
    return promise.getFuture();
}

bool StreamConsumerImpl::isSegmentStillAssignedLocked(std::uint64_t segmentId) const {
    for (const auto& assigned : currentAssignment_) {
        if (assigned.segment.segmentId == segmentId) return true;
    }
    return false;
}

void StreamConsumerImpl::dropSegmentLocked(std::uint64_t segmentId, std::uint64_t generation) {
    auto it = segmentConsumers_.find(segmentId);
    // A different generation is the live entry of a later re-assignment (the segment
    // was released and handed back in between): leave it alone.
    if (it == segmentConsumers_.end() || it->second.generation != generation) return;
    segmentConsumers_.erase(it);
    latestDelivered_.erase(static_cast<std::int64_t>(segmentId));
}

void StreamConsumerImpl::subscribeSegmentWithRetry(const AssignedSegment& assigned, int attempt) {
    std::weak_ptr<StreamConsumerImpl> weak = weak_from_this();
    getOrCreateSegmentConsumerAsync(assigned).addListener([weak, assigned, attempt](
                                                              const Expected<pulsar::Consumer>& result) {
        auto self = weak.lock();
        if (result || !self || self->closed_.load()) return;
        if (!isRebalanceCollision(result.error().result)) {
            LOG_ERROR("[" << self->topic_ << "] segment " << assigned.segment.segmentId
                          << " subscribe failed (" << result.error()
                          << "); not a rebalance collision, waiting for the next assignment");
            return;
        }
        if (attempt + 1 >= kSubscribeRetryMaxAttempts) {
            LOG_ERROR("[" << self->topic_ << "] segment " << assigned.segment.segmentId
                          << " subscribe still colliding after " << kSubscribeRetryMaxAttempts
                          << " attempts; giving up until the next assignment: " << result.error());
            return;
        }
        {
            std::lock_guard<std::mutex> lock(self->mutex_);
            if (!self->isSegmentStillAssignedLocked(assigned.segment.segmentId)) return;
        }
        LOG_INFO("[" << self->topic_ << "] segment " << assigned.segment.segmentId
                     << " is still held by its previous owner; retrying, attempt " << (attempt + 1) << " of "
                     << kSubscribeRetryMaxAttempts);
        auto timer = self->executor_->createDeadlineTimer();
        const std::int64_t delayMs = std::min<std::int64_t>(100 * (attempt + 1), kSubscribeRetryMaxBackoffMs);
        timer->expires_from_now(std::chrono::milliseconds(delayMs));
        // Weak ref: closeAsync() does not cancel these timers (`timer` keeps itself alive).
        timer->async_wait([weak, assigned, attempt, timer](const ASIO_ERROR& ec) {
            auto self = weak.lock();
            if (ec || !self || self->closed_.load()) return;
            self->subscribeSegmentWithRetry(assigned, attempt + 1);
        });
    });
}

void StreamConsumerImpl::startReceiveLoop(pulsar::Consumer consumer, std::uint64_t segmentId,
                                          std::uint64_t generation) {
    if (closed_.load()) return;
    auto self = shared_from_this();
    consumer.receiveAsync(
        [self, consumer, segmentId, generation](pulsar::Result result, const pulsar::Message& message) {
            if (result != pulsar::ResultOk) {
                if (result == pulsar::ResultTopicTerminated) {
                    // The sealed segment is fully drained: close immediately and drop its
                    // bookkeeping. A late cumulative ack carrying this segment's position is
                    // a no-op — the cursor is already at the end. (The queue consumer's
                    // deferred close does not transfer here: one cumulative ack settles an
                    // unbounded prefix, so there is no per-message outstanding count.)
                    {
                        std::lock_guard<std::mutex> lock(self->mutex_);
                        self->dropSegmentLocked(segmentId, generation);
                    }
                    pulsar::Consumer done = consumer;
                    done.closeAsync([](pulsar::Result) {});
                }
                // Otherwise (AlreadyClosed / consumer closing) just stop the loop.
                return;
            }
            // Snapshot the position vector AT DELIVERY TIME, inside the segment loop: every
            // delivered message carries where all segments stood when it was handed over, so
            // acknowledging it cumulatively advances exactly what had been delivered by then.
            std::map<std::int64_t, pulsar::MessageId> positionVector;
            {
                std::lock_guard<std::mutex> lock(self->mutex_);
                self->latestDelivered_[static_cast<std::int64_t>(segmentId)] = message.getMessageId();
                positionVector = self->latestDelivered_;
            }
            MessageId id = MessageIdFactory::create(
                message.getMessageId(), static_cast<std::int64_t>(segmentId), std::move(positionVector));
            // Report the scalable topic as the source, not the internal segment:// backing topic.
            auto messageImpl = std::make_shared<MessageImpl>(message, std::move(id), self->topic_);
            // Re-arm only once the fan-in queue has room, and hop through the executor so
            // the per-message chain is a loop rather than recursion (see QueueConsumerImpl).
            self->receiveQueue_->offer(std::move(messageImpl))
                .addListener([self, consumer, segmentId, generation](const Expected<void>&) {
                    self->executor_->postWork([self, consumer, segmentId, generation] {
                        self->startReceiveLoop(consumer, segmentId, generation);
                    });
                });
        });
}

Future<MessageImplPtr> StreamConsumerImpl::receiveAsync() { return receiveQueue_->receiveAsync(); }

Future<MessageImplPtr> StreamConsumerImpl::receiveAsync(std::chrono::milliseconds timeout) {
    return receiveQueue_->receiveAsync(timeout);
}

Future<std::vector<MessageImplPtr>> StreamConsumerImpl::receiveMultiAsync(int maxMessages,
                                                                          std::chrono::milliseconds timeout) {
    return receiveQueue_->receiveMultiAsync(maxMessages, timeout);
}

void StreamConsumerImpl::acknowledgeCumulative(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    if (!impl) return;
    // Fan the position vector out as one cumulative ack per segment. This may also
    // advance segments past messages still sitting unread in the mux queue — the
    // vector records what had been DELIVERED into the queue when this message was,
    // not what the application has read; that is the contract, not a defect.
    auto positions = impl->positionVector;
    if (positions.empty() && impl->segmentId != MessageIdImpl::kNoSegment) {
        // An id without a vector (not minted by this consumer): ack its own segment.
        positions.emplace(impl->segmentId, impl->v4MessageId);
    }
    for (const auto& [segmentId, position] : positions) {
        std::optional<Future<pulsar::Consumer>> future;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = segmentConsumers_.find(static_cast<std::uint64_t>(segmentId));
            if (it != segmentConsumers_.end()) future = it->second.future;
        }
        if (!future) continue;  // drained or released: the cursor no longer needs this ack
        const pulsar::MessageId v4 = position;
        future->addListener([v4](const Expected<pulsar::Consumer>& result) {
            if (result) {
                pulsar::Consumer consumer = *result;
                consumer.acknowledgeCumulativeAsync(v4, [](pulsar::Result) {});
            }
        });
    }
}

void StreamConsumerImpl::acknowledgeCumulative(const MessageId& /*id*/, const Transaction& /*txn*/) {
    // Transactions are not implemented yet in the scalable-topics client, and an ack is
    // fire-and-forget void (no error channel). Drop it — the cursor simply does not advance.
    LOG_WARN("[" << topic_ << "] transactional acknowledge is not implemented yet; dropping the ack");
}

Future<void> StreamConsumerImpl::closeAsync() {
    if (closed_.exchange(true)) {
        detail::Promise<void> promise;
        promise.setSuccess();  // idempotent
        return promise.getFuture();
    }
    if (session_) session_->close();
    if (receiveQueue_) receiveQueue_->close();  // fail pending receives

    std::vector<Future<pulsar::Consumer>> consumers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        consumers.reserve(segmentConsumers_.size());
        for (auto& [segmentId, entry] : segmentConsumers_) consumers.push_back(entry.future);
        segmentConsumers_.clear();
        latestDelivered_.clear();
        currentAssignment_.clear();
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

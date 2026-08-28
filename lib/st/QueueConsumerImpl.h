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
#pragma once

#include <pulsar/Consumer.h>
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/st/QueueConsumer.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "DagWatchSession.h"
#include "ReceiveQueue.h"
#include "SegmentLayout.h"
#include "lib/ClientImpl.h"
#include "lib/ExecutorService.h"

namespace pulsar::st {

/**
 * The scalable-topics queue consumer (single scalable topic): a Shared subscription fanned across
 * the topic's segments, a port of the Java v5 ScalableQueueConsumer.
 *
 * It owns one DagWatchSession and, per segment, a classic Shared-subscription pulsar::Consumer on
 * that segment's segment:// backing topic (created via ClientImpl::subscribeSegmentAsync). Both
 * active AND sealed segments are subscribed — a sealed segment may still hold undrained messages
 * and pending acks. Each segment runs a receive loop that stamps the segment id onto every message
 * and fans it into a shared ReceiveQueue; the user receives from that queue. Individual acks route
 * back to the owning segment's consumer via the message id's segment id. Layout changes add
 * consumers for new segments and close ones that left the DAG; a segment that reports
 * TopicTerminated (a sealed segment fully drained) is closed and dropped — but only once every
 * message it delivered has been acked or nacked, so acks for messages still in the mux queue or
 * in the application's hands can still be routed.
 */
class QueueConsumerImpl : public std::enable_shared_from_this<QueueConsumerImpl> {
   public:
    QueueConsumerImpl(pulsar::ClientImplPtr classic, QueueConsumerConfig config);

    /** Start the DAG watch and subscribe the initial segments; completes once they are attached. */
    Future<void> start();

    Future<MessageImplPtr> receiveAsync();
    Future<MessageImplPtr> receiveAsync(std::chrono::milliseconds timeout);
    void acknowledge(const MessageId& id);
    void acknowledge(const MessageId& id, const Transaction& txn);
    void negativeAcknowledge(const MessageId& id);
    Future<void> closeAsync();

    std::string_view topic() const { return topic_; }
    std::string_view subscription() const { return subscription_; }
    std::string_view consumerName() const { return consumerName_; }

   private:
    // How many messages the fan-in queue buffers before back-pressuring the segment receive loops.
    static constexpr std::size_t kReceiveQueueCapacity = 1000;
    // Bounded retry for a segment subscribe that fails off the first-layout path (a layout push
    // also retries, but the DAG may stay quiet for a long time). Mirrors the producer's constants.
    static constexpr int kSubscribeRetryMaxAttempts = 10;
    static constexpr std::int64_t kSubscribeRetryMaxBackoffMs = 500;

    pulsar::ConsumerConfiguration buildSegmentConfiguration(const Segment& segment) const;
    Future<pulsar::Consumer> getOrCreateSegmentConsumerAsync(const Segment& segment);
    // getOrCreateSegmentConsumerAsync plus a bounded backoff retry on failure, used off the
    // first-layout path where no start error surfaces the problem to the caller.
    void subscribeSegmentWithRetry(const Segment& segment, int attempt);
    void startReceiveLoop(pulsar::Consumer consumer, std::uint64_t segmentId);

    // Outstanding-message bookkeeping: fanned-in minus settled (acked or nacked). A terminated
    // segment's consumer closes only once its count reaches zero, so late acks still route.
    void onMessageFannedIn(std::uint64_t segmentId);
    void onMessageSettled(std::uint64_t segmentId);
    // Remove a fully-drained terminated segment's bookkeeping, mark it drained, and hand back its
    // consumer future so the caller can close it outside the lock. Caller holds mutex_.
    std::optional<Future<pulsar::Consumer>> takeDrainedSegmentLocked(std::uint64_t segmentId);
    // Whether the segment is still in the current DAG and not already drained. Caller holds mutex_.
    bool isSegmentStillWantedLocked(std::uint64_t segmentId) const;

    // start()'s future handler: surfaces a start-time lookup failure only; the success path (apply
    // the initial layout, subscribe its segments, complete startPromise_) runs in the listener.
    void onStartResult(const Expected<SegmentLayout>& result);
    void onLayoutChange(const SegmentLayout& newLayout, const SegmentLayout& oldLayout);

    // Route an ack/nack to the consumer that owns the message's segment; a no-op if that segment's
    // consumer is gone (the message will simply be redelivered).
    Future<pulsar::Consumer> segmentConsumerFor(const MessageId& id) const;

    pulsar::ClientImplPtr classic_;
    const QueueConsumerConfig config_;
    const std::string topic_;
    const std::string subscription_;
    const std::string consumerName_;
    // One IO executor shared with the ReceiveQueue: receive-loop re-arms hop through it (so the
    // per-message chain is a loop, not recursion) and retry/timeout timers run on it.
    const pulsar::ExecutorServicePtr executor_;
    DagWatchSessionPtr dagWatch_;
    ReceiveQueuePtr receiveQueue_;
    detail::Promise<void> startPromise_;
    std::atomic<bool> closed_{false};

    mutable std::mutex mutex_;
    bool sawFirstLayout_ = false;                                                   // guarded by mutex_
    std::shared_ptr<const SegmentLayout> currentLayout_;                            // guarded by mutex_
    std::unordered_map<std::uint64_t, Future<pulsar::Consumer>> segmentConsumers_;  // guarded by mutex_
    // Messages fanned in per segment that the application has not yet acked or nacked. Guarded by
    // mutex_.
    std::unordered_map<std::uint64_t, std::size_t> outstanding_;
    // Segments that reported end-of-topic while messages were still outstanding: their consumer
    // stays in segmentConsumers_ for ack routing, and the close runs when the count hits zero.
    // Guarded by mutex_.
    std::unordered_set<std::uint64_t> terminatedSegments_;
    // Segments that have reported end-of-topic and been fully drained (closed); kept so a
    // reconcile does not re-subscribe a still-in-DAG sealed segment (which would redeliver its
    // unacked messages). Pruned when a segment leaves the DAG. Guarded by mutex_.
    std::unordered_set<std::uint64_t> drainedSegments_;
};

using QueueConsumerImplPtr = std::shared_ptr<QueueConsumerImpl>;

}  // namespace pulsar::st

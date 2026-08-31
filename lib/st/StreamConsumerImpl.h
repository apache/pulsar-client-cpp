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
#include <pulsar/st/StreamConsumer.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "ConsumerAssignmentSession.h"
#include "ReceiveQueue.h"
#include "lib/ClientImpl.h"
#include "lib/ExecutorService.h"

namespace pulsar::st {

/**
 * The scalable-topics stream consumer (single scalable topic): ordered consumption
 * over the segment DAG, a port of the Java v5 ScalableStreamConsumer.
 *
 * Unlike the queue consumer, the client enforces no ordering itself: the broker's
 * subscription coordinator withholds a child segment from the assignment until every
 * parent has drained (per-subscription backlog reaching zero), so per-key order
 * across splits and merges holds as long as the application keeps acknowledging.
 * The client's job is to subscribe — Exclusive — to exactly the assigned segments
 * (delivered by the ConsumerAssignmentSession, updated on every rebalance), run one
 * receive loop per segment fanning into the shared mux ReceiveQueue, and stamp every
 * delivered message with a snapshot of all segments' latest-delivered positions (the
 * position vector), so one cumulative acknowledgment advances every segment's cursor.
 *
 * A segment reporting TopicTerminated (sealed and fully drained) is closed and its
 * bookkeeping dropped immediately; a late cumulative ack for it is a no-op — the
 * cursor is already at the end (Java parity; the queue consumer's outstanding-count
 * deferral does not transfer, because one cumulative ack settles an unbounded
 * prefix).
 *
 * PIP-486 bucket-shared segments (non-empty ownedBucketRanges) are not supported
 * yet: the whole-segment Exclusive path covers every assignment the controller
 * produces while consumers do not outnumber segments.
 */
class StreamConsumerImpl : public std::enable_shared_from_this<StreamConsumerImpl> {
   public:
    StreamConsumerImpl(pulsar::ClientImplPtr classic, StreamConsumerConfig config);

    /** Register with the controller and subscribe the initially assigned segments. */
    Future<void> start();

    Future<MessageImplPtr> receiveAsync();
    Future<MessageImplPtr> receiveAsync(std::chrono::milliseconds timeout);
    Future<std::vector<MessageImplPtr>> receiveMultiAsync(int maxMessages, std::chrono::milliseconds timeout);
    void acknowledgeCumulative(const MessageId& id);
    void acknowledgeCumulative(const MessageId& id, const Transaction& txn);
    Future<void> closeAsync();

    std::string_view topic() const { return topic_; }
    std::string_view subscription() const { return subscription_; }
    std::string_view consumerName() const { return consumerName_; }

   private:
    // How many messages the fan-in queue buffers before back-pressuring the segment receive loops.
    static constexpr std::size_t kReceiveQueueCapacity = 1000;
    // Rebalance handoffs are expected to collide briefly (the previous Exclusive owner
    // has not released the segment yet): retry those within a bounded backoff and fail
    // everything else fast. Constants mirror the producer's send retry.
    static constexpr int kSubscribeRetryMaxAttempts = 10;
    static constexpr std::int64_t kSubscribeRetryMaxBackoffMs = 500;

    pulsar::ConsumerConfiguration buildSegmentConfiguration(const AssignedSegment& assigned) const;
    Future<pulsar::Consumer> getOrCreateSegmentConsumerAsync(const AssignedSegment& assigned);
    // getOrCreateSegmentConsumerAsync plus a bounded backoff retry on the rebalance
    // collisions (ConsumerBusy / ConsumerAssignError), used off the start path.
    void subscribeSegmentWithRetry(const AssignedSegment& assigned, int attempt);
    void startReceiveLoop(pulsar::Consumer consumer, std::uint64_t segmentId);

    void onAssignmentChange(const std::vector<AssignedSegment>& newSegments,
                            const std::vector<AssignedSegment>& oldSegments);
    // Whether the segment is still in the current assignment. Caller holds mutex_.
    bool isSegmentStillAssignedLocked(std::uint64_t segmentId) const;

    pulsar::ClientImplPtr classic_;
    const StreamConsumerConfig config_;
    const std::string topic_;
    const std::string subscription_;
    // The controller registration key: defaults to "v5-stream-" + 8 random hex chars
    // when the application did not set one (Java parity).
    const std::string consumerName_;
    const pulsar::ExecutorServicePtr executor_;
    ConsumerAssignmentSessionPtr session_;
    ReceiveQueuePtr receiveQueue_;
    detail::Promise<void> startPromise_;
    std::atomic<bool> closed_{false};

    mutable std::mutex mutex_;
    bool sawFirstAssignment_ = false;                                               // guarded by mutex_
    std::vector<AssignedSegment> currentAssignment_;                                // guarded by mutex_
    std::unordered_map<std::uint64_t, Future<pulsar::Consumer>> segmentConsumers_;  // guarded by mutex_
    // Every segment's latest-delivered position, snapshotted into each delivered
    // message's position vector at delivery time. Guarded by mutex_.
    std::map<std::int64_t, pulsar::MessageId> latestDelivered_;
};

using StreamConsumerImplPtr = std::shared_ptr<StreamConsumerImpl>;

}  // namespace pulsar::st

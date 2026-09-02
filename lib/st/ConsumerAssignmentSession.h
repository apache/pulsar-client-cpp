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

#include <pulsar/st/Future.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "SegmentLayout.h"
#include "lib/Backoff.h"
#include "lib/ClientConnection.h"
#include "lib/ClientImpl.h"
#include "lib/ProtoApiEnums.h"

namespace pulsar::st {

/** One segment assigned to this consumer by the controller. */
struct AssignedSegment {
    Segment segment;
    /**
     * PIP-486: the entry-bucket hash ranges this consumer owns within the segment.
     * Empty means the consumer owns the whole segment and subscribes Exclusive;
     * non-empty means the segment is shared by bucket (Key_Shared STICKY).
     */
    std::vector<HashRange> ownedBucketRanges;
};

/** The controller's assignment of segments to this consumer at one layout epoch. */
struct ConsumerAssignment {
    std::uint64_t layoutEpoch = 0;
    std::vector<AssignedSegment> segments;
};

/**
 * The controller-registration session of one stream/checkpoint consumer, ported from
 * the Java v5 client's ScalableConsumerClient so the two clients behave identically.
 *
 * Unlike the queue consumer — which watches the DAG itself and attaches to every
 * segment — a stream consumer is told what to consume: start() resolves the
 * controller leader's URL through a one-shot DAG-watch lookup, connects to it,
 * registers this consumer (CommandScalableTopicSubscribe) and completes with the
 * initial segment assignment. The controller pushes a new assignment
 * (CommandScalableTopicAssignmentUpdate) after every rebalance — a peer joining or
 * leaving the subscription, or a segment split/merge whose parents have drained —
 * and each accepted (non-stale by layout epoch) assignment is reported to the
 * listener. When the connection drops after the initial assignment the session
 * reconnects with exponential backoff and re-subscribes (within the controller's
 * grace period that returns the same assignment); if it drops before the first
 * assignment, start()'s future fails instead. close() only removes the local
 * registration — the broker reaps the registration through its grace timer.
 */
class ConsumerAssignmentSession : public std::enable_shared_from_this<ConsumerAssignmentSession> {
   public:
    /** Invoked for every accepted assignment after the initial one (and on setListener replay). */
    using AssignmentChangeListener = std::function<void(const std::vector<AssignedSegment>& newSegments,
                                                        const std::vector<AssignedSegment>& oldSegments)>;

    ConsumerAssignmentSession(pulsar::ClientImplPtr client, std::string topic, std::string subscription,
                              std::string consumerName, pulsar::ScalableConsumerType consumerType);

    /**
     * Start the session. May be called once.
     * @return a future completing with the initial assignment's segments, or the failure.
     */
    Future<std::vector<AssignedSegment>> start();

    /** Snapshot of the most recent assignment (empty before the first one). */
    std::vector<AssignedSegment> currentAssignment() const;

    /**
     * Register the listener notified on every accepted assignment update. If an
     * assignment has already been received, it is replayed immediately (newSegments ==
     * oldSegments) so an update that raced the registration is not lost; appliers must
     * be idempotent. Nothing is replayed before the first assignment.
     */
    void setListener(AssignmentChangeListener listener);

    /** Close the session: stop reconnecting and drop the local registration. Idempotent. */
    void close();

    std::uint64_t consumerId() const { return consumerId_; }

   private:
    // One connect-register-subscribe attempt; completes the promise with the
    // controller's assignment or the first failure. Used by start() and reconnect().
    void connectAndSubscribe(const detail::Promise<ConsumerAssignment>& promise);
    void subscribeOn(const pulsar::ClientConnectionPtr& cnx,
                     const detail::Promise<ConsumerAssignment>& promise);
    void handleSessionEvent(pulsar::Result result,
                            const pulsar::proto::CommandScalableTopicAssignmentUpdate* update);
    // Epoch-gated apply + listener notification; used for the initial assignment,
    // pushed updates, and reconnect responses alike.
    void handleAssignmentReceived(const ConsumerAssignment& assignment);
    void handleConnectionClosed();
    void scheduleReconnect();
    void reconnect();

    pulsar::ClientImplPtr client_;
    const std::string topic_;
    const std::string subscription_;
    const std::string consumerName_;
    const pulsar::ScalableConsumerType consumerType_;
    const std::uint64_t consumerId_;
    pulsar::Backoff backoff_;
    DeadlineTimerPtr reconnectTimer_;  // global-scope alias from AsioTimer.h
    detail::Promise<std::vector<AssignedSegment>> initialAssignmentPromise_;
    std::atomic<bool> sawInitialAssignment_{false};
    std::atomic<bool> closed_{false};

    mutable std::mutex mutex_;
    std::vector<AssignedSegment> currentAssignment_;  // guarded by mutex_
    std::int64_t currentEpoch_ = -1;                  // guarded by mutex_; -1 until the first assignment
    AssignmentChangeListener listener_;               // guarded by mutex_
    pulsar::ClientConnectionWeakPtr cnx_;             // guarded by mutex_
    // The subscribe request awaiting its response on cnx_, so close() can withdraw it.
    std::optional<std::uint64_t> pendingSubscribeRequestId_;  // guarded by mutex_
};

using ConsumerAssignmentSessionPtr = std::shared_ptr<ConsumerAssignmentSession>;

}  // namespace pulsar::st

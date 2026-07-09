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
#include <string>

#include "SegmentLayout.h"
#include "lib/Backoff.h"
#include "lib/ClientConnection.h"
#include "lib/ClientImpl.h"

namespace pulsar::st {

/**
 * The DAG-watch session of one scalable topic, ported from the Java v5 client's
 * DagWatchClient so the two clients behave identically.
 *
 * start() connects to a broker (through the classic client's pool) and sends a
 * CommandScalableTopicLookup; the broker answers — and afterwards pushes — a
 * CommandScalableTopicUpdate carrying the full DAG, correlated by the
 * client-assigned session id. Each accepted update replaces the current
 * SegmentLayout and is reported to the layout-change listener. When the
 * connection drops after the initial layout, the session reconnects with
 * exponential backoff and re-issues the lookup; if it drops before the first
 * layout, start()'s future fails instead (the caller's create() surfaces the
 * error rather than silently retrying). Stale pushes cannot arrive out of
 * order: updates are ordered per connection, and the old connection's registry
 * is drained before the session re-attaches to a new one.
 */
class DagWatchSession : public std::enable_shared_from_this<DagWatchSession> {
   public:
    /** Invoked for every accepted layout, including the first (oldLayout is empty then). */
    using LayoutChangeListener =
        std::function<void(const SegmentLayout& newLayout, const SegmentLayout& oldLayout)>;

    /**
     * @param client the classic client whose connection pool and executors are reused.
     * @param topic the scalable topic, in any accepted spelling ("topic://t/n/x",
     *        "persistent://t/n/x", or a short form); the broker resolves it and the
     *        session adopts the canonical identity from the first response.
     * @param createIfMissing whether the broker may auto-create a missing topic on
     *        lookup (single-topic producers/consumers pass true; namespace watchers
     *        pass false so a deleted topic is not resurrected by a reconnect).
     */
    DagWatchSession(pulsar::ClientImplPtr client, std::string topic, bool createIfMissing);

    /**
     * Start the session. May be called once.
     * @return a future completing with the initial layout, or the lookup failure.
     */
    Future<SegmentLayout> start();

    /** Snapshot of the most recent layout (empty before the first update). */
    SegmentLayout currentLayout() const;

    /** The canonical topic identity, falling back to the input before the first response. */
    std::string topicName() const;

    /** Register the listener notified on every accepted layout update. */
    void setLayoutChangeListener(LayoutChangeListener listener);

    /** Close the session: stop reconnecting and tell the broker. Idempotent. */
    void close();

    std::uint64_t sessionId() const { return sessionId_; }

    /**
     * The topic spelling used for the classic connection lookup. The classic
     * TopicName does not know the topic:// scheme, so that spelling maps to its
     * persistent://... twin for the purpose of finding a broker; the wire lookup
     * still carries the caller's original spelling.
     */
    static std::string lookupCompatibleTopic(const std::string& topic);

   private:
    void attach(const pulsar::ClientConnectionPtr& cnx);
    void handleSessionEvent(pulsar::Result result, const pulsar::proto::CommandScalableTopicUpdate* update);
    void handleDagUpdate(const pulsar::proto::CommandScalableTopicUpdate& update);
    void handleServerError(const pulsar::proto::CommandScalableTopicUpdate& update);
    void handleConnectionClosed();
    void scheduleReconnect();
    void reconnect();

    pulsar::ClientImplPtr client_;
    const std::string inputTopic_;
    const bool createIfMissing_;
    const std::uint64_t sessionId_;
    pulsar::Backoff backoff_;
    DeadlineTimerPtr reconnectTimer_;  // global-scope alias from AsioTimer.h
    detail::Promise<SegmentLayout> initialLayoutPromise_;
    std::atomic<bool> closed_{false};

    mutable std::mutex mutex_;
    SegmentLayout currentLayout_;          // guarded by mutex_
    std::string resolvedTopicName_;        // guarded by mutex_; empty until the first response
    LayoutChangeListener listener_;        // guarded by mutex_
    pulsar::ClientConnectionWeakPtr cnx_;  // guarded by mutex_
};

using DagWatchSessionPtr = std::shared_ptr<DagWatchSession>;

}  // namespace pulsar::st

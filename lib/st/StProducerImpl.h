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

#include <pulsar/Producer.h>
#include <pulsar/ProducerConfiguration.h>
#include <pulsar/st/Producer.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "DagWatchSession.h"
#include "ProducerImplBase.h"
#include "SegmentLayout.h"
#include "lib/ClientImpl.h"

namespace pulsar::st {

/**
 * The scalable-topics producer: a faithful port of the Java v5 ScalableTopicProducer.
 *
 * A scalable topic's segments are ordinary persistent topics, so this owns ONE
 * DagWatchSession per topic and, per active segment, a classic pulsar::Producer
 * pinned to the DAG-provided owner broker (created lazily on first send, or eagerly
 * for exclusive access modes). Each publish is routed by key to a segment through
 * the SegmentRouter, the classic producer publishes it, and the returned classic
 * MessageId is wrapped with the segment id into a pulsar::st::MessageId. When a
 * segment is sealed/terminated under the producer, the send retries with backoff and
 * re-routes onto the new layout the DAG watch delivers.
 *
 * Unlike Java, no per-segment dispatch chain is needed: the C++ Future fires its
 * listeners in registration (FIFO) order, so multiple sends waiting on one segment's
 * creation future dispatch in call order for free.
 */
class StProducerImpl final : public ProducerImplBase, public std::enable_shared_from_this<StProducerImpl> {
   public:
    StProducerImpl(pulsar::ClientImplPtr classic, ProducerConfig config);

    /**
     * Start the DAG watch and become ready. The returned future completes once the
     * initial layout arrives (and, for exclusive access modes, once every active
     * segment's producer is attached), or fails if the lookup/attach fails.
     */
    Future<void> start();

    // ProducerImplBase
    Future<MessageId> sendAsync(OutgoingMessage message) override;
    std::string_view topic() const override { return topic_; }
    std::string_view producerName() const override { return producerName_; }
    std::optional<std::int64_t> lastSequenceId() const override;
    Future<void> flushAsync() override;
    Future<void> closeAsync() override;

   private:
    friend struct StProducerTestAccess;  // broker-free access to the pure config/routing helpers

    // Number of send attempts once a segment is gone (seal/terminate), and the cap on
    // the per-attempt backoff — matches the Java v5 producer.
    static constexpr int kSendRetryMaxAttempts = 10;
    static constexpr std::int64_t kSendRetryMaxBackoffMs = 500;

    // Build a FRESH classic producer configuration for one segment. Never copy-then-mutate
    // a shared base: pulsar::ProducerConfiguration's copy constructor shares its impl, so a
    // per-segment copy would clobber every sibling.
    pulsar::ProducerConfiguration buildSegmentConfiguration(const Segment& segment) const;

    Future<pulsar::Producer> getOrCreateSegmentProducerAsync(std::uint64_t segmentId);

    // start()'s future handler: only surfaces a start-time failure. The success path (apply
    // the initial layout and complete startPromise_) is driven by the layout listener, which
    // the DagWatchSession invokes right after completing start()'s future.
    void onStartResult(const Expected<SegmentLayout>& result);
    // The layout-change listener: fires for every accepted layout (the first has an empty
    // oldLayout). Swaps the current layout, retires producers for departed segments, and on
    // the first call completes startPromise_ (eagerly attaching all segments for exclusive
    // modes).
    void onLayoutChange(const SegmentLayout& newLayout, const SegmentLayout& oldLayout);
    void completeStart();

    void dispatchSend(OutgoingMessage message, std::uint64_t segmentId, int attempt,
                      const detail::Promise<MessageId>& userPromise);
    // On a send failure: retry after backoff on a fresh layout if the segment is gone and the
    // attempt budget remains, otherwise fail the user's future.
    void handleSegmentFailure(Error error, OutgoingMessage message, std::uint64_t segmentId, int attempt,
                              const detail::Promise<MessageId>& userPromise);
    pulsar::Message buildClassicMessage(const OutgoingMessage& message) const;

    std::shared_ptr<const SegmentLayout> snapshotLayout() const;
    static bool isSegmentGoneError(pulsar::Result result, const std::string& message);
    static bool requiresExclusiveAttach(ProducerAccessMode mode);
    static pulsar::ProducerConfiguration::ProducerAccessMode toClassicAccessMode(ProducerAccessMode mode);

    pulsar::ClientImplPtr classic_;
    const ProducerConfig config_;
    const std::string topic_;
    const std::string producerName_;
    DagWatchSessionPtr dagWatch_;
    SegmentRouter router_;
    detail::Promise<void> startPromise_;
    std::atomic<bool> closed_{false};

    mutable std::mutex mutex_;
    bool sawFirstLayout_ = false;                         // guarded by mutex_
    std::shared_ptr<const SegmentLayout> currentLayout_;  // guarded by mutex_ (never null)
    std::unordered_map<std::uint64_t, Future<pulsar::Producer>> segmentProducers_;  // guarded by mutex_
    std::unordered_map<std::uint64_t, Future<MessageId>> inFlight_;  // guarded by mutex_ (for flush)
    std::uint64_t nextInFlightId_ = 0;                               // guarded by mutex_
};

using StProducerImplPtr = std::shared_ptr<StProducerImpl>;

}  // namespace pulsar::st

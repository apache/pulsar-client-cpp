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

#include <pulsar/st/Expected.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// The client-side model of a scalable topic's segment layout, built from the
// ScalableTopicDAG the broker sends on the DAG-watch session, plus the key ->
// segment router. Ported from the Java v5 client (ClientSegmentLayout /
// SegmentRouter / ScalableTopicHashing) so the two clients route identically.

namespace pulsar {
namespace proto {
class ScalableTopicDAG;
}
}  // namespace pulsar

namespace pulsar::st {

/**
 * Scalable-topic key hashing — the single source of truth for both segment
 * routing and entry-bucketing.
 *
 * A single raw (unmasked) 32-bit Murmur3 hash of a key splits into two
 * independent 16-bit halves: the HIGH half routes segments, the LOW half
 * routes entry-buckets. The raw hash is required so the high half is
 * full-range (the classic masked hash clears bit 31, which would confine the
 * high half to [0, 0x7FFF]). Compute murmur() once per key and split it.
 */
struct ScalableTopicHashing {
    /** The raw 32-bit Murmur3 hash of a key. */
    static std::uint32_t murmur(std::string_view key);

    /** The 16-bit segment-routing hash (high 16 bits) of a precomputed murmur(). */
    static std::uint32_t segmentHash(std::uint32_t murmur) { return (murmur >> 16) & 0xFFFFu; }

    /** The 16-bit entry-bucket hash (low 16 bits) of a precomputed murmur(). */
    static std::uint32_t entryBucketHash(std::uint32_t murmur) { return murmur & 0xFFFFu; }
};

/** An inclusive hash range [start, end] within the 16-bit hash space (0x0000-0xFFFF). */
struct HashRange {
    static constexpr std::uint32_t kMinHash = 0x0000;
    static constexpr std::uint32_t kMaxHash = 0xFFFF;

    std::uint32_t start = kMinHash;
    std::uint32_t end = kMaxHash;

    bool contains(std::uint32_t hash) const { return hash >= start && hash <= end; }
    bool operator==(const HashRange&) const = default;
};

/**
 * One segment of a scalable topic, as seen by the client: its identity, the
 * hash range it owns, and the topic its per-segment producer/consumer attaches
 * to.
 *
 * `legacyTopicName` is set for legacy segments — entries in a synthetic layout
 * that wrap an existing, externally managed persistent:// topic (e.g. one
 * partition of a not-yet-migrated regular topic). Those attach to the wrapped
 * topic; regular controller-managed segments attach to the computed
 * segment://... topic.
 */
struct Segment {
    std::uint64_t segmentId = 0;
    HashRange range;
    /** The computed segment://tenant/ns/topic/xxxx-xxxx-id topic. */
    std::string segmentTopicName;
    /** Wrapped persistent:// topic for legacy segments; unset otherwise. */
    std::optional<std::string> legacyTopicName;
    /** Entry-bucket split points within the segment (empty = one bucket). */
    std::vector<std::uint32_t> entryBucketSplits;

    bool isLegacy() const { return legacyTopicName.has_value(); }

    /** The topic a per-segment producer/consumer attaches to. */
    const std::string& attachTopicName() const {
        return legacyTopicName ? *legacyTopicName : segmentTopicName;
    }
};

/**
 * Immutable client-side view of a scalable topic's segment layout at one DAG
 * epoch. Built from the ScalableTopicDAG the broker returns on the DAG-watch
 * session; consumers of this class swap whole layouts as updates arrive (epoch
 * monotonicity is enforced by the watch session, not here).
 */
class SegmentLayout {
   public:
    /** An empty layout: epoch 0, no segments. */
    SegmentLayout() = default;

    /**
     * Build a layout from the broker-provided DAG.
     *
     * @param dag the DAG snapshot from CommandScalableTopicUpdate.
     * @param resolvedTopicName the canonical "topic://tenant/ns/topic" identity
     *        (CommandScalableTopicUpdate.resolved_topic_name); used to compute
     *        the per-segment segment:// topic names.
     * @return the layout, or an Error if the DAG is malformed (bad topic scheme
     *         or an out-of-bounds hash range).
     */
    static Expected<SegmentLayout> fromProto(const pulsar::proto::ScalableTopicDAG& dag,
                                             const std::string& resolvedTopicName);

    /** The DAG generation this layout represents. */
    std::uint64_t epoch() const { return epoch_; }

    /** Active segments, sorted by hash-range start (the routing order). */
    const std::vector<Segment>& activeSegments() const { return activeSegments_; }

    /** Sealed segments still present in the DAG (finite, eventually drained), sorted by id. */
    const std::vector<Segment>& sealedSegments() const { return sealedSegments_; }

    /** The broker serving a segment, or nullptr if the DAG did not name one. */
    const std::string* brokerUrl(std::uint64_t segmentId) const;
    /** The TLS address of the broker serving a segment, or nullptr if absent. */
    const std::string* brokerUrlTls(std::uint64_t segmentId) const;

    /** The controller leader's address, when the DAG carries one. */
    const std::optional<std::string>& controllerBrokerUrl() const { return controllerBrokerUrl_; }
    const std::optional<std::string>& controllerBrokerUrlTls() const { return controllerBrokerUrlTls_; }

   private:
    struct BrokerAddress {
        std::string url;
        std::optional<std::string> urlTls;
    };

    std::uint64_t epoch_ = 0;
    std::vector<Segment> activeSegments_;
    std::vector<Segment> sealedSegments_;
    std::unordered_map<std::uint64_t, BrokerAddress> segmentBrokers_;
    std::optional<std::string> controllerBrokerUrl_;
    std::optional<std::string> controllerBrokerUrlTls_;
};

/**
 * Routes messages to segments.
 *
 * Keyed messages route by the 16-bit segment hash to the active segment whose
 * range contains it. If EVERY active segment is a legacy segment (synthetic
 * layout for a not-yet-migrated regular topic), routing switches to
 * signSafeMod(classicMurmur3(key), N) over segment_id, so producers using this
 * client route exactly like the classic partitioned-topic producers still
 * attached to the same topic. Keyless messages route round-robin.
 */
class SegmentRouter {
   public:
    /**
     * Route a keyed message.
     *
     * @return the owning segment's id, or an Error if the layout has no active
     *         segments (ResultServiceUnitNotReady) or no active segment covers
     *         the hash — a malformed DAG (ResultUnknownError).
     */
    Expected<std::uint64_t> route(std::string_view key, const SegmentLayout& layout);

    /** Route a keyless message round-robin across the active segments. */
    Expected<std::uint64_t> routeRoundRobin(const SegmentLayout& layout);

   private:
    std::atomic<std::uint32_t> roundRobinCounter_{0};
};

}  // namespace pulsar::st

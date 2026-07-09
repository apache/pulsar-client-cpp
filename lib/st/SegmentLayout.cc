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
#include "SegmentLayout.h"

#include <algorithm>
#include <cstdio>
#include <string>

#include "PulsarApi.pb.h"
#include "lib/Murmur3_32Hash.h"

namespace pulsar::st {

namespace {

constexpr std::string_view kTopicScheme = "topic://";
constexpr std::string_view kSegmentScheme = "segment://";

// "<start>-<end>-<segmentId>" with the range bounds as 4-digit hex, matching
// SegmentTopicName.formatDescriptor in the Java client/broker.
std::string segmentDescriptor(const HashRange& range, std::uint64_t segmentId) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04x-%04x-%llu", range.start, range.end,
                  static_cast<unsigned long long>(segmentId));
    return buf;
}

int signSafeMod(int dividend, int divisor) {
    int mod = dividend % divisor;
    return mod < 0 ? mod + divisor : mod;
}

}  // namespace

std::uint32_t ScalableTopicHashing::murmur(std::string_view key) {
    return pulsar::Murmur3_32Hash().makeRawHash(key.data(), static_cast<int64_t>(key.size()));
}

Expected<SegmentLayout> SegmentLayout::fromProto(const pulsar::proto::ScalableTopicDAG& dag,
                                                 const std::string& resolvedTopicName) {
    if (resolvedTopicName.rfind(kTopicScheme, 0) != 0) {
        return unexpected(
            ResultInvalidTopicName,
            "resolved scalable-topic name must use the topic:// scheme, got: " + resolvedTopicName);
    }
    // "topic://tenant/ns/topic" -> "tenant/ns/topic"; the segment topics live at
    // "segment://tenant/ns/topic/<descriptor>".
    const std::string topicPath = resolvedTopicName.substr(kTopicScheme.size());

    SegmentLayout layout;
    layout.epoch_ = dag.epoch();

    for (int i = 0; i < dag.segment_brokers_size(); i++) {
        const auto& addr = dag.segment_brokers(i);
        BrokerAddress broker;
        broker.url = addr.broker_url();
        if (addr.has_broker_url_tls()) {
            broker.urlTls = addr.broker_url_tls();
        }
        layout.segmentBrokers_[addr.segment_id()] = std::move(broker);
    }

    for (int i = 0; i < dag.segments_size(); i++) {
        const auto& seg = dag.segments(i);
        if (seg.hash_start() > HashRange::kMaxHash || seg.hash_end() > HashRange::kMaxHash ||
            seg.hash_end() < seg.hash_start()) {
            return unexpected(ResultUnknownError,
                              "malformed DAG: segment " + std::to_string(seg.segment_id()) +
                                  " has an invalid hash range [" + std::to_string(seg.hash_start()) + ", " +
                                  std::to_string(seg.hash_end()) + "]");
        }

        Segment segment;
        segment.segmentId = seg.segment_id();
        segment.range = HashRange{seg.hash_start(), seg.hash_end()};
        segment.segmentTopicName = std::string(kSegmentScheme) + topicPath + "/" +
                                   segmentDescriptor(segment.range, seg.segment_id());
        if (seg.has_legacy_topic_name()) {
            segment.legacyTopicName = seg.legacy_topic_name();
        }
        segment.entryBucketSplits.reserve(seg.entry_bucket_splits_size());
        for (int j = 0; j < seg.entry_bucket_splits_size(); j++) {
            segment.entryBucketSplits.push_back(seg.entry_bucket_splits(j));
        }

        if (seg.state() == pulsar::proto::ACTIVE) {
            layout.activeSegments_.push_back(std::move(segment));
        } else {
            layout.sealedSegments_.push_back(std::move(segment));
        }
    }

    // Active segments sort by hash-range start (the routing order); sealed order
    // doesn't matter for correctness, sort by id for stable iteration.
    std::sort(layout.activeSegments_.begin(), layout.activeSegments_.end(),
              [](const Segment& a, const Segment& b) { return a.range.start < b.range.start; });
    std::sort(layout.sealedSegments_.begin(), layout.sealedSegments_.end(),
              [](const Segment& a, const Segment& b) { return a.segmentId < b.segmentId; });

    if (dag.has_controller_broker_url()) {
        layout.controllerBrokerUrl_ = dag.controller_broker_url();
    }
    if (dag.has_controller_broker_url_tls()) {
        layout.controllerBrokerUrlTls_ = dag.controller_broker_url_tls();
    }
    return layout;
}

const std::string* SegmentLayout::brokerUrl(std::uint64_t segmentId) const {
    auto it = segmentBrokers_.find(segmentId);
    return it != segmentBrokers_.end() ? &it->second.url : nullptr;
}

const std::string* SegmentLayout::brokerUrlTls(std::uint64_t segmentId) const {
    auto it = segmentBrokers_.find(segmentId);
    return (it != segmentBrokers_.end() && it->second.urlTls) ? &*it->second.urlTls : nullptr;
}

Expected<std::uint64_t> SegmentRouter::route(std::string_view key, const SegmentLayout& layout) {
    const auto& active = layout.activeSegments();
    if (active.empty()) {
        return unexpected(ResultServiceUnitNotReady, "no active segments in the topic layout");
    }

    // Synthetic layout for a not-yet-migrated regular topic: route
    // signSafeMod(classicMurmur3(key), N) over segment_id, exactly like the
    // classic partitioned-topic producers still attached to the same topic.
    const bool allLegacy =
        std::all_of(active.begin(), active.end(), [](const Segment& s) { return s.isLegacy(); });
    if (allLegacy) {
        const int hash32 = pulsar::Murmur3_32Hash().makeHash(std::string(key));
        const int partition = signSafeMod(hash32, static_cast<int>(active.size()));
        for (const auto& segment : active) {
            if (segment.segmentId == static_cast<std::uint64_t>(partition)) {
                return segment.segmentId;
            }
        }
        return unexpected(ResultUnknownError,
                          "synthetic layout is missing segment_id=" + std::to_string(partition) +
                              " (N=" + std::to_string(active.size()) + ")");
    }

    const std::uint32_t hash = ScalableTopicHashing::segmentHash(ScalableTopicHashing::murmur(key));
    for (const auto& segment : active) {
        if (segment.range.contains(hash)) {
            return segment.segmentId;
        }
    }
    return unexpected(ResultUnknownError,
                      "no active segment covers hash " + std::to_string(hash) + " for the message key");
}

Expected<std::uint64_t> SegmentRouter::routeRoundRobin(const SegmentLayout& layout) {
    const auto& active = layout.activeSegments();
    if (active.empty()) {
        return unexpected(ResultServiceUnitNotReady, "no active segments in the topic layout");
    }
    const std::uint32_t index = roundRobinCounter_.fetch_add(1, std::memory_order_relaxed);
    return active[index % active.size()].segmentId;
}

}  // namespace pulsar::st

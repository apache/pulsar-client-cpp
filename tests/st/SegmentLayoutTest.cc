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
#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <string>

#include "PulsarApi.pb.h"
#include "lib/Murmur3_32Hash.h"
#include "lib/st/SegmentLayout.h"

using namespace pulsar::st;
namespace proto = pulsar::proto;

namespace {

const std::string kTopic = "topic://public/default/orders";

void addSegment(proto::ScalableTopicDAG& dag, std::uint64_t id, std::uint32_t start, std::uint32_t end,
                proto::SegmentState state, const std::string& legacyTopic = {}) {
    auto* seg = dag.add_segments();
    seg->set_segment_id(id);
    seg->set_hash_start(start);
    seg->set_hash_end(end);
    seg->set_state(state);
    seg->set_created_at_epoch(1);
    seg->set_created_at_ms(1000);
    if (!legacyTopic.empty()) {
        seg->set_legacy_topic_name(legacyTopic);
    }
}

void addBroker(proto::ScalableTopicDAG& dag, std::uint64_t segmentId, const std::string& url,
               const std::string& urlTls = {}) {
    auto* addr = dag.add_segment_brokers();
    addr->set_segment_id(segmentId);
    addr->set_broker_url(url);
    if (!urlTls.empty()) {
        addr->set_broker_url_tls(urlTls);
    }
}

// A deterministic key whose 16-bit segment hash falls within [start, end].
std::string findKeyInRange(std::uint32_t start, std::uint32_t end) {
    for (int i = 0; i < 1000000; i++) {
        std::string key = "key-" + std::to_string(i);
        std::uint32_t hash = ScalableTopicHashing::segmentHash(ScalableTopicHashing::murmur(key));
        if (hash >= start && hash <= end) {
            return key;
        }
    }
    ADD_FAILURE() << "no key found in range [" << start << ", " << end << "]";
    return {};
}

}  // namespace

TEST(ScalableTopicHashingTest, testHashSplitsIntoIndependentHalves) {
    const std::string key = "some-routing-key";
    std::uint32_t murmur = ScalableTopicHashing::murmur(key);
    std::uint32_t segment = ScalableTopicHashing::segmentHash(murmur);
    std::uint32_t bucket = ScalableTopicHashing::entryBucketHash(murmur);
    ASSERT_LE(segment, 0xFFFFu);
    ASSERT_LE(bucket, 0xFFFFu);
    ASSERT_EQ((segment << 16) | bucket, murmur);
}

TEST(ScalableTopicHashingTest, testRawHashIsUnmasked) {
    // The classic makeHash clears bit 31; the raw hash must not, or the high
    // half of the split would be confined to [0, 0x7FFF]. Find a key where the
    // two differ, proving the raw path is in use.
    bool foundHighBit = false;
    for (int i = 0; i < 100000 && !foundHighBit; i++) {
        std::string key = "key-" + std::to_string(i);
        foundHighBit = (ScalableTopicHashing::murmur(key) & 0x80000000u) != 0;
    }
    ASSERT_TRUE(foundHighBit);
}

TEST(SegmentLayoutTest, testFromProtoBuildsSortedLayout) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(7);
    // Add out of order to verify sorting by range start.
    addSegment(dag, 2, 0x8000, 0xFFFF, proto::ACTIVE);
    addSegment(dag, 1, 0x0000, 0x7FFF, proto::ACTIVE);
    addSegment(dag, 0, 0x0000, 0xFFFF, proto::SEALED);
    addBroker(dag, 1, "pulsar://broker1:6650", "pulsar+ssl://broker1:6651");
    addBroker(dag, 2, "pulsar://broker2:6650");
    dag.set_controller_broker_url("pulsar://controller:6650");

    auto layout = SegmentLayout::fromProto(dag, kTopic);
    ASSERT_TRUE(layout);
    ASSERT_EQ(layout->epoch(), 7u);

    ASSERT_EQ(layout->activeSegments().size(), 2u);
    ASSERT_EQ(layout->activeSegments()[0].segmentId, 1u);  // sorted by range start
    ASSERT_EQ(layout->activeSegments()[1].segmentId, 2u);
    ASSERT_EQ(layout->activeSegments()[0].range, (HashRange{0x0000, 0x7FFF}));
    ASSERT_EQ(layout->activeSegments()[0].segmentTopicName, "segment://public/default/orders/0000-7fff-1");
    ASSERT_EQ(layout->activeSegments()[1].segmentTopicName, "segment://public/default/orders/8000-ffff-2");
    ASSERT_FALSE(layout->activeSegments()[0].isLegacy());
    ASSERT_EQ(layout->activeSegments()[0].attachTopicName(), layout->activeSegments()[0].segmentTopicName);

    ASSERT_EQ(layout->sealedSegments().size(), 1u);
    ASSERT_EQ(layout->sealedSegments()[0].segmentId, 0u);

    ASSERT_NE(layout->brokerUrl(1), nullptr);
    ASSERT_EQ(*layout->brokerUrl(1), "pulsar://broker1:6650");
    ASSERT_NE(layout->brokerUrlTls(1), nullptr);
    ASSERT_EQ(*layout->brokerUrlTls(1), "pulsar+ssl://broker1:6651");
    ASSERT_EQ(layout->brokerUrlTls(2), nullptr);
    ASSERT_EQ(layout->brokerUrl(42), nullptr);

    ASSERT_TRUE(layout->controllerBrokerUrl().has_value());
    ASSERT_EQ(*layout->controllerBrokerUrl(), "pulsar://controller:6650");
    ASSERT_FALSE(layout->controllerBrokerUrlTls().has_value());
}

TEST(SegmentLayoutTest, testFromProtoParsesLegacyAndBucketSplits) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(1);
    addSegment(dag, 0, 0x0000, 0xFFFF, proto::ACTIVE, "persistent://public/default/orders-partition-0");
    auto* seg = dag.mutable_segments(0);
    seg->add_entry_bucket_splits(0x4000);
    seg->add_entry_bucket_splits(0x8000);

    auto layout = SegmentLayout::fromProto(dag, kTopic);
    ASSERT_TRUE(layout);
    const Segment& segment = layout->activeSegments()[0];
    ASSERT_TRUE(segment.isLegacy());
    ASSERT_EQ(segment.attachTopicName(), "persistent://public/default/orders-partition-0");
    ASSERT_EQ(segment.entryBucketSplits, (std::vector<std::uint32_t>{0x4000, 0x8000}));
}

TEST(SegmentLayoutTest, testFromProtoRejectsBadTopicScheme) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(1);
    auto layout = SegmentLayout::fromProto(dag, "persistent://public/default/orders");
    ASSERT_FALSE(layout);
    ASSERT_EQ(layout.error().result, pulsar::ResultInvalidTopicName);
}

TEST(SegmentLayoutTest, testFromProtoRejectsMalformedHashRange) {
    {
        proto::ScalableTopicDAG dag;
        dag.set_epoch(1);
        addSegment(dag, 0, 0x8000, 0x7FFF, proto::ACTIVE);  // end < start
        ASSERT_FALSE(SegmentLayout::fromProto(dag, kTopic));
    }
    {
        proto::ScalableTopicDAG dag;
        dag.set_epoch(1);
        addSegment(dag, 0, 0x0000, 0x10000, proto::ACTIVE);  // end > 16-bit space
        ASSERT_FALSE(SegmentLayout::fromProto(dag, kTopic));
    }
}

namespace {

Expected<SegmentLayout> fourSegmentLayout() {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(1);
    addSegment(dag, 0, 0x0000, 0x3FFF, proto::ACTIVE);
    addSegment(dag, 1, 0x4000, 0x7FFF, proto::ACTIVE);
    addSegment(dag, 2, 0x8000, 0xBFFF, proto::ACTIVE);
    addSegment(dag, 3, 0xC000, 0xFFFF, proto::ACTIVE);
    return SegmentLayout::fromProto(dag, kTopic);
}

}  // namespace

TEST(SegmentRouterTest, testKeyedRoutingHitsOwningRange) {
    auto layout = fourSegmentLayout();
    ASSERT_TRUE(layout);
    SegmentRouter router;

    for (int i = 0; i < 200; i++) {
        std::string key = "order-" + std::to_string(i);
        auto id = router.route(key, *layout);
        ASSERT_TRUE(id);
        std::uint32_t hash = ScalableTopicHashing::segmentHash(ScalableTopicHashing::murmur(key));
        const Segment& segment = layout->activeSegments()[*id];  // ids == sorted positions here
        ASSERT_TRUE(segment.range.contains(hash)) << "key " << key << " hash " << hash;
    }
}

TEST(SegmentRouterTest, testKeyedRoutingIsDeterministic) {
    auto layout = fourSegmentLayout();
    ASSERT_TRUE(layout);
    SegmentRouter router;
    for (int i = 0; i < 20; i++) {
        std::string key = "stable-key-" + std::to_string(i);
        auto first = router.route(key, *layout);
        auto second = router.route(key, *layout);
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
        ASSERT_EQ(*first, *second);
    }
}

TEST(SegmentRouterTest, testSingleFullRangeSegmentGetsEverything) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(1);
    addSegment(dag, 9, 0x0000, 0xFFFF, proto::ACTIVE);
    auto layout = SegmentLayout::fromProto(dag, kTopic);
    ASSERT_TRUE(layout);
    SegmentRouter router;
    for (int i = 0; i < 50; i++) {
        auto id = router.route("k" + std::to_string(i), *layout);
        ASSERT_TRUE(id);
        ASSERT_EQ(*id, 9u);
    }
}

TEST(SegmentRouterTest, testNoActiveSegmentsIsAnError) {
    SegmentLayout empty;
    SegmentRouter router;
    auto id = router.route("key", empty);
    ASSERT_FALSE(id);
    ASSERT_EQ(id.error().result, pulsar::ResultServiceUnitNotReady);
    auto rr = router.routeRoundRobin(empty);
    ASSERT_FALSE(rr);
}

TEST(SegmentRouterTest, testUncoveredHashIsAnError) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(1);
    addSegment(dag, 0, 0x0000, 0x7FFF, proto::ACTIVE);  // gap: [0x8000, 0xFFFF] uncovered
    auto layout = SegmentLayout::fromProto(dag, kTopic);
    ASSERT_TRUE(layout);
    SegmentRouter router;
    std::string uncoveredKey = findKeyInRange(0x8000, 0xFFFF);
    auto id = router.route(uncoveredKey, *layout);
    ASSERT_FALSE(id);
    ASSERT_EQ(id.error().result, pulsar::ResultUnknownError);
}

TEST(SegmentRouterTest, testAllLegacyRoutesModNLikeClassicPartitions) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(1);
    const int n = 4;
    for (int i = 0; i < n; i++) {
        addSegment(dag, i, i * 0x4000, (i + 1) * 0x4000 - 1, proto::ACTIVE,
                   "persistent://public/default/orders-partition-" + std::to_string(i));
    }
    auto layout = SegmentLayout::fromProto(dag, kTopic);
    ASSERT_TRUE(layout);
    SegmentRouter router;

    for (int i = 0; i < 100; i++) {
        std::string key = "legacy-key-" + std::to_string(i);
        auto id = router.route(key, *layout);
        ASSERT_TRUE(id);
        // Must match the classic partitioned-topic routing exactly.
        int expected = pulsar::Murmur3_32Hash().makeHash(key) % n;
        ASSERT_EQ(*id, static_cast<std::uint64_t>(expected)) << "key " << key;
    }
}

TEST(SegmentRouterTest, testMixedLegacyAndRegularUsesRangeRouting) {
    proto::ScalableTopicDAG dag;
    dag.set_epoch(2);
    addSegment(dag, 0, 0x0000, 0x7FFF, proto::ACTIVE, "persistent://public/default/orders-partition-0");
    addSegment(dag, 1, 0x8000, 0xFFFF, proto::ACTIVE);  // one regular segment -> range routing
    auto layout = SegmentLayout::fromProto(dag, kTopic);
    ASSERT_TRUE(layout);
    SegmentRouter router;

    std::string keyInUpperHalf = findKeyInRange(0x8000, 0xFFFF);
    auto id = router.route(keyInUpperHalf, *layout);
    ASSERT_TRUE(id);
    ASSERT_EQ(*id, 1u);
}

TEST(SegmentRouterTest, testRoundRobinCyclesAllActiveSegments) {
    auto layout = fourSegmentLayout();
    ASSERT_TRUE(layout);
    SegmentRouter router;
    std::set<std::uint64_t> seen;
    for (int i = 0; i < 8; i++) {
        auto id = router.routeRoundRobin(*layout);
        ASSERT_TRUE(id);
        seen.insert(*id);
    }
    ASSERT_EQ(seen.size(), 4u);  // every active segment hit
}

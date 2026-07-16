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
#include <pulsar/MessageId.h>
#include <pulsar/ProducerConfiguration.h>

#include <cstdint>
#include <string>

#include "lib/st/MessageIdImpl.h"
#include "lib/st/StProducerImpl.h"

namespace pulsar::st {

// Test-only access to StProducerImpl's private, broker-independent helpers. These read
// only config_ and the segment argument (never the classic client), so they can run with
// a null client and no connection.
struct StProducerTestAccess {
    static pulsar::ProducerConfiguration segmentConfig(StProducerImpl& producer, const Segment& segment) {
        return producer.buildSegmentConfiguration(segment);
    }
    static pulsar::ProducerConfiguration::ProducerAccessMode toClassic(ProducerAccessMode mode) {
        return StProducerImpl::toClassicAccessMode(mode);
    }
    static bool segmentGone(pulsar::Result result, const std::string& message) {
        return StProducerImpl::isSegmentGoneError(result, message);
    }
};

namespace {

ProducerConfig baseConfig() {
    ProducerConfig config;
    config.topic = "topic://public/default/orders";
    config.producerName = "orders-producer";
    return config;
}

Segment normalSegment(std::uint64_t id) {
    Segment segment;
    segment.segmentId = id;
    segment.segmentTopicName = "segment://public/default/orders/0000-ffff-" + std::to_string(id);
    return segment;
}

Segment legacySegment(std::uint64_t id) {
    Segment segment = normalSegment(id);
    segment.legacyTopicName = "persistent://public/default/orders-partition-" + std::to_string(id);
    return segment;
}

// Guards the ProducerConfiguration shallow-copy defect: pulsar::ProducerConfiguration's copy
// constructor shares its impl, so building a second segment's config by copy-then-mutate would
// clobber the first. Each segment must get an independent, freshly-built config.
TEST(StProducerTest, testPerSegmentConfigsAreIsolated) {
    StProducerImpl producer(nullptr, baseConfig());

    auto confA = StProducerTestAccess::segmentConfig(producer, normalSegment(3));
    auto confB = StProducerTestAccess::segmentConfig(producer, legacySegment(7));

    // If the configs shared state, building B would rename A too.
    EXPECT_EQ(confA.getProducerName(), "orders-producer-seg-3");
    EXPECT_EQ(confB.getProducerName(), "orders-producer-seg-7");

    // The legacy managed-metadata marker belongs to the legacy segment only.
    EXPECT_EQ(confB.getProperties().count("__pulsar.v5.managed"), 1u);
    EXPECT_EQ(confA.getProperties().count("__pulsar.v5.managed"), 0u);
}

// The st and classic ProducerAccessMode enums do NOT share numeric values (st orders
// ExclusiveWithFencing before WaitForExclusive; classic is the reverse), so a cast would be
// wrong — the mapping must go by name.
TEST(StProducerTest, testAccessModeMappedByName) {
    using Classic = pulsar::ProducerConfiguration;
    EXPECT_EQ(StProducerTestAccess::toClassic(ProducerAccessMode::Shared), Classic::Shared);
    EXPECT_EQ(StProducerTestAccess::toClassic(ProducerAccessMode::Exclusive), Classic::Exclusive);
    EXPECT_EQ(StProducerTestAccess::toClassic(ProducerAccessMode::ExclusiveWithFencing),
              Classic::ExclusiveWithFencing);
    EXPECT_EQ(StProducerTestAccess::toClassic(ProducerAccessMode::WaitForExclusive),
              Classic::WaitForExclusive);
}

TEST(StProducerTest, testSegmentGoneClassification) {
    EXPECT_TRUE(StProducerTestAccess::segmentGone(ResultTopicTerminated, ""));
    EXPECT_TRUE(StProducerTestAccess::segmentGone(ResultAlreadyClosed, ""));
    // A not-ready layout is a transient routing failure, NOT a gone segment — treating it as
    // gone would spin the retry loop instead of failing terminally.
    EXPECT_FALSE(StProducerTestAccess::segmentGone(ResultServiceUnitNotReady, ""));
    EXPECT_FALSE(StProducerTestAccess::segmentGone(ResultConnectError, ""));
}

TEST(StProducerTest, testMintedMessageIdCarriesSegmentId) {
    MessageId id = MessageIdFactory::create(pulsar::MessageId::earliest(), 42);
    ASSERT_TRUE(static_cast<bool>(id));
    const auto& impl = MessageIdFactory::impl(id);
    ASSERT_TRUE(impl);
    EXPECT_EQ(impl->segmentId, 42);
    EXPECT_NE(impl->segmentId, MessageIdImpl::kNoSegment);
}

}  // namespace
}  // namespace pulsar::st

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
// End-to-end producer tests against a real scalable-topics broker. These are gated on
// the PULSAR_ST_E2E environment variable so the ordinary (broker-free) unit-test run
// skips them; the docker harness sets it. The broker URL defaults to the standard test
// service and can be overridden with PULSAR_ST_E2E_SERVICE_URL.
#include <gtest/gtest.h>
#include <pulsar/st/Client.h>

#include <cstdlib>
#include <set>
#include <string>
#include <vector>

#include "lib/st/MessageIdImpl.h"

using namespace pulsar::st;

namespace {

bool e2eEnabled() { return std::getenv("PULSAR_ST_E2E") != nullptr; }

std::string serviceUrl() {
    const char* url = std::getenv("PULSAR_ST_E2E_SERVICE_URL");
    return url != nullptr ? url : "pulsar://localhost:6650";
}

// The segment id carried by a produced message id (the whole point of the mapping).
std::int64_t segmentIdOf(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    return impl ? impl->segmentId : MessageIdImpl::kNoSegment;
}

TEST(StProducerE2ETest, testProduceKeyedAndKeyless) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult =
        client.newProducer(Schema<std::string>{}).topic("topic://public/default/st-e2e-produce").create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    constexpr int kKeyed = 20;
    for (int i = 0; i < kKeyed; i++) {
        auto sent =
            producer.newMessage().key("key-" + std::to_string(i % 4)).value("v-" + std::to_string(i)).send();
        ASSERT_TRUE(sent) << "keyed send " << i << " failed: " << sent.error();
        ASSERT_TRUE(static_cast<bool>(*sent)) << "keyed send " << i << " returned an empty message id";
        EXPECT_GE(segmentIdOf(*sent), 0) << "keyed send " << i << " has no real segment id";
    }

    constexpr int kKeyless = 5;
    for (int i = 0; i < kKeyless; i++) {
        auto sent = producer.send("keyless-" + std::to_string(i));
        ASSERT_TRUE(sent) << "keyless send " << i << " failed: " << sent.error();
        EXPECT_GE(segmentIdOf(*sent), 0) << "keyless send " << i << " has no real segment id";
    }

    // After N sends, the last sequence id reflects them.
    auto lastSeq = producer.lastSequenceId();
    ASSERT_TRUE(lastSeq.has_value());
    EXPECT_GE(*lastSeq, kKeyed + kKeyless - 1);

    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

TEST(StProducerE2ETest, testAsyncSendsAllComplete) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult =
        client.newProducer(Schema<std::string>{}).topic("topic://public/default/st-e2e-produce").create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // Fire many async sends across keys (fanning out to segments) without blocking, then await
    // the whole batch — exercises the in-flight tracking and flush.
    constexpr int kCount = 50;
    std::vector<Future<MessageId>> futures;
    futures.reserve(kCount);
    for (int i = 0; i < kCount; i++) {
        futures.push_back(producer.newMessage()
                              .key("k-" + std::to_string(i % 8))
                              .value("a-" + std::to_string(i))
                              .sendAsync());
    }
    for (int i = 0; i < kCount; i++) {
        auto result = futures[i].get();
        ASSERT_TRUE(result) << "async send " << i << " failed: " << result.error();
        EXPECT_GE(segmentIdOf(*result), 0);
    }
    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

// Produce to a topic the harness has split into two active segments and assert keys actually
// distribute across both — the single-segment tests above route every key to the one segment,
// so they never exercise cross-segment routing.
TEST(StProducerE2ETest, testProduceAcrossSplitSegments) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult =
        client.newProducer(Schema<std::string>{}).topic("topic://public/default/st-e2e-split").create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // With 60 distinct keys over two half-range segments, both are hit with overwhelming
    // probability (all landing on one segment would be ~2^-60).
    std::set<std::int64_t> segments;
    for (int i = 0; i < 60; i++) {
        auto sent =
            producer.newMessage().key("key-" + std::to_string(i)).value("v-" + std::to_string(i)).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
        segments.insert(segmentIdOf(*sent));
    }
    EXPECT_GE(segments.size(), 2u) << "keys did not distribute across the split segments";

    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

// Produce to a topic the harness has split and then merged back together. A merge seals the two
// children and creates one full-range active segment, so every key routes to that merged segment;
// this exercises the layout parser and router on a DAG that carries several sealed segments.
TEST(StProducerE2ETest, testProduceAfterMerge) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult =
        client.newProducer(Schema<std::string>{}).topic("topic://public/default/st-e2e-merge").create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    std::set<std::int64_t> segments;
    for (int i = 0; i < 30; i++) {
        auto sent =
            producer.newMessage().key("key-" + std::to_string(i)).value("v-" + std::to_string(i)).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
        segments.insert(segmentIdOf(*sent));
    }
    EXPECT_EQ(segments.size(), 1u) << "every key should route to the single active merged segment";

    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

// Split a segment out from under a live producer and assert publishing keeps working. This is the
// segment-gone path: sends routed to the just-sealed segment fail with TopicTerminated and the
// producer retries + re-routes onto the new layout the DAG watch delivers. The split is triggered
// through the admin command the harness passes in PULSAR_ST_E2E_SPLIT_CMD (so the container name
// stays out of the test); the exact moment a send races the seal is timing-dependent, but every
// send must ultimately succeed and publishing must reach the post-split segments.
TEST(StProducerE2ETest, testProduceContinuesAcrossLiveSplit) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";
    const char* splitCommand = std::getenv("PULSAR_ST_E2E_SPLIT_CMD");
    if (splitCommand == nullptr) GTEST_SKIP() << "PULSAR_ST_E2E_SPLIT_CMD (admin split command) not set";

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult =
        client.newProducer(Schema<std::string>{}).topic("topic://public/default/st-e2e-live-split").create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // Warm up so a per-segment producer is cached on the single active segment before it is sealed.
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(producer.send("warmup-" + std::to_string(i)));
    }

    // Seal the active segment (split it into two children) while publishing continues.
    ASSERT_EQ(std::system(splitCommand), 0) << "split command failed: " << splitCommand;

    // Burst of async sends spanning the split-propagation window; the producer must transparently
    // retry the ones that hit the sealed segment and re-route onto the new layout.
    constexpr int kBurst = 200;
    std::vector<Future<MessageId>> futures;
    futures.reserve(kBurst);
    for (int i = 0; i < kBurst; i++) {
        futures.push_back(
            producer.newMessage().key("k-" + std::to_string(i)).value("b-" + std::to_string(i)).sendAsync());
    }
    std::set<std::int64_t> segments;
    for (int i = 0; i < kBurst; i++) {
        auto result = futures[i].get();
        ASSERT_TRUE(result) << "burst send " << i << " failed across the live split: " << result.error();
        segments.insert(segmentIdOf(*result));
    }
    // Publishing reached at least one segment created by the split (id > 0; the original was 0).
    EXPECT_GT(*segments.rbegin(), 0) << "no send reached a post-split segment";

    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

}  // namespace

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
// End-to-end producer tests against a real scalable-topics broker. These are gated on the
// PULSAR_ST_E2E environment variable so the ordinary (broker-free) unit-test run skips them; the
// docker harness sets it. Each test is self-contained: it creates its own scalable topic and drives
// any split/merge itself, through the admin REST API, so no external CLI setup is needed. The broker
// URLs default to the standard test service and can be overridden with PULSAR_ST_E2E_SERVICE_URL /
// PULSAR_ST_E2E_ADMIN_URL.
#include <gtest/gtest.h>
#include <pulsar/st/Client.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "lib/st/MessageIdImpl.h"
#include "tests/HttpHelper.h"

using namespace pulsar::st;

namespace {

bool e2eEnabled() { return std::getenv("PULSAR_ST_E2E") != nullptr; }

std::string serviceUrl() {
    const char* url = std::getenv("PULSAR_ST_E2E_SERVICE_URL");
    return url != nullptr ? url : "pulsar://localhost:6650";
}

std::string adminUrl() {
    const char* url = std::getenv("PULSAR_ST_E2E_ADMIN_URL");
    return url != nullptr ? url : "http://localhost:8080";
}

// A fresh topic name per test run so tests never collide with one another or with a topic left on a
// reused broker (the same convention the classic tests use).
std::string uniqueName(const std::string& prefix) {
    static int counter = 0;
    return prefix + "-" + std::to_string(std::time(nullptr)) + "-" + std::to_string(counter++);
}

std::string topicUrl(const std::string& name) { return "topic://public/default/" + name; }

// The admin REST base for a scalable topic under public/default.
std::string scalablePath(const std::string& name) {
    return adminUrl() + "/admin/v2/scalable/public/default/" + name;
}

// Create a scalable topic with the given number of initial segments. Retries while the
// scalable-topics controller finishes coming up after broker start (only the first test waits).
bool createScalableTopic(const std::string& name, int numInitialSegments = 1) {
    const std::string url = scalablePath(name) + "?numInitialSegments=" + std::to_string(numInitialSegments);
    for (int attempt = 0; attempt < 30; attempt++) {
        const int code = makePutRequest(url, "");
        if (code >= 200 && code < 300) return true;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return false;
}

// Split a segment into two half-range children (POST .../split/{segmentId}).
bool splitSegment(const std::string& name, std::int64_t segmentId) {
    const int code = makePostRequest(scalablePath(name) + "/split/" + std::to_string(segmentId), "");
    return code >= 200 && code < 300;
}

// Merge two segments back into one full-range child (POST .../merge/{segmentId1}/{segmentId2}).
bool mergeSegments(const std::string& name, std::int64_t segmentId1, std::int64_t segmentId2) {
    const int code = makePostRequest(
        scalablePath(name) + "/merge/" + std::to_string(segmentId1) + "/" + std::to_string(segmentId2), "");
    return code >= 200 && code < 300;
}

// The segment id carried by a produced message id (the whole point of the mapping).
std::int64_t segmentIdOf(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    return impl ? impl->segmentId : MessageIdImpl::kNoSegment;
}

TEST(StProducerE2ETest, testProduceKeyedAndKeyless) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string topic = uniqueName("st-e2e-produce");
    ASSERT_TRUE(createScalableTopic(topic)) << "failed to create scalable topic " << topic;

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topicUrl(topic)).create();
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

    const std::string topic = uniqueName("st-e2e-async");
    ASSERT_TRUE(createScalableTopic(topic)) << "failed to create scalable topic " << topic;

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topicUrl(topic)).create();
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

// Split a topic into two active segments (via REST) and assert keys actually distribute across both
// children — the single-segment tests above route every key to the one segment, so they never
// exercise cross-segment routing.
TEST(StProducerE2ETest, testProduceAcrossSplitSegments) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string topic = uniqueName("st-e2e-split");
    ASSERT_TRUE(createScalableTopic(topic)) << "failed to create scalable topic " << topic;
    ASSERT_TRUE(splitSegment(topic, 0)) << "failed to split segment 0 of " << topic;

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topicUrl(topic)).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // The split propagates to the producer's DAG watch asynchronously, so early sends may still land
    // on the original segment 0; keep publishing distinct keys until both half-range children (id > 0)
    // are hit. Once the layout arrives both are hit with overwhelming probability (all landing on one
    // is ~2^-k); the bound just stops a broken layout from looping forever.
    std::set<std::int64_t> children;
    for (int i = 0; i < 300 && children.size() < 2; i++) {
        auto sent =
            producer.newMessage().key("key-" + std::to_string(i)).value("v-" + std::to_string(i)).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
        if (segmentIdOf(*sent) > 0) children.insert(segmentIdOf(*sent));
    }
    EXPECT_GE(children.size(), 2u) << "keys did not distribute across the two split children";

    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

// Split a topic and then merge it back together (both via REST). A merge seals the two children and
// creates one full-range active segment, so every key routes to that merged segment; this exercises
// the layout parser and router on a DAG that carries several sealed segments.
TEST(StProducerE2ETest, testProduceAfterMerge) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string topic = uniqueName("st-e2e-merge");
    ASSERT_TRUE(createScalableTopic(topic)) << "failed to create scalable topic " << topic;
    ASSERT_TRUE(splitSegment(topic, 0)) << "failed to split segment 0 of " << topic;  // 0 -> children 1, 2
    ASSERT_TRUE(mergeSegments(topic, 1, 2)) << "failed to merge segments 1, 2 of " << topic;  // -> child 3

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topicUrl(topic)).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // Wait for the merged layout to reach the producer: publish until a send lands on the merged
    // full-range child, whose id is past the split children (1 and 2).
    std::int64_t merged = -1;
    for (int i = 0; i < 300 && merged < 0; i++) {
        auto sent = producer.send("settle-" + std::to_string(i));
        ASSERT_TRUE(sent) << "settle send " << i << " failed: " << sent.error();
        if (segmentIdOf(*sent) > 2) merged = segmentIdOf(*sent);
    }
    ASSERT_GE(merged, 0) << "the merge did not propagate to the producer";

    // Every key now routes to that single merged segment.
    std::set<std::int64_t> segments;
    for (int i = 0; i < 30; i++) {
        auto sent =
            producer.newMessage().key("key-" + std::to_string(i)).value("v-" + std::to_string(i)).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
        segments.insert(segmentIdOf(*sent));
    }
    EXPECT_EQ(segments.size(), 1u) << "every key should route to the single active merged segment";
    EXPECT_EQ(*segments.begin(), merged) << "keys routed to a segment other than the merged one";

    EXPECT_TRUE(producer.flush());
    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(client.close());
}

// Split a segment out from under a live producer (via REST) and assert publishing keeps working.
// This is the segment-gone path: sends routed to the just-sealed segment fail with TopicTerminated
// and the producer retries + re-routes onto the new layout the DAG watch delivers. The exact moment
// a send races the seal is timing-dependent, but every send must ultimately succeed and publishing
// must reach the post-split segments.
TEST(StProducerE2ETest, testProduceContinuesAcrossLiveSplit) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string topic = uniqueName("st-e2e-live-split");
    ASSERT_TRUE(createScalableTopic(topic)) << "failed to create scalable topic " << topic;

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topicUrl(topic)).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // Warm up so a per-segment producer is cached on the single active segment before it is sealed.
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(producer.send("warmup-" + std::to_string(i)));
    }

    // Seal the active segment (split it into two children) while publishing continues.
    ASSERT_TRUE(splitSegment(topic, 0)) << "failed to split segment 0 of " << topic;

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

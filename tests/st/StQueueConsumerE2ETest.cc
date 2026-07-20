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
// End-to-end queue-consumer tests against a real scalable-topics broker: a produce -> consume
// round-trip over a Shared subscription. Gated on the PULSAR_ST_E2E environment variable so the
// ordinary (broker-free) unit-test run skips them; the docker harness sets it. The broker URL
// defaults to the standard test service and can be overridden with PULSAR_ST_E2E_SERVICE_URL.
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

// The segment id carried by a received message id — the fan-in stamps it on every message.
std::int64_t segmentIdOf(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    return impl ? impl->segmentId : MessageIdImpl::kNoSegment;
}

// Give each message plenty of time to arrive; a healthy broker delivers in milliseconds.
constexpr std::chrono::seconds kReceiveTimeout{20};

TEST(StQueueConsumerE2ETest, testProduceThenConsumeRoundTrip) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string name = uniqueName("st-e2e-queue");
    ASSERT_TRUE(createScalableTopic(name)) << "failed to create scalable topic " << name;
    const std::string topic = topicUrl(name);

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    // Subscribe first (Earliest) so the subscription and its per-segment cursors exist before we
    // publish — every produced message is then guaranteed to be delivered.
    auto consumerResult = client.newQueueConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(consumerResult) << consumerResult.error();
    QueueConsumer<std::string> consumer = std::move(consumerResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topic).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    constexpr int kCount = 25;
    std::set<std::string> produced;
    for (int i = 0; i < kCount; i++) {
        std::string value = "v-" + std::to_string(i);
        auto sent = producer.newMessage().key("key-" + std::to_string(i % 4)).value(value).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
        produced.insert(value);
    }
    ASSERT_TRUE(producer.flush());

    // Receive exactly kCount messages; a Shared subscription gives no cross-segment order, so
    // compare the received payloads as a set rather than a sequence.
    std::set<std::string> received;
    for (int i = 0; i < kCount; i++) {
        auto message = consumer.receive(kReceiveTimeout);
        ASSERT_TRUE(message) << "receive " << i << " failed: " << message.error();
        EXPECT_GE(segmentIdOf(message->id()), 0) << "received message " << i << " has no real segment id";
        received.insert(message->value());
        consumer.acknowledge(message->id());
    }
    EXPECT_EQ(received, produced) << "consumed payloads did not match what was produced";

    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(consumer.close());
    EXPECT_TRUE(client.close());
}

// Consume from a topic split (via REST) into two active segments and assert the messages actually
// arrive from both — this is the fan-in the queue consumer exists for: one Shared subscription
// multiplexed across a per-segment classic consumer each, drained through the mux receive queue.
// The single-segment round-trip above never exercises multi-segment fan-in.
TEST(StQueueConsumerE2ETest, testConsumeAcrossSplitSegments) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string name = uniqueName("st-e2e-queue-split");
    ASSERT_TRUE(createScalableTopic(name)) << "failed to create scalable topic " << name;
    ASSERT_TRUE(splitSegment(name, 0)) << "failed to split segment 0 of " << name;
    const std::string topic = topicUrl(name);

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto consumerResult = client.newQueueConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(consumerResult) << consumerResult.error();
    QueueConsumer<std::string> consumer = std::move(consumerResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topic).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    // 60 distinct keys over two half-range segments hit both with overwhelming probability.
    constexpr int kCount = 60;
    std::set<std::string> produced;
    for (int i = 0; i < kCount; i++) {
        std::string value = "v-" + std::to_string(i);
        auto sent = producer.newMessage().key("key-" + std::to_string(i)).value(value).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
        produced.insert(value);
    }
    ASSERT_TRUE(producer.flush());

    std::set<std::string> received;
    std::set<std::int64_t> segments;
    for (int i = 0; i < kCount; i++) {
        auto message = consumer.receive(kReceiveTimeout);
        ASSERT_TRUE(message) << "receive " << i << " failed: " << message.error();
        segments.insert(segmentIdOf(message->id()));
        received.insert(message->value());
        consumer.acknowledge(message->id());
    }
    EXPECT_EQ(received, produced) << "consumed payloads did not match what was produced";
    EXPECT_GE(segments.size(), 2u) << "messages did not fan in from both split segments";

    EXPECT_TRUE(producer.close());
    EXPECT_TRUE(consumer.close());
    EXPECT_TRUE(client.close());
}

}  // namespace

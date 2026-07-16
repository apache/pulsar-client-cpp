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

// The segment id carried by a received message id — the fan-in stamps it on every message.
std::int64_t segmentIdOf(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    return impl ? impl->segmentId : MessageIdImpl::kNoSegment;
}

// Give each message plenty of time to arrive; a healthy broker delivers in milliseconds.
constexpr std::chrono::seconds kReceiveTimeout{20};

TEST(StQueueConsumerE2ETest, testProduceThenConsumeRoundTrip) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";
    const std::string topic = "topic://public/default/st-e2e-queue";

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

// Consume from a topic the harness has split into two active segments and assert the messages
// actually arrive from both — this is the fan-in the queue consumer exists for: one Shared
// subscription multiplexed across a per-segment classic consumer each, drained through the mux
// receive queue. The single-segment round-trip above never exercises multi-segment fan-in.
TEST(StQueueConsumerE2ETest, testConsumeAcrossSplitSegments) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";
    const std::string topic = "topic://public/default/st-e2e-queue-split";

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

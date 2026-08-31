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
// End-to-end stream-consumer tests against a real scalable-topics broker: ordered
// consumption over the segment DAG through the controller-assigned Exclusive
// per-segment consumers, and the position-vector cumulative acknowledgment. Gated on
// PULSAR_ST_E2E like the other scalable e2e suites.
#include <gtest/gtest.h>
#include <pulsar/st/Client.h>

#include <chrono>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "lib/st/MessageIdImpl.h"
#include "tests/st/StE2EAdmin.h"

using namespace pulsar::st;
using namespace st_e2e;

namespace {

// The segment id carried by a received message id.
std::int64_t segmentIdOf(const MessageId& id) {
    const auto& impl = MessageIdFactory::impl(id);
    return impl ? impl->segmentId : MessageIdImpl::kNoSegment;
}

// Give each message plenty of time to arrive; a healthy broker delivers in milliseconds.
constexpr std::chrono::seconds kReceiveTimeout{20};

// A single-segment topic delivers in total order through one Exclusive consumer, and
// one cumulative ack of the last message settles the whole stream: a second consumer
// on the same subscription receives nothing.
TEST(StStreamConsumerE2ETest, testOrderedRoundTripAndCumulativeAck) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string name = uniqueName("st-e2e-stream");
    ASSERT_TRUE(createScalableTopic(name)) << "failed to create scalable topic " << name;
    const std::string topic = topicUrl(name);

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto consumerResult = client.newStreamConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(consumerResult) << consumerResult.error();
    StreamConsumer<std::string> consumer = std::move(consumerResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topic).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    constexpr int kCount = 25;
    for (int i = 0; i < kCount; i++) {
        auto sent =
            producer.newMessage().key("key-" + std::to_string(i % 4)).value("v-" + std::to_string(i)).send();
        ASSERT_TRUE(sent) << "send " << i << " failed: " << sent.error();
    }
    ASSERT_TRUE(producer.flush());
    ASSERT_TRUE(producer.close());

    // One segment, one Exclusive consumer: delivery is the publish order, exactly.
    MessageId lastId = MessageId::earliest();
    for (int i = 0; i < kCount; i++) {
        auto message = consumer.receive(kReceiveTimeout);
        ASSERT_TRUE(message) << "receive " << i << " failed: " << message.error();
        EXPECT_EQ(message->value(), "v-" + std::to_string(i)) << "out of order at position " << i;
        EXPECT_EQ(segmentIdOf(message->id()), 0);
        lastId = message->id();
    }
    // One cumulative ack of the last message settles everything delivered.
    consumer.acknowledgeCumulative(lastId);
    ASSERT_TRUE(consumer.close());

    auto verifierResult = client.newStreamConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(verifierResult) << verifierResult.error();
    StreamConsumer<std::string> verifier = std::move(verifierResult).value();
    auto redelivered = verifier.receive(std::chrono::seconds(3));
    ASSERT_FALSE(redelivered) << "cumulative ack did not stick: \"" << redelivered->value()
                              << "\" was redelivered";
    EXPECT_EQ(redelivered.error().result, pulsar::ResultTimeout);
    EXPECT_TRUE(verifier.close());
    EXPECT_TRUE(client.close());
}

// The DAG-replay scenario: everything produced before a split sits in the sealed
// parent, everything after lands on the children — and the broker withholds the
// children from the assignment until the parent is drained FOR THIS SUBSCRIPTION.
// Acking as we go is what lets the parent's backlog reach zero and unblock the
// children; every parent message must arrive before any child message.
TEST(StStreamConsumerE2ETest, testDrainsSealedParentBeforeChildren) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string name = uniqueName("st-e2e-stream-replay");
    ASSERT_TRUE(createScalableTopic(name)) << "failed to create scalable topic " << name;
    const std::string topic = topicUrl(name);

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    // Create the durable subscription up front (and detach) so the pre-split backlog
    // is retained for it.
    {
        auto subscriberResult = client.newStreamConsumer(Schema<std::string>{})
                                    .topic(topic)
                                    .subscriptionName("sub")
                                    .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                                    .subscribe();
        ASSERT_TRUE(subscriberResult) << subscriberResult.error();
        StreamConsumer<std::string> subscriber = std::move(subscriberResult).value();
        ASSERT_TRUE(subscriber.close());
    }

    auto producerResult = client.newProducer(Schema<std::string>{}).topic(topic).create();
    ASSERT_TRUE(producerResult) << producerResult.error();
    Producer<std::string> producer = std::move(producerResult).value();

    constexpr int kBefore = 60;
    std::set<std::string> producedBefore;
    for (int i = 0; i < kBefore; i++) {
        std::string value = "before-" + std::to_string(i);
        auto sent = producer.newMessage().key("key-" + std::to_string(i)).value(value).send();
        ASSERT_TRUE(sent) << "pre-split send " << i << " failed: " << sent.error();
        producedBefore.insert(std::move(value));
    }
    ASSERT_TRUE(producer.flush());

    // Seal the parent with its backlog behind, then publish the post-split batch onto
    // the children.
    ASSERT_TRUE(splitSegment(name, 0)) << "failed to split segment 0 of " << name;
    constexpr int kAfter = 40;
    std::set<std::string> producedAfter;
    for (int i = 0; i < kAfter; i++) {
        std::string value = "after-" + std::to_string(i);
        auto sent = producer.newMessage().key("key-" + std::to_string(i)).value(value).send();
        ASSERT_TRUE(sent) << "post-split send " << i << " failed: " << sent.error();
        producedAfter.insert(std::move(value));
    }
    ASSERT_TRUE(producer.flush());
    ASSERT_TRUE(producer.close());

    auto consumerResult = client.newStreamConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(consumerResult) << consumerResult.error();
    StreamConsumer<std::string> consumer = std::move(consumerResult).value();

    // Ack cumulatively as we go: the broker decides the parent is drained by this
    // subscription's backlog reaching zero, so deferring acks to the end would keep
    // the children withheld forever.
    std::set<std::string> receivedBefore;
    std::set<std::string> receivedAfter;
    bool sawChild = false;
    for (int i = 0; i < kBefore + kAfter; i++) {
        auto message = consumer.receive(kReceiveTimeout);
        ASSERT_TRUE(message) << "receive " << i << " failed: " << message.error();
        if (segmentIdOf(message->id()) == 0) {
            EXPECT_FALSE(sawChild) << "parent message \"" << message->value()
                                   << "\" arrived after a child message — DAG order broken";
            receivedBefore.insert(std::string(message->value()));
        } else {
            sawChild = true;
            receivedAfter.insert(std::string(message->value()));
        }
        consumer.acknowledgeCumulative(message->id());
    }
    EXPECT_EQ(receivedBefore, producedBefore) << "the sealed parent's backlog did not fully arrive";
    EXPECT_EQ(receivedAfter, producedAfter) << "the post-split children's messages did not fully arrive";

    EXPECT_TRUE(consumer.close());
    EXPECT_TRUE(client.close());
}

// Two initial segments (no parents, so both are assigned immediately): drain both via
// batch receives without acking, then acknowledge only the very last message — its
// position vector must advance BOTH segments' cursors.
TEST(StStreamConsumerE2ETest, testCumulativeAckCoversAllSegments) {
    if (!e2eEnabled()) GTEST_SKIP() << "set PULSAR_ST_E2E=1 to run against a scalable-topics broker";

    const std::string name = uniqueName("st-e2e-stream-vector");
    ASSERT_TRUE(createScalableTopic(name, /*numInitialSegments*/ 2)) << "failed to create " << name;
    const std::string topic = topicUrl(name);

    auto clientResult = PulsarClient::builder().serviceUrl(serviceUrl()).build();
    ASSERT_TRUE(clientResult) << clientResult.error();
    PulsarClient client = std::move(clientResult).value();

    auto consumerResult = client.newStreamConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(consumerResult) << consumerResult.error();
    StreamConsumer<std::string> consumer = std::move(consumerResult).value();

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
        produced.insert(std::move(value));
    }
    ASSERT_TRUE(producer.flush());
    ASSERT_TRUE(producer.close());

    // Drain through batch receives, acking nothing along the way.
    std::set<std::string> received;
    std::set<std::int64_t> segments;
    MessageId lastId = MessageId::earliest();
    while (static_cast<int>(received.size()) < kCount) {
        auto batch = consumer.receiveMulti(20, std::chrono::seconds(5));
        ASSERT_TRUE(batch) << "batch receive failed: " << batch.error();
        ASSERT_FALSE(batch->empty()) << "drain stalled at " << received.size() << " of " << kCount;
        for (const auto& message : *batch) {
            segments.insert(segmentIdOf(message.id()));
            received.insert(std::string(message.value()));
            lastId = message.id();
        }
    }
    EXPECT_EQ(received, produced);
    EXPECT_GE(segments.size(), 2u) << "messages did not arrive from both segments";

    // One ack: its position vector advances every segment's cursor.
    consumer.acknowledgeCumulative(lastId);
    ASSERT_TRUE(consumer.close());

    auto verifierResult = client.newStreamConsumer(Schema<std::string>{})
                              .topic(topic)
                              .subscriptionName("sub")
                              .subscriptionInitialPosition(SubscriptionInitialPosition::Earliest)
                              .subscribe();
    ASSERT_TRUE(verifierResult) << verifierResult.error();
    StreamConsumer<std::string> verifier = std::move(verifierResult).value();
    auto redelivered = verifier.receive(std::chrono::seconds(3));
    ASSERT_FALSE(redelivered) << "the position vector did not cover every segment: \"" << redelivered->value()
                              << "\" was redelivered";
    EXPECT_EQ(redelivered.error().result, pulsar::ResultTimeout);
    EXPECT_TRUE(verifier.close());
    EXPECT_TRUE(client.close());
}

}  // namespace

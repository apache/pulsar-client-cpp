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
#include <pulsar/Client.h>

#include <chrono>

#include "ThreadSafeMessages.h"
#include "lib/LogUtils.h"

static const std::string lookupUrl = "pulsar://localhost:6650";

DECLARE_LOG_OBJECT()

using namespace pulsar;

extern std::string unique_str();

TEST(MultiTopicsConsumerTest, testSeekToNewerPosition) {
    const std::string topicPrefix = "multi-topics-consumer-seek-to-newer-position";
    Client client{lookupUrl};
    std::vector<std::string> topics{topicPrefix + unique_str(), topicPrefix + unique_str()};
    Producer producer1;
    ASSERT_EQ(ResultOk, client.createProducer(topics[0], producer1));
    Producer producer2;
    ASSERT_EQ(ResultOk, client.createProducer(topics[1], producer2));
    producer1.send(MessageBuilder().setContent("1-0").build());
    producer2.send(MessageBuilder().setContent("2-0").build());
    producer1.send(MessageBuilder().setContent("1-1").build());
    producer2.send(MessageBuilder().setContent("2-1").build());

    Consumer consumer;
    ConsumerConfiguration conf;
    conf.setSubscriptionInitialPosition(InitialPositionEarliest);
    ASSERT_EQ(ResultOk, client.subscribe(topics, "sub", conf, consumer));
    std::vector<int64_t> timestamps;
    Message msg;
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        timestamps.emplace_back(msg.getPublishTimestamp());
    }
    std::sort(timestamps.begin(), timestamps.end());
    const auto timestamp = timestamps[2];
    consumer.close();

    ThreadSafeMessages messages{2};

    // Test synchronous receive after seek
    ASSERT_EQ(ResultOk, client.subscribe(topics, "sub-2", conf, consumer));
    consumer.seek(timestamp);
    for (int i = 0; i < 2; i++) {
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        messages.add(msg);
    }
    ASSERT_EQ(messages.getSortedValues(), (std::vector<std::string>{"1-1", "2-1"}));
    consumer.close();

    // Test asynchronous receive after seek
    ASSERT_EQ(ResultOk, client.subscribe(topics, "sub-3", conf, consumer));
    messages.clear();
    consumer.seek(timestamp);
    for (int i = 0; i < 2; i++) {
        consumer.receiveAsync([&messages](Result result, const Message& msg) {
            if (result == ResultOk) {
                messages.add(msg);
            } else {
                LOG_ERROR("Failed to receive: " << result);
            }
        });
    }
    ASSERT_TRUE(messages.wait(std::chrono::seconds(3)));
    ASSERT_EQ(messages.getSortedValues(), (std::vector<std::string>{"1-1", "2-1"}));
    consumer.close();

    // Test message listener
    conf.setMessageListener([&messages](Consumer consumer, Message msg) { messages.add(msg); });
    messages.clear();
    messages.setMinNumMsgs(4);
    ASSERT_EQ(ResultOk, client.subscribe(topics, "sub-4", conf, consumer));
    ASSERT_TRUE(messages.wait(std::chrono::seconds(3)));
    ASSERT_EQ(messages.getSortedValues(), (std::vector<std::string>{"1-0", "1-1", "2-0", "2-1"}));
    messages.clear();
    messages.setMinNumMsgs(2);
    consumer.seek(timestamp);
    ASSERT_TRUE(messages.wait(std::chrono::seconds(3)));
    ASSERT_EQ(messages.getSortedValues(), (std::vector<std::string>{"1-1", "2-1"}));

    client.close();
}

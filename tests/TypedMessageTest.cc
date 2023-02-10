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
#include <pulsar/MessageBuilder.h>
#include <pulsar/TypedMessage.h>
#include <pulsar/TypedMessageBuilder.h>

#include <mutex>
#include <vector>

#include "WaitUtils.h"
#include "lib/Latch.h"

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";

extern std::string unique_str();

using IntMessageBuilder = TypedMessageBuilder<int>;
static auto intEncoder = [](int x) { return std::to_string(x); };
static auto intDecoder = [](const char* data, size_t size) { return std::stoi(std::string(data, size)); };

TEST(TypedMessageTest, testReceive) {
    const auto topic = "typed-message-test-receive-" + unique_str();
    Client client(lookupUrl);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumer));

    std::vector<MessageId> messageIds;
    constexpr int numMessages = 100;

    for (int i = 0; i < numMessages; i++) {
        MessageId messageId;
        auto msg = TypedMessageBuilder<int>{intEncoder}.setValue(i).build();
        ASSERT_EQ(ResultOk, producer.send(msg, messageId));
        messageIds.emplace_back(messageId);
    }
    for (int i = 0; i < numMessages; i++) {
        TypedMessage<int> msg;
        // ensure the thread safety for `msg`, which could be modified in the callback of `receiveAsync`
        std::mutex msgMutex;
        if (i % 3 == 0) {
            ASSERT_EQ(ResultOk, consumer.receive(msg, intDecoder));
        } else if (i % 3 == 1) {
            ASSERT_EQ(ResultOk, consumer.receive(msg, 3000, intDecoder));
        } else {
            Latch latch{1};
            consumer.receiveAsync(
                [&latch, &msg, &msgMutex](Result result, const TypedMessage<int>& receivedMsg) {
                    ASSERT_EQ(ResultOk, result);
                    {
                        std::lock_guard<std::mutex> lock{msgMutex};
                        msg = TypedMessage<int>{receivedMsg, intDecoder};
                    }
                    latch.countdown();
                });
            ASSERT_TRUE(latch.wait(std::chrono::seconds(1)));
        }

        std::lock_guard<std::mutex> lock{msgMutex};
        ASSERT_EQ(msg.getValue(), i);
        ASSERT_EQ(msg.getMessageId(), messageIds[i]);
    }

    client.close();
}

TEST(TypedMessageTest, testListener) {
    const auto topic = "typed-message-test-listener-" + unique_str();
    Client client(lookupUrl);

    ConsumerConfiguration conf;
    std::vector<int> values;
    std::mutex valuesMutex;
    conf.setTypedMessageListener<int>(
        [&values, &valuesMutex](Consumer& consumer, const TypedMessage<int>& msg) {
            std::lock_guard<std::mutex> lock{valuesMutex};
            values.emplace_back(msg.getValue());
        },
        intDecoder);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", conf, consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    constexpr int numMessages = 100;
    for (int i = 0; i < numMessages; i++) {
        auto msg = TypedMessageBuilder<int>{intEncoder}.setValue(i).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    waitUntil(std::chrono::seconds(3), [&values, &valuesMutex] {
        std::lock_guard<std::mutex> lock{valuesMutex};
        return values.size() == numMessages;
    });
    std::lock_guard<std::mutex> lock{valuesMutex};
    ASSERT_EQ(values.size(), numMessages);
    for (int i = 0; i < numMessages; i++) {
        ASSERT_EQ(values[i], i);
    }
}

TEST(TypedMessageTest, testValidate) {
    auto encoder = [](int x) { return std::to_string(x); };
    auto validator = [](const char* data, size_t size) {
        if (size > 3) {
            throw std::runtime_error(std::to_string(size));
        }
    };
    IntMessageBuilder intMessageBuilder{encoder, validator};
    intMessageBuilder.setValue(1);
    ASSERT_THROW(intMessageBuilder.setValue(1234), std::runtime_error);
    intMessageBuilder.setValue(123);

    BytesMessageBuilder{validator}.setContent("123");
}

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
#include <future>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>

#include "HttpHelper.h"
#include "lib/ClientConnection.h"
#include "lib/LogUtils.h"
#include "lib/MockServer.h"
#include "tests/PulsarFriend.h"

DECLARE_LOG_OBJECT()

static const std::string lookupUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

extern std::string unique_str();

namespace pulsar {

class ConsumerSeekTest : public ::testing::TestWithParam<bool> {
   public:
    void SetUp() override { client_ = Client{lookupUrl}; }

    void TearDown() override { client_.close(); }

   protected:
    Client client_{lookupUrl};
    ProducerConfiguration producerConf_;

    std::vector<Producer> initProducersForPartitionedTopic(const std::string& topic) {
        constexpr int numPartitions = 3;
        int res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + topic + "/partitions",
                                 std::to_string(numPartitions));
        if (res != 204 && res != 409) {
            throw std::runtime_error("Failed to create partitioned topic: " + std::to_string(res));
        }

        std::vector<Producer> producers(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            auto result = client_.createProducer(topic + "-partition-" + std::to_string(i), producers[i]);
            if (result != ResultOk) {
                throw std::runtime_error(std::string{"Failed to create producer: "} + strResult(result));
            }
        }
        return producers;
    }

    Consumer createConsumer(const std::string& topic) {
        Consumer consumer;
        ConsumerConfiguration conf;
        conf.setStartMessageIdInclusive(GetParam());
        auto result = client_.subscribe(topic, "sub", conf, consumer);
        if (result != ResultOk) {
            throw std::runtime_error(std::string{"Failed to subscribe: "} + strResult(result));
        }
        return consumer;
    }
};

TEST_P(ConsumerSeekTest, testSeekForMessageId) {
    Client client(lookupUrl);

    const std::string topic = "test-seek-for-message-id-" + std::string((GetParam() ? "batch-" : "")) +
                              std::to_string(time(nullptr));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf_, producer));

    Consumer consumerExclusive;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub-0", consumerExclusive));

    Consumer consumerInclusive;
    ASSERT_EQ(ResultOk,
              client.subscribe(topic, "sub-1", ConsumerConfiguration().setStartMessageIdInclusive(true),
                               consumerInclusive));

    const auto numMessages = 100;
    MessageId seekMessageId;

    int r = (rand() % (numMessages - 1));
    for (int i = 0; i < numMessages; i++) {
        MessageId id;
        ASSERT_EQ(ResultOk,
                  producer.send(MessageBuilder().setContent("msg-" + std::to_string(i)).build(), id));

        if (i == r) {
            seekMessageId = id;
        }
    }

    LOG_INFO("The seekMessageId is: " << seekMessageId << ", r : " << r);

    consumerExclusive.seek(seekMessageId);
    Message msg0;
    ASSERT_EQ(ResultOk, consumerExclusive.receive(msg0, 3000));

    consumerInclusive.seek(seekMessageId);
    Message msg1;
    ASSERT_EQ(ResultOk, consumerInclusive.receive(msg1, 3000));

    LOG_INFO("consumerExclusive received " << msg0.getDataAsString() << " from " << msg0.getMessageId());
    LOG_INFO("consumerInclusive received " << msg1.getDataAsString() << " from " << msg1.getMessageId());

    ASSERT_EQ(msg0.getDataAsString(), "msg-" + std::to_string(r + 1));
    ASSERT_EQ(msg1.getDataAsString(), "msg-" + std::to_string(r));

    consumerInclusive.close();
    consumerExclusive.close();
    producer.close();
}

TEST_P(ConsumerSeekTest, testMultiTopicsSeekAll) {
    std::string topic = "consumer-seek-test-multi-topics-seek-all-" + unique_str();
    auto producers = initProducersForPartitionedTopic(topic);
    auto consumer = createConsumer(topic);
    const auto numPartitions = producers.size();

    auto receive = [&consumer, numPartitions] {
        std::set<std::string> values;
        for (int i = 0; i < numPartitions; i++) {
            Message msg;
            auto result = consumer.receive(msg, 3000);
            if (result != ResultOk) {
                throw std::runtime_error(std::string{"Receive failed: "} + strResult(result));
            }
            values.emplace(msg.getDataAsString());
        }
        return values;
    };

    for (int i = 0; i < numPartitions; i++) {
        producers[i].send(MessageBuilder().setContent("msg-" + std::to_string(i) + "-0").build());
    }
    ASSERT_EQ(receive(), (std::set<std::string>{"msg-0-0", "msg-1-0", "msg-2-0"}));

    // Seek to earliest
    ASSERT_EQ(ResultOk, consumer.seek(MessageId::earliest()));
    ASSERT_EQ(receive(), (std::set<std::string>{"msg-0-0", "msg-1-0", "msg-2-0"}));

    // Seek to latest
    for (int i = 0; i < numPartitions; i++) {
        producers[i].send(MessageBuilder().setContent("msg-" + std::to_string(i) + "-1").build());
    }
    ASSERT_EQ(ResultOk, consumer.seek(MessageId::latest()));

    for (int i = 0; i < numPartitions; i++) {
        producers[i].send(MessageBuilder().setContent("msg-" + std::to_string(i) + "-2").build());
    }
    ASSERT_EQ(receive(), (std::set<std::string>{"msg-0-2", "msg-1-2", "msg-2-2"}));
}

TEST_P(ConsumerSeekTest, testMultiTopicsSeekSingle) {
    std::string topic = "consumer-seek-test-multi-topics-seek-single-" + unique_str();
    auto producers = initProducersForPartitionedTopic(topic);
    auto consumer = createConsumer(topic);

    MessageId msgId;
    producers[0].send(MessageBuilder().setContent("msg-0").build(), msgId);
    ASSERT_EQ(ResultOperationNotSupported, consumer.seek(msgId));
    producers[0].send(MessageBuilder().setContent("msg-1").build(), msgId);
    ASSERT_EQ(ResultOperationNotSupported, consumer.seek(msgId));

    std::vector<MessageId> msgIds;
    Message msg;
    for (int i = 0; i < 2; i++) {
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        msgIds.emplace_back(msg.getMessageId());
    }

    ASSERT_EQ(ResultOk, consumer.seek(msgIds[0]));
    ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
    if (GetParam()) {
        ASSERT_EQ(msg.getMessageId(), msgIds[0]);
    } else {
        ASSERT_EQ(msg.getMessageId(), msgIds[1]);
    }
}

TEST_F(ConsumerSeekTest, testNoInternalConsumer) {
    Consumer consumer;
    ASSERT_EQ(ResultOk, client_.subscribeWithRegex("testNoInternalConsumer.*", "sub", consumer));
    ASSERT_EQ(ResultOk, consumer.seek(MessageId::earliest()));
}

static void assertSeekWithTimeout(Consumer& consumer) {
    using namespace std::chrono_literals;
    auto promise = std::make_shared<std::promise<Result>>();
    std::weak_ptr<std::promise<Result>> weakPromise = promise;
    consumer.seekAsync(0L, [weakPromise](Result result) {
        if (auto promise = weakPromise.lock()) {
            promise->set_value(result);
        }
    });
    auto future = promise->get_future();
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    ASSERT_EQ(future.get(), ResultOk);
}

// Verify the `seek` method won't be blocked forever in any order of the Subscribe response and Seek response
TEST_F(ConsumerSeekTest, testSubscribeSeekRaces) {
    Client client(lookupUrl);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe("testSubscribeSeekRaces", "sub", consumer));

    auto connection = *PulsarFriend::getConnections(client).begin();
    auto mockServer = std::make_shared<MockServer>(connection);
    connection->attachMockServer(mockServer);

    mockServer->setRequestDelay({{"SUBSCRIBE", 1000}, {"SEEK", 500}});
    assertSeekWithTimeout(consumer);

    mockServer->setRequestDelay({{"SUBSCRIBE", 500}, {"SEEK", 1000}});
    assertSeekWithTimeout(consumer);

    ASSERT_EQ(mockServer->close(), 0);
    client.close();
}

TEST_F(ConsumerSeekTest, testReconnectionSlow) {
    Client client(lookupUrl, ClientConfiguration().setInitialBackoffIntervalMs(500));
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe("testReconnectionSlow", "sub", consumer));

    auto connection = *PulsarFriend::getConnections(client).begin();
    auto mockServer = std::make_shared<MockServer>(connection);
    connection->attachMockServer(mockServer);

    // Make seek response received before `connectionOpened` is called
    mockServer->setRequestDelay({{"SEEK", 500}, {"CLOSE_CONSUMER", 1000}});
    assertSeekWithTimeout(consumer);

    // The CLOSE_CONSUMER request is in still flight
    ASSERT_EQ(mockServer->close(), 1);
    client.close();
}

INSTANTIATE_TEST_SUITE_P(Pulsar, ConsumerSeekTest, ::testing::Values(true, false));

}  // namespace pulsar

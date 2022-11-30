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

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";

extern std::string unique_str();

class AcknowledgeTest : public testing::TestWithParam<int> {};

TEST_P(AcknowledgeTest, testAckMsgList) {
    Client client(lookupUrl);
    auto clientImplPtr = PulsarFriend::getClientImplPtr(client);

    constexpr auto numMsg = 100;
    std::string uniqueChunk = unique_str();
    std::string topicName = "persistent://public/default/test-ack-msgs" + uniqueChunk;
    const std::string subName = "sub-ack-list";

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    ConsumerConfiguration consumerConfig;
    consumerConfig.setAckGroupingMaxSize(numMsg);
    consumerConfig.setAckGroupingTimeMs(GetParam());
    consumerConfig.setUnAckedMessagesTimeoutMs(10000);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumerConfig, consumer));

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }
    ASSERT_EQ(ResultOk, consumer.acknowledge(recvMsgId));

    // try redeliver unack messages.
    consumer.redeliverUnacknowledgedMessages();

    auto consumerStats = PulsarFriend::getConsumerStatsPtr(consumer);
    auto ackMap = consumerStats->getAckedMsgMap();
    unsigned long totalAck = ackMap[std::make_pair(ResultOk, CommandAck_AckType_Individual)];
    ASSERT_EQ(totalAck, numMsg);

    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();

    producer.close();
    consumer.close();
    client.close();
}

TEST_P(AcknowledgeTest, testAckMsgListWithMultiConsumer) {
    Client client(lookupUrl);
    auto clientImplPtr = PulsarFriend::getClientImplPtr(client);

    std::string uniqueChunk = unique_str();
    std::string topicName = "persistent://public/default/test-ack-msgs" + uniqueChunk;

    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/v2/persistent/public/default/test-ack-msgs" + uniqueChunk + "/partitions";
    int res = makePutRequest(url, "5");
    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    constexpr auto numMsg = 100;
    const std::string subName = "sub-ack-list";

    Producer producer;
    ProducerConfiguration producerConfig;
    // Turn off batch to ensure even distribution
    producerConfig.setBatchingEnabled(false);
    producerConfig.setPartitionsRoutingMode(pulsar::ProducerConfiguration::RoundRobinDistribution);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfig, producer));

    ConsumerConfiguration consumerConfig;
    // set ack grouping max size is 10
    consumerConfig.setAckGroupingMaxSize(10);
    consumerConfig.setAckGroupingTimeMs(GetParam());
    consumerConfig.setUnAckedMessagesTimeoutMs(10000);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumerConfig, consumer));

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }
    ASSERT_EQ(ResultOk, consumer.acknowledge(recvMsgId));

    // try redeliver unack messages.
    consumer.redeliverUnacknowledgedMessages();

    // assert stats
    unsigned long totalAck = 0;
    auto consumerStatsList = PulsarFriend::getConsumerStatsPtrList(consumer);
    for (auto consumerStats : consumerStatsList) {
        auto ackMap = consumerStats->getAckedMsgMap();
        totalAck += ackMap[std::make_pair(ResultOk, CommandAck_AckType_Individual)];
    }
    ASSERT_EQ(totalAck, numMsg);

    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();

    producer.close();
    consumer.close();
    client.close();
}

INSTANTIATE_TEST_SUITE_P(BasicEndToEndTest, AcknowledgeTest, testing::Values(100, 0));

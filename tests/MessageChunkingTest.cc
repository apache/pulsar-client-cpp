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

#include <ctime>
#include <random>

#include "PulsarFriend.h"
#include "WaitUtils.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";

// See the `maxMessageSize` config in test-conf/standalone-ssl.conf
static constexpr size_t maxMessageSize = 1024000;

static std::string toString(CompressionType compressionType) {
    switch (compressionType) {
        case CompressionType::CompressionNone:
            return "None";
        case CompressionType::CompressionLZ4:
            return "LZ4";
        case CompressionType::CompressionZLib:
            return "ZLib";
        case CompressionType::CompressionZSTD:
            return "ZSTD";
        case CompressionType::CompressionSNAPPY:
            return "SNAPPY";
        default:
            return "Unknown (" + std::to_string(compressionType) + ")";
    }
}

inline std::string createLargeMessage() {
    std::string largeMessage(maxMessageSize * 3, 'a');
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<unsigned> u(0, 25);
    for (size_t i = 0; i < largeMessage.size(); i++) {
        largeMessage[i] = 'a' + u(e);
    }
    return largeMessage;
}

class MessageChunkingTest : public ::testing::TestWithParam<CompressionType> {
   public:
    static std::string largeMessage;

    void TearDown() override { client_.close(); }

    void createProducer(const std::string& topic, Producer& producer) {
        ProducerConfiguration conf;
        conf.setBatchingEnabled(false);
        conf.setChunkingEnabled(true);
        conf.setCompressionType(GetParam());
        LOG_INFO("Create producer to topic: " << topic
                                              << ", compression: " << toString(conf.getCompressionType()));
        ASSERT_EQ(ResultOk, client_.createProducer(topic, conf, producer));
    }

    void createConsumer(const std::string& topic, Consumer& consumer) {
        ASSERT_EQ(ResultOk, client_.subscribe(topic, "my-sub", consumer));
    }

    void createConsumer(const std::string& topic, Consumer& consumer, ConsumerConfiguration& conf) {
        ASSERT_EQ(ResultOk, client_.subscribe(topic, "my-sub", conf, consumer));
    }

   private:
    Client client_{lookupUrl};
};

std::string MessageChunkingTest::largeMessage = createLargeMessage();

TEST_F(MessageChunkingTest, testInvalidConfig) {
    Client client(lookupUrl);
    ProducerConfiguration conf;
    conf.setBatchingEnabled(true);
    conf.setChunkingEnabled(true);
    Producer producer;
    ASSERT_THROW(client.createProducer("xxx", conf, producer), std::invalid_argument);
    client.close();
}

TEST_P(MessageChunkingTest, testEndToEnd) {
    const std::string topic =
        "MessageChunkingTest-EndToEnd-" + toString(GetParam()) + std::to_string(time(nullptr));
    Consumer consumer;
    createConsumer(topic, consumer);
    Producer producer;
    createProducer(topic, producer);

    constexpr int numMessages = 10;

    std::vector<MessageId> sendMessageIds;
    for (int i = 0; i < numMessages; i++) {
        MessageId messageId;
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), messageId));
        LOG_INFO("Send " << i << " to " << messageId);
        sendMessageIds.emplace_back(messageId);
    }

    Message msg;
    std::vector<MessageId> receivedMessageIds;
    for (int i = 0; i < numMessages; i++) {
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        LOG_INFO("Receive " << msg.getLength() << " bytes from " << msg.getMessageId());
        ASSERT_EQ(msg.getDataAsString(), largeMessage);
        ASSERT_EQ(msg.getMessageId().batchIndex(), -1);
        ASSERT_EQ(msg.getMessageId().batchSize(), 0);
        receivedMessageIds.emplace_back(msg.getMessageId());
    }
    ASSERT_EQ(receivedMessageIds, sendMessageIds);
    ASSERT_EQ(receivedMessageIds.front().ledgerId(), receivedMessageIds.front().ledgerId());
    ASSERT_GT(receivedMessageIds.back().entryId(), numMessages);

    // Verify the cache has been cleared
    auto& chunkedMessageCache = PulsarFriend::getChunkedMessageCache(consumer);
    ASSERT_EQ(chunkedMessageCache.size(), 0);

    producer.close();
    consumer.close();
}

TEST_P(MessageChunkingTest, testExpireIncompleteChunkMessage) {
    // This test is time-consuming and is not related to the compressionType. So skip other compressionType
    // here.
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic = "MessageChunkingTest-testExpireIncompleteChunkMessage-" + toString(GetParam()) +
                              std::to_string(time(nullptr));
    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setExpireTimeOfIncompleteChunkedMessageMs(5000);
    consumerConf.setAutoAckOldestChunkedMessageOnQueueFull(true);
    createConsumer(topic, consumer, consumerConf);
    Producer producer;
    createProducer(topic, producer);

    auto msg = MessageBuilder().setContent("test-data").build();
    auto& metadata = PulsarFriend::getMessageMetadata(msg);
    metadata.set_num_chunks_from_msg(2);
    metadata.set_chunk_id(0);
    metadata.set_total_chunk_msg_size(100);

    producer.send(msg);

    auto& chunkedMessageCache = PulsarFriend::getChunkedMessageCache(consumer);

    waitUntil(
        std::chrono::seconds(2), [&] { return chunkedMessageCache.size() > 0; }, 1000);
    ASSERT_EQ(chunkedMessageCache.size(), 1);

    // Wait for triggering the check of the expiration.
    // Need to wait for 2 * expireTime because there may be a gap in checking the expiration time.
    waitUntil(
        std::chrono::seconds(10), [&] { return chunkedMessageCache.size() == 0; }, 1000);
    ASSERT_EQ(chunkedMessageCache.size(), 0);

    producer.close();
    consumer.close();
}

TEST_P(MessageChunkingTest, testMaxPendingChunkMessages) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic = "MessageChunkingTest-testMaxPendingChunkMessages-" + toString(GetParam()) +
                              std::to_string(time(nullptr));
    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setMaxPendingChunkedMessage(1);
    consumerConf.setAutoAckOldestChunkedMessageOnQueueFull(true);
    createConsumer(topic, consumer, consumerConf);
    Producer producer;
    createProducer(topic, producer);

    auto msg = MessageBuilder().setContent("chunk-0-0|").build();
    auto& metadata = PulsarFriend::getMessageMetadata(msg);
    metadata.set_num_chunks_from_msg(2);
    metadata.set_chunk_id(0);
    metadata.set_uuid("0");
    metadata.set_total_chunk_msg_size(100);

    producer.send(msg);

    auto msg2 = MessageBuilder().setContent("chunk-1-0|").build();
    auto& metadata2 = PulsarFriend::getMessageMetadata(msg2);
    metadata2.set_num_chunks_from_msg(2);
    metadata2.set_uuid("1");
    metadata2.set_chunk_id(0);
    metadata2.set_total_chunk_msg_size(100);

    producer.send(msg2);

    auto msg3 = MessageBuilder().setContent("chunk-1-1|").build();
    auto& metadata3 = PulsarFriend::getMessageMetadata(msg3);
    metadata3.set_num_chunks_from_msg(2);
    metadata3.set_uuid("1");
    metadata3.set_chunk_id(1);
    metadata3.set_total_chunk_msg_size(100);

    producer.send(msg3);

    Message receivedMsg;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 3000));
    ASSERT_EQ(receivedMsg.getDataAsString(), "chunk-1-0|chunk-1-1|");

    consumer.redeliverUnacknowledgedMessages();

    // The consumer may acknowledge the wrong message(the latest message) in the old version of codes. This
    // test case ensure that it should not happen again.
    Message receivedMsg2;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg2, 3000));
    ASSERT_EQ(receivedMsg2.getDataAsString(), "chunk-1-0|chunk-1-1|");

    consumer.acknowledge(receivedMsg2);

    consumer.redeliverUnacknowledgedMessages();
    auto msg4 = MessageBuilder().setContent("chunk-0-1|").build();
    auto& metadata4 = PulsarFriend::getMessageMetadata(msg4);
    metadata4.set_num_chunks_from_msg(2);
    metadata4.set_uuid("0");
    metadata4.set_chunk_id(1);
    metadata4.set_total_chunk_msg_size(100);

    producer.send(msg4);

    // This ensures that the message chunk-0-0 was acknowledged successfully. So we cannot receive it anymore.
    Message receivedMsg3;
    consumer.receive(receivedMsg3, 3000);

    producer.close();
    consumer.close();
}

// The CI env is Ubuntu 16.04, the gtest-dev version is 1.8.0 that doesn't have INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_CASE_P(Pulsar, MessageChunkingTest,
                        ::testing::Values(CompressionNone, CompressionLZ4, CompressionZLib, CompressionZSTD,
                                          CompressionSNAPPY));

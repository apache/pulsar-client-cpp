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
#include <pulsar/DeadLetterPolicyBuilder.h>
#include <pulsar/MessageIdBuilder.h>

#include <ctime>
#include <random>
#include <sstream>

#include "PulsarFriend.h"
#include "WaitUtils.h"
#include "lib/ChunkMessageIdImpl.h"
#include "lib/ConsumerImpl.h"
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
        ConsumerConfiguration conf;
        conf.setBrokerConsumerStatsCacheTimeInMs(1000);
        ASSERT_EQ(ResultOk, client_.subscribe(topic, "my-sub", conf, consumer));
    }

    void createConsumer(const std::string& topic, Consumer& consumer, ConsumerConfiguration& conf) {
        ASSERT_EQ(ResultOk, client_.subscribe(topic, "my-sub", conf, consumer));
    }

   private:
    Client client_{lookupUrl};
};

std::string MessageChunkingTest::largeMessage = createLargeMessage();

// Helper function: send a single chunk message
static void sendSingleChunk(Producer& producer, const std::string& uuid, int chunkId, int totalChunks) {
    std::string content = "chunk-" + uuid + "-" + std::to_string(chunkId) + "|";
    auto msg = MessageBuilder().setContent(content).build();
    auto& metadata = PulsarFriend::getMessageMetadata(msg);
    metadata.set_num_chunks_from_msg(totalChunks);
    metadata.set_chunk_id(chunkId);
    metadata.set_uuid(uuid);
    metadata.set_total_chunk_msg_size(100);
    MessageId messageId;
    ASSERT_EQ(ResultOk, producer.send(msg, messageId));
}

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
        auto messageId = msg.getMessageId();
        receivedMessageIds.emplace_back(messageId);
        consumer.acknowledge(messageId);
    }
    ASSERT_EQ(receivedMessageIds, sendMessageIds);
    for (int i = 0; i < sendMessageIds.size(); ++i) {
        auto sendChunkMsgId =
            std::dynamic_pointer_cast<ChunkMessageIdImpl>(PulsarFriend::getMessageIdImpl(sendMessageIds[i]));
        ASSERT_TRUE(sendChunkMsgId);
        auto receiveChunkMsgId = std::dynamic_pointer_cast<ChunkMessageIdImpl>(
            PulsarFriend::getMessageIdImpl(receivedMessageIds[i]));
        ASSERT_TRUE(receiveChunkMsgId);
        ASSERT_EQ(sendChunkMsgId->getChunkedMessageIds(), receiveChunkMsgId->getChunkedMessageIds());
    }
    ASSERT_GT(receivedMessageIds.back().entryId(), numMessages);

    // Verify the cache has been cleared
    auto& chunkedMessageCache = PulsarFriend::getChunkedMessageCache(consumer);
    ASSERT_EQ(chunkedMessageCache.size(), 0);

    BrokerConsumerStats consumerStats;
    waitUntil(
        std::chrono::seconds(10),
        [&] {
            return consumer.getBrokerConsumerStats(consumerStats) == ResultOk &&
                   consumerStats.getMsgBacklog() == 0;
        },
        1000);
    ASSERT_EQ(consumerStats.getMsgBacklog(), 0);

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

    sendSingleChunk(producer, "expire-test", 0, 2);

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

    sendSingleChunk(producer, "0", 0, 2);
    sendSingleChunk(producer, "1", 0, 2);
    sendSingleChunk(producer, "1", 1, 2);

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
    sendSingleChunk(producer, "0", 1, 2);

    // This ensures that the message chunk-0-0 was acknowledged successfully. So we cannot receive it anymore.
    Message receivedMsg3;
    consumer.receive(receivedMsg3, 3000);

    producer.close();
    consumer.close();
}

TEST_P(MessageChunkingTest, testSeekChunkMessages) {
    const std::string topic =
        "MessageChunkingTest-testSeekChunkMessages-" + toString(GetParam()) + std::to_string(time(nullptr));

    constexpr int numMessages = 10;

    Consumer consumer1;
    ConsumerConfiguration consumer1Conf;
    consumer1Conf.setStartMessageIdInclusive(true);
    createConsumer(topic, consumer1, consumer1Conf);

    Producer producer;
    createProducer(topic, producer);

    for (int i = 0; i < numMessages; i++) {
        MessageId messageId;
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), messageId));
        LOG_INFO("Send " << i << " to " << messageId);
    }

    Message msg;
    std::vector<MessageId> receivedMessageIds;
    for (int i = 0; i < numMessages; i++) {
        ASSERT_EQ(ResultOk, consumer1.receive(msg, 3000));
        LOG_INFO("Receive " << msg.getLength() << " bytes from " << msg.getMessageId());
        receivedMessageIds.emplace_back(msg.getMessageId());
    }

    consumer1.seek(receivedMessageIds[1]);
    for (int i = 1; i < numMessages; i++) {
        Message msgAfterSeek;
        ASSERT_EQ(ResultOk, consumer1.receive(msgAfterSeek, 3000));
        ASSERT_EQ(msgAfterSeek.getMessageId(), receivedMessageIds[i]);
    }

    consumer1.close();
    Consumer consumer2;
    createConsumer(topic, consumer2);

    consumer2.seek(receivedMessageIds[1]);
    for (int i = 2; i < numMessages; i++) {
        Message msgAfterSeek;
        ASSERT_EQ(ResultOk, consumer2.receive(msgAfterSeek, 3000));
        ASSERT_EQ(msgAfterSeek.getMessageId(), receivedMessageIds[i]);
    }

    consumer2.close();
    producer.close();
}

TEST(ChunkMessageIdTest, testSetChunkMessageId) {
    MessageId msgId;
    {
        ChunkMessageIdImplPtr chunkMsgId = std::make_shared<ChunkMessageIdImpl>(
            std::vector<MessageId>({MessageIdBuilder().ledgerId(1).entryId(2).partition(3).build(),
                                    MessageIdBuilder().ledgerId(4).entryId(5).partition(6).build()}));
        msgId = chunkMsgId->build();
        // Test the destructor of the underlying message id should also work for the generated messageId.
    }

    std::string msgIdData;
    msgId.serialize(msgIdData);
    MessageId deserializedMsgId = MessageId::deserialize(msgIdData);

    ASSERT_EQ(deserializedMsgId.ledgerId(), 4);
    ASSERT_EQ(deserializedMsgId.entryId(), 5);
    ASSERT_EQ(deserializedMsgId.partition(), 6);

    const auto& chunkMsgId =
        std::dynamic_pointer_cast<ChunkMessageIdImpl>(PulsarFriend::getMessageIdImpl(deserializedMsgId));
    ASSERT_TRUE(chunkMsgId);
    auto firstChunkMsgId = chunkMsgId->getChunkedMessageIds().front();
    ASSERT_EQ(firstChunkMsgId.ledgerId(), 1);
    ASSERT_EQ(firstChunkMsgId.entryId(), 2);
    ASSERT_EQ(firstChunkMsgId.partition(), 3);
}

// Aligned with Java testResendChunkMessagesWithoutAckHole
TEST_P(MessageChunkingTest, testResendChunkMessagesWithoutAckHole) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic =
        "MessageChunkingTest-testResendChunkMessagesWithoutAckHole-" + std::to_string(time(nullptr));
    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setMaxPendingChunkedMessage(10);
    consumerConf.setAutoAckOldestChunkedMessageOnQueueFull(true);
    consumerConf.setBrokerConsumerStatsCacheTimeInMs(1000);
    createConsumer(topic, consumer, consumerConf);
    Producer producer;
    createProducer(topic, producer);

    // Send chunk sequence: uuid="0" chunkId=0 -> uuid="0" chunkId=0 (resend) -> uuid="0" chunkId=1
    sendSingleChunk(producer, "0", 0, 2);
    sendSingleChunk(producer, "0", 0, 2);  // Resend the first chunk
    sendSingleChunk(producer, "0", 1, 2);

    Message receivedMsg;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 5000));
    ASSERT_EQ(receivedMsg.getDataAsString(), "chunk-0-0|chunk-0-1|");
    consumer.acknowledge(receivedMsg);

    // Verify no ack hole: backlog should be 0 after ack
    BrokerConsumerStats consumerStats;
    waitUntil(
        std::chrono::seconds(10),
        [&] {
            return consumer.getBrokerConsumerStats(consumerStats) == ResultOk &&
                   consumerStats.getMsgBacklog() == 0;
        },
        1000);
    ASSERT_EQ(consumerStats.getMsgBacklog(), 0);

    producer.close();
    consumer.close();
}

// Aligned with Java testResendChunkMessages
TEST_P(MessageChunkingTest, testResendChunkMessages) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic = "MessageChunkingTest-testResendChunkMessages-" + std::to_string(time(nullptr));
    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setMaxPendingChunkedMessage(10);
    consumerConf.setAutoAckOldestChunkedMessageOnQueueFull(true);
    createConsumer(topic, consumer, consumerConf);
    Producer producer;
    createProducer(topic, producer);

    // Send interleaved chunk sequence with multiple uuid resends
    sendSingleChunk(producer, "0", 0, 2);
    sendSingleChunk(producer, "0", 0, 2);  // Resend first chunk of uuid="0"
    sendSingleChunk(producer, "1", 0, 3);  // Interleave uuid="1"
    sendSingleChunk(producer, "1", 1, 3);
    sendSingleChunk(producer, "1", 0, 3);  // Resend first chunk of uuid="1"
    sendSingleChunk(producer, "0", 1, 2);  // Complete uuid="0"

    Message receivedMsg;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 5000));
    ASSERT_EQ(receivedMsg.getDataAsString(), "chunk-0-0|chunk-0-1|");
    consumer.acknowledge(receivedMsg);

    // Continue sending to complete uuid="1"
    sendSingleChunk(producer, "1", 1, 3);
    sendSingleChunk(producer, "1", 2, 3);

    Message receivedMsg2;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg2, 5000));
    ASSERT_EQ(receivedMsg2.getDataAsString(), "chunk-1-0|chunk-1-1|chunk-1-2|");
    consumer.acknowledge(receivedMsg2);

    producer.close();
    consumer.close();
}

// Aligned with Go TestResendChunkWithAckHoleMessages
TEST_P(MessageChunkingTest, testResendChunkWithAckHoleMessages) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic =
        "MessageChunkingTest-testResendChunkWithAckHoleMessages-" + std::to_string(time(nullptr));
    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setMaxPendingChunkedMessage(10);
    consumerConf.setAutoAckOldestChunkedMessageOnQueueFull(true);
    createConsumer(topic, consumer, consumerConf);
    Producer producer;
    createProducer(topic, producer);

    // Scenario 1: middle chunk resend, verify message assembles correctly (duplicated chunks are filtered)
    sendSingleChunk(producer, "0", 0, 4);
    sendSingleChunk(producer, "0", 1, 4);
    sendSingleChunk(producer, "0", 2, 4);
    sendSingleChunk(producer, "0", 1, 4);  // Resend chunkId=1
    sendSingleChunk(producer, "0", 2, 4);  // Resend chunkId=2
    sendSingleChunk(producer, "0", 3, 4);  // Complete

    Message receivedMsg;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 5000));
    ASSERT_EQ(receivedMsg.getDataAsString(), "chunk-0-0|chunk-0-1|chunk-0-2|chunk-0-3|");
    consumer.acknowledge(receivedMsg);

    // Scenario 2: chunk gap (chunkId jump), verify consumer cannot receive message (context is cleaned up)
    sendSingleChunk(producer, "1", 0, 4);
    sendSingleChunk(producer, "1", 1, 4);
    sendSingleChunk(producer, "1", 4, 4);  // Gap: skipped chunkId=2 and 3

    Message receivedMsg2;
    ASSERT_NE(ResultOk, consumer.receive(receivedMsg2, 3000));

    producer.close();
    consumer.close();
}

// Aligned with Go TestChunkAckAndNAck and Java testNegativeAckChunkedMessage
TEST_P(MessageChunkingTest, testNegativeAckChunkedMessage) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic =
        "MessageChunkingTest-testNegativeAckChunkedMessage-" + std::to_string(time(nullptr));

    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerShared);
    consumerConf.setNegativeAckRedeliveryDelayMs(1000);
    createConsumer(topic, consumer, consumerConf);

    Producer producer;
    createProducer(topic, producer);

    // Send a chunked message
    MessageId sendMsgId;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), sendMsgId));

    // Receive and nack
    Message msg;
    ASSERT_EQ(ResultOk, consumer.receive(msg, 5000));
    ASSERT_EQ(msg.getDataAsString(), largeMessage);
    consumer.negativeAcknowledge(msg);

    // The message should be redelivered after nack delay
    Message redeliveredMsg;
    ASSERT_EQ(ResultOk, consumer.receive(redeliveredMsg, 5000));
    ASSERT_EQ(redeliveredMsg.getDataAsString(), largeMessage);
    consumer.acknowledge(redeliveredMsg);

    // Verify no more messages
    Message noMsg;
    ASSERT_NE(ResultOk, consumer.receive(noMsg, 2000));

    producer.close();
    consumer.close();
}

// Aligned with Java testLargeMessageAckTimeOut
TEST_P(MessageChunkingTest, testAckTimeoutChunkedMessage) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic =
        "MessageChunkingTest-testAckTimeoutChunkedMessage-" + std::to_string(time(nullptr));

    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerShared);
    // Set ack timeout to 2 seconds
    PulsarFriend::setConsumerUnAckMessagesTimeoutMs(consumerConf, 2000);
    createConsumer(topic, consumer, consumerConf);

    Producer producer;
    createProducer(topic, producer);

    // Send a chunked message
    MessageId sendMsgId;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), sendMsgId));

    // Receive but do NOT acknowledge - let ack timeout trigger redelivery
    Message msg;
    ASSERT_EQ(ResultOk, consumer.receive(msg, 5000));
    ASSERT_EQ(msg.getDataAsString(), largeMessage);

    // Wait for ack timeout to trigger redelivery
    // The message should be redelivered after ack timeout (2s)
    Message redeliveredMsg;
    ASSERT_EQ(ResultOk, consumer.receive(redeliveredMsg, 5000));
    ASSERT_EQ(redeliveredMsg.getDataAsString(), largeMessage);
    consumer.acknowledge(redeliveredMsg);

    // Verify no more messages
    Message noMsg;
    ASSERT_NE(ResultOk, consumer.receive(noMsg, 2000));

    producer.close();
    consumer.close();
}

TEST_P(MessageChunkingTest, testChunkedMessageDLQ) {
    if (toString(GetParam()) != "None") {
        return;
    }
    const std::string topic = "persistent://public/default/MessageChunkingTest-testChunkedMessageDLQ-" +
                              std::to_string(time(nullptr));
    const std::string subName = "my-sub";
    const std::string dlqTopic = topic + "-" + subName + "-DLQ";

    Client client(lookupUrl);

    auto dlqPolicy =
        DeadLetterPolicyBuilder().maxRedeliverCount(2).initialSubscriptionName("dlq-init-sub").build();

    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerShared);
    consumerConf.setNegativeAckRedeliveryDelayMs(100);
    consumerConf.setDeadLetterPolicy(dlqPolicy);
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConf, consumer));

    // Subscribe to DLQ topic to verify messages arrive there
    Consumer dlqConsumer;
    ConsumerConfiguration dlqConsumerConf;
    dlqConsumerConf.setConsumerType(ConsumerShared);
    ASSERT_EQ(ResultOk, client.subscribe(dlqTopic, "dlq-sub", dlqConsumerConf, dlqConsumer));

    Producer producer;
    createProducer(topic, producer);

    // Send a chunked message
    MessageId sendMsgId;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(largeMessage).build(), sendMsgId));

    // Nack the message maxRedeliverCount + 1 times to trigger DLQ
    Message msg;
    for (int i = 0; i < dlqPolicy.getMaxRedeliverCount() + 1; i++) {
        ASSERT_EQ(ResultOk, consumer.receive(msg, 5000));
        ASSERT_EQ(msg.getDataAsString(), largeMessage);
        consumer.negativeAcknowledge(msg);
    }

    // Verify the message arrives in DLQ with correct content
    Message dlqMsg;
    ASSERT_EQ(ResultOk, dlqConsumer.receive(dlqMsg, 10000));
    ASSERT_EQ(dlqMsg.getDataAsString(), largeMessage);
    std::stringstream expectedOriginMsgId;
    expectedOriginMsgId << sendMsgId;
    ASSERT_EQ(dlqMsg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID), expectedOriginMsgId.str());
    ASSERT_EQ(dlqMsg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC), topic);

    // Verify no more messages in DLQ
    Message noMsg;
    ASSERT_NE(ResultOk, dlqConsumer.receive(noMsg, 2000));

    // Verify original consumer has no more messages (message was acked after DLQ send)
    ASSERT_NE(ResultOk, consumer.receive(noMsg, 2000));

    producer.close();
    consumer.close();
    dlqConsumer.close();
    client.close();
}

// The CI env is Ubuntu 16.04, the gtest-dev version is 1.8.0 that doesn't have INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_CASE_P(Pulsar, MessageChunkingTest,
                        ::testing::Values(CompressionNone, CompressionLZ4, CompressionZLib, CompressionZSTD,
                                          CompressionSNAPPY));

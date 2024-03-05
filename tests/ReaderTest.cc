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
#include <pulsar/Reader.h>
#include <time.h>

#include <string>

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "WaitUtils.h"
#include "lib/ClientConnection.h"
#include "lib/Latch.h"
#include "lib/LogUtils.h"
#include "lib/ReaderImpl.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

static std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

class ReaderTest : public ::testing::TestWithParam<bool> {
   public:
    void initTopic(std::string topicName) {
        if (isMultiTopic_) {
            // call admin api to make it partitioned
            std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
            int res = makePutRequest(url, "5");
            LOG_INFO("res = " << res);
            ASSERT_FALSE(res != 204 && res != 409);
        }
    }

   protected:
    bool isMultiTopic_ = GetParam();
};

TEST_P(ReaderTest, testSimpleReader) {
    Client client(serviceUrl);

    std::string topicName =
        "test-simple-reader" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

TEST_P(ReaderTest, testAsyncRead) {
    Client client(serviceUrl);

    std::string topicName = "testAsyncRead" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 0; i < 10; i++) {
        reader.readNextAsync([i](Result result, const Message& msg) {
            ASSERT_EQ(ResultOk, result);
            std::string content = msg.getDataAsString();
            std::string expected = "my-message-" + std::to_string(i);
            ASSERT_EQ(expected, content);
        });
    }

    waitUntil(
        std::chrono::seconds(5),
        [&]() {
            bool hasMsg;
            reader.hasMessageAvailable(hasMsg);
            return !hasMsg;
        },
        1000);
    bool hasMsg;
    reader.hasMessageAvailable(hasMsg);
    ASSERT_FALSE(hasMsg);

    producer.close();
    reader.close();
    client.close();
}

TEST_P(ReaderTest, testReaderAfterMessagesWerePublished) {
    Client client(serviceUrl);

    std::string topicName = "testReaderAfterMessagesWerePublished" + std::to_string(time(nullptr)) +
                            std::to_string(isMultiTopic_);
    initTopic(topicName);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

TEST_P(ReaderTest, testMultipleReaders) {
    Client client(serviceUrl);

    std::string topicName =
        "testMultipleReaders" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader1;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader1));

    Reader reader2;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader2));

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader1.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader2.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader1.close();
    reader2.close();
    client.close();
}

TEST_P(ReaderTest, testReaderOnLastMessage) {
    Client client(serviceUrl);

    std::string topicName =
        "testReaderOnLastMessage" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 10; i < 20; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

TEST_P(ReaderTest, testReaderOnSpecificMessage) {
    Client client(serviceUrl);

    std::string topicName =
        "testReaderOnSpecificMessage" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    MessageId lastMessageId;

    for (int i = 0; i < 5; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);

        lastMessageId = msg.getMessageId();
    }

    // Create another reader starting on msgid4
    ASSERT_EQ(ResultOk, client.createReader(topicName, lastMessageId, readerConf, reader));

    for (int i = 5; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    client.close();
}

/**
 * build, file MessageIdBuilder.cc, line 45.?? Test that we can position on a particular message even within a
 * batch
 */
TEST_P(ReaderTest, testReaderOnSpecificMessageWithBatches) {
    Client client(serviceUrl);

    std::string topicName = "testReaderOnSpecificMessageWithBatches" + std::to_string(time(nullptr)) +
                            std::to_string(isMultiTopic_);
    initTopic(topicName);

    Producer producer;
    // Enable batching
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(true);
    producerConf.setBatchingMaxPublishDelayMs(1000);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        producer.sendAsync(msg, NULL);
    }

    // Send one sync message, to wait for everything before to be persisted as well
    std::string content = "my-message-10";
    Message msg = MessageBuilder().setContent(content).build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    std::string lastMessageId;

    for (int i = 0; i < 5; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);

        msg.getMessageId().serialize(lastMessageId);
    }

    // Create another reader starting on msgid4
    auto msgId4 = MessageId::deserialize(lastMessageId);
    Reader reader2;
    ASSERT_EQ(ResultOk, client.createReader(topicName, msgId4, readerConf, reader2));

    for (int i = 5; i < 11; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader2.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(i);
        ASSERT_EQ(expected, content);
    }

    producer.close();
    reader.close();
    reader2.close();
    client.close();
}

TEST_P(ReaderTest, testReaderReachEndOfTopic) {
    Client client(serviceUrl);

    std::string topicName =
        "testReaderReachEndOfTopic" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    // 1. create producer
    Producer producer;
    // Enable batching
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(true);
    producerConf.setBatchingMaxPublishDelayMs(1000);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    // 2. create reader, and expect hasMessageAvailable return false since no message produced.
    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    bool hasMessageAvailable;
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_FALSE(hasMessageAvailable);

    // 3. produce 10 messages.
    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // 4. expect hasMessageAvailable return true, and after read 10 messages out, it return false.
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    int readMessageCount = 0;
    for (; hasMessageAvailable; readMessageCount++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(readMessageCount);
        ASSERT_EQ(expected, content);
        reader.hasMessageAvailable(hasMessageAvailable);
    }

    ASSERT_EQ(readMessageCount, 10);
    ASSERT_FALSE(hasMessageAvailable);

    // 5. produce another 10 messages, expect hasMessageAvailable return true,
    //    and after read these 10 messages out, it return false.
    for (int i = 10; i < 20; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    for (; hasMessageAvailable; readMessageCount++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(readMessageCount);
        ASSERT_EQ(expected, content);
        reader.hasMessageAvailable(hasMessageAvailable);
    }
    ASSERT_EQ(readMessageCount, 20);
    ASSERT_FALSE(hasMessageAvailable);

    producer.close();
    reader.close();
    client.close();
}

TEST_P(ReaderTest, testReaderReachEndOfTopicMessageWithoutBatches) {
    Client client(serviceUrl);

    std::string topicName = "testReaderReachEndOfTopicMessageWithoutBatches" + std::to_string(time(nullptr)) +
                            std::to_string(isMultiTopic_);
    initTopic(topicName);

    // 1. create producer
    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    // 2. create reader, and expect hasMessageAvailable return false since no message produced.
    ReaderConfiguration readerConf;
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), readerConf, reader));

    bool hasMessageAvailable;
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_FALSE(hasMessageAvailable);

    // 3. produce 10 messages in batches way.
    for (int i = 0; i < 10; i++) {
        std::string content = "my-message-" + std::to_string(i);
        Message msg = MessageBuilder().setContent(content).build();
        producer.sendAsync(msg, NULL);
    }
    // Send one sync message, to wait for everything before to be persisted as well
    std::string content = "my-message-10";
    Message msg = MessageBuilder().setContent(content).build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    // 4. expect hasMessageAvailable return true, and after read 11 messages out, it return false.
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    std::string lastMessageId;
    int readMessageCount = 0;
    for (; hasMessageAvailable; readMessageCount++) {
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg));

        std::string content = msg.getDataAsString();
        std::string expected = "my-message-" + std::to_string(readMessageCount);
        ASSERT_EQ(expected, content);
        reader.hasMessageAvailable(hasMessageAvailable);
        msg.getMessageId().serialize(lastMessageId);
    }
    ASSERT_FALSE(hasMessageAvailable);
    ASSERT_EQ(readMessageCount, 11);

    producer.close();
    reader.close();
    client.close();
}

TEST(ReaderTest, testPartitionIndex) {
    Client client(serviceUrl);

    const std::string nonPartitionedTopic = "ReaderTestPartitionIndex-topic-" + std::to_string(time(nullptr));
    const std::string partitionedTopic =
        "ReaderTestPartitionIndex-par-topic-" + std::to_string(time(nullptr));

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    const std::string partition0 = partitionedTopic + "-partition-0";
    const std::string partition1 = partitionedTopic + "-partition-1";

    ReaderConfiguration readerConf;
    Reader readers[3];
    ASSERT_EQ(ResultOk,
              client.createReader(nonPartitionedTopic, MessageId::earliest(), readerConf, readers[0]));
    ASSERT_EQ(ResultOk, client.createReader(partition0, MessageId::earliest(), readerConf, readers[1]));
    ASSERT_EQ(ResultOk, client.createReader(partition1, MessageId::earliest(), readerConf, readers[2]));

    Producer producers[3];
    ASSERT_EQ(ResultOk, client.createProducer(nonPartitionedTopic, producers[0]));
    ASSERT_EQ(ResultOk, client.createProducer(partition0, producers[1]));
    ASSERT_EQ(ResultOk, client.createProducer(partition1, producers[2]));

    for (auto& producer : producers) {
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("hello").build()));
    }

    Message msg;
    readers[0].readNext(msg);
    ASSERT_EQ(msg.getMessageId().partition(), -1);
    readers[1].readNext(msg);
    ASSERT_EQ(msg.getMessageId().partition(), 0);
    readers[2].readNext(msg);
    ASSERT_EQ(msg.getMessageId().partition(), 1);

    client.close();
}

TEST_P(ReaderTest, testSubscriptionNameSetting) {
    Client client(serviceUrl);

    std::string topicName =
        "testSubscriptionNameSetting" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);
    std::string subName = "test-sub";

    ReaderConfiguration readerConf;
    readerConf.setInternalSubscriptionName(subName);
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    ASSERT_EQ(subName, PulsarFriend::getConsumer(reader)->getSubscriptionName());

    reader.close();
    client.close();
}

TEST_P(ReaderTest, testSetSubscriptionNameAndPrefix) {
    Client client(serviceUrl);

    std::string topicName =
        "testSetSubscriptionNameAndPrefix" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);
    std::string subName = "test-sub";

    ReaderConfiguration readerConf;
    readerConf.setInternalSubscriptionName(subName);
    readerConf.setSubscriptionRolePrefix("my-prefix");
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf, reader));

    ASSERT_EQ(subName, PulsarFriend::getConsumer(reader)->getSubscriptionName());

    reader.close();
    client.close();
}

TEST_P(ReaderTest, testMultiSameSubscriptionNameReaderShouldFail) {
    Client client(serviceUrl);

    std::string topicName = "testMultiSameSubscriptionNameReaderShouldFail" + std::to_string(time(nullptr)) +
                            std::to_string(isMultiTopic_);
    initTopic(topicName);
    std::string subscriptionName = "test-sub";

    ReaderConfiguration readerConf1;
    readerConf1.setInternalSubscriptionName(subscriptionName);
    Reader reader1;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), readerConf1, reader1));

    ReaderConfiguration readerConf2;
    readerConf2.setInternalSubscriptionName(subscriptionName);
    Reader reader2;
    ASSERT_EQ(ResultConsumerBusy,
              client.createReader(topicName, MessageId::earliest(), readerConf2, reader2));

    reader1.close();
    reader2.close();
    client.close();
}

TEST_P(ReaderTest, testIsConnected) {
    Client client(serviceUrl);

    std::string topicName = "testIsConnected" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    Reader reader;
    ASSERT_FALSE(reader.isConnected());

    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::earliest(), {}, reader));
    ASSERT_TRUE(reader.isConnected());

    ASSERT_EQ(ResultOk, reader.close());
    ASSERT_FALSE(reader.isConnected());
}

TEST_P(ReaderTest, testHasMessageAvailableWhenCreated) {
    Client client(serviceUrl);

    std::string topicName =
        "testHasMessageAvailableWhenCreated" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    ProducerConfiguration producerConf;
    producerConf.setBatchingMaxMessages(3);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConf, producer));

    std::vector<MessageId> messageIds;
    constexpr int numMessages = 7;
    Latch latch(numMessages);
    for (int i = 0; i < numMessages; i++) {
        producer.sendAsync(MessageBuilder().setContent("msg-" + std::to_string(i)).build(),
                           [i, &messageIds, &latch](Result result, const MessageId& messageId) {
                               if (result == ResultOk) {
                                   LOG_INFO("Send " << i << " to " << messageId);
                                   messageIds.emplace_back(messageId);
                               } else {
                                   LOG_ERROR("Failed to send " << i << ": " << messageId);
                               }
                               latch.countdown();
                           });
    }
    latch.wait(std::chrono::seconds(3));
    ASSERT_EQ(messageIds.size(), numMessages);

    Reader reader;
    bool hasMessageAvailable;

    for (size_t i = 0; i < messageIds.size() - 1; i++) {
        ASSERT_EQ(ResultOk, client.createReader(topicName, messageIds[i], {}, reader));
        ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
        EXPECT_TRUE(hasMessageAvailable);
    }

    // The start message ID is exclusive by default, so when we start at the last message, there should be no
    // message available.
    ASSERT_EQ(ResultOk, client.createReader(topicName, messageIds.back(), {}, reader));
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    EXPECT_FALSE(hasMessageAvailable);
    client.close();
}

TEST_P(ReaderTest, testReceiveAfterSeek) {
    Client client(serviceUrl);

    std::string topicName =
        "testReceiveAfterSeek" + std::to_string(time(nullptr)) + std::to_string(isMultiTopic_);
    initTopic(topicName);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    MessageId seekMessageId;
    for (int i = 0; i < 5; i++) {
        MessageId messageId;
        producer.send(MessageBuilder().setContent("msg-" + std::to_string(i)).build(), messageId);
        if (i == 3) {
            seekMessageId = messageId;
        }
    }

    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicName, MessageId::latest(), {}, reader));

    reader.seek(seekMessageId);

    bool hasMessageAvailable;
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));

    client.close();
}

TEST(ReaderSeekTest, testSeekForMessageId) {
    Client client(serviceUrl);

    const std::string topic = "test-seek-for-message-id-" + std::to_string(time(nullptr));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));

    Reader readerExclusive;
    ASSERT_EQ(ResultOk,
              client.createReader(topic, MessageId::latest(), ReaderConfiguration(), readerExclusive));

    Reader readerInclusive;
    ASSERT_EQ(ResultOk,
              client.createReader(topic, MessageId::latest(),
                                  ReaderConfiguration().setStartMessageIdInclusive(true), readerInclusive));

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

    readerExclusive.seek(seekMessageId);
    Message msg0;
    ASSERT_EQ(ResultOk, readerExclusive.readNext(msg0, 3000));

    readerInclusive.seek(seekMessageId);
    Message msg1;
    ASSERT_EQ(ResultOk, readerInclusive.readNext(msg1, 3000));

    LOG_INFO("readerExclusive received " << msg0.getDataAsString() << " from " << msg0.getMessageId());
    LOG_INFO("readerInclusive received " << msg1.getDataAsString() << " from " << msg1.getMessageId());

    ASSERT_EQ(msg0.getDataAsString(), "msg-" + std::to_string(r + 1));
    ASSERT_EQ(msg1.getDataAsString(), "msg-" + std::to_string(r));

    readerExclusive.close();
    readerInclusive.close();
    producer.close();
}

TEST(ReaderSeekTest, testStartAtLatestMessageId) {
    Client client(serviceUrl);

    const std::string topic = "test-seek-latest-message-id-" + std::to_string(time(nullptr));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));

    MessageId id;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("msg").build(), id));

    Reader readerExclusive;
    ASSERT_EQ(ResultOk,
              client.createReader(topic, MessageId::latest(), ReaderConfiguration(), readerExclusive));

    Reader readerInclusive;
    ASSERT_EQ(ResultOk,
              client.createReader(topic, MessageId::latest(),
                                  ReaderConfiguration().setStartMessageIdInclusive(true), readerInclusive));

    Message msg;
    bool hasMsgAvaliable = false;
    readerInclusive.hasMessageAvailable(hasMsgAvaliable);
    ASSERT_TRUE(hasMsgAvaliable);
    ASSERT_EQ(ResultOk, readerInclusive.readNext(msg, 3000));
    ASSERT_EQ(ResultTimeout, readerExclusive.readNext(msg, 3000));

    readerExclusive.close();
    readerInclusive.close();
    producer.close();
}

TEST(ReaderTest, testSeekInProgress) {
    Client client(serviceUrl);
    const auto topic = "test-seek-in-progress-" + std::to_string(time(nullptr));
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topic, MessageId::earliest(), {}, reader));

    reader.seekAsync(MessageId::earliest(), [](Result) {});
    Promise<Result, Result> promise;
    reader.seekAsync(MessageId::earliest(), [promise](Result result) { promise.setValue(result); });
    Result result;
    promise.getFuture().get(result);
    ASSERT_EQ(result, ResultNotAllowedError);
    client.close();
}

TEST_P(ReaderTest, testHasMessageAvailableAfterSeekToEnd) {
    Client client(serviceUrl);
    const auto topic = "test-has-message-available-after-seek-to-end-" + std::to_string(time(nullptr));
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topic, MessageId::earliest(), {}, reader));

    producer.send(MessageBuilder().setContent("msg-0").build());
    producer.send(MessageBuilder().setContent("msg-1").build());

    bool hasMessageAvailable;
    if (GetParam()) {
        // Test the case when `ConsumerImpl.lastMessageIdInBroker_` has been initialized
        reader.hasMessageAvailable(hasMessageAvailable);
    }

    ASSERT_EQ(ResultOk, reader.seek(MessageId::latest()));
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_FALSE(hasMessageAvailable);

    producer.send(MessageBuilder().setContent("msg-2").build());
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_TRUE(hasMessageAvailable);

    Message msg;
    ASSERT_EQ(ResultOk, reader.readNext(msg, 1000));
    ASSERT_EQ("msg-2", msg.getDataAsString());

    // Test the 2nd seek
    ASSERT_EQ(ResultOk, reader.seek(MessageId::latest()));
    ASSERT_EQ(ResultOk, reader.hasMessageAvailable(hasMessageAvailable));
    ASSERT_FALSE(hasMessageAvailable);

    client.close();
}

INSTANTIATE_TEST_SUITE_P(Pulsar, ReaderTest, ::testing::Values(true, false));

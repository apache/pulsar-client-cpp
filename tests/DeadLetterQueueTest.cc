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

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "lib/ClientConnection.h"
#include "lib/LogUtils.h"
#include "lib/MessageIdUtil.h"
#include "lib/UnAckedMessageTrackerEnabled.h"
#include "lib/Utils.h"

static const std::string lookupUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

DECLARE_LOG_OBJECT()

namespace pulsar {

TEST(DeadLetterQueueTest, testDLQWithSchema) {
    Client client(lookupUrl);
    const std::string topic = "testAutoSchema-" + std::to_string(time(nullptr));
    const std::string subName = "dlq-sub";

    static const std::string jsonSchema =
        R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
    SchemaInfo schemaInfo(JSON, "test-json", jsonSchema);

    auto dlqPolicy = DeadLetterPolicyBuilder()
                         .maxRedeliverCount(3)
                         .deadLetterTopic(topic + subName + "-DLQ")
                         .initialSubscriptionName("init-sub")
                         .build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    consumerConfig.setConsumerType(ConsumerType::ConsumerShared);
    consumerConfig.setSchema(schemaInfo);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    // Initialize the DLQ subscription first and make sure that DLQ topic is created and a schema exists.
    ConsumerConfiguration dlqConsumerConfig;
    dlqConsumerConfig.setConsumerType(ConsumerType::ConsumerShared);
    dlqConsumerConfig.setSchema(schemaInfo);
    Consumer deadLetterConsumer;
    ASSERT_EQ(ResultOk, client.subscribe(dlqPolicy.getDeadLetterTopic(), subName, dlqConsumerConfig,
                                         deadLetterConsumer));

    Producer producer;
    ProducerConfiguration producerConfig;
    producerConfig.setSchema(schemaInfo);
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfig, producer));
    std::string data = "{\"re\":2.1,\"im\":1.23}";
    const int num = 10;
    for (int i = 0; i < num; ++i) {
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(data).build()));
    }

    // nack all msg.
    Message msg;
    for (int i = 0; i < dlqPolicy.getMaxRedeliverCount() * num + num; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        consumer.negativeAcknowledge(msg);
    }

    // assert dlq msg.
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(ResultOk, deadLetterConsumer.receive(msg, 5000));
        ASSERT_FALSE(msg.getDataAsString().empty());
        ASSERT_TRUE(msg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC).find(topic));
        ASSERT_FALSE(msg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID).empty());
    }
    ASSERT_EQ(ResultTimeout, deadLetterConsumer.receive(msg, 200));

    client.close();
}

// If the user never receives this message, the message should not be delivered to the DLQ.
TEST(DeadLetterQueueTest, testWithoutConsumerReceiveImmediately) {
    Client client(lookupUrl);
    const std::string topic = "testWithoutConsumerReceiveImmediately-" + std::to_string(time(nullptr));
    const std::string subName = "dlq-sub";
    auto dlqPolicy =
        DeadLetterPolicyBuilder().maxRedeliverCount(3).initialSubscriptionName("init-sub").build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    consumerConfig.setConsumerType(ConsumerType::ConsumerShared);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    // set ack timeout is 10 ms.
    auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
    consumerImpl.unAckedMessageTrackerPtr_.reset(
        new UnAckedMessageTrackerEnabled(10, PulsarFriend::getClientImplPtr(client), consumerImpl));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    producer.send(MessageBuilder().setContent("msg").build());

    // Wait a while, message should not be send to DLQ
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    Message msg;
    ASSERT_EQ(ResultOk, consumer.receive(msg));
    client.close();
}

TEST(DeadLetterQueueTest, testAutoSetDLQTopicName) {
    Client client(lookupUrl);
    const std::string topic = "testAutoSetDLQName-" + std::to_string(time(nullptr));
    const std::string subName = "dlq-sub";
    const std::string dlqTopic = "persistent://public/default/" + topic + "-" + subName + "-DLQ";
    auto dlqPolicy =
        DeadLetterPolicyBuilder().maxRedeliverCount(3).initialSubscriptionName("init-sub").build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    consumerConfig.setConsumerType(ConsumerType::ConsumerShared);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
    ASSERT_EQ(consumerImpl.deadLetterPolicy_.getDeadLetterTopic(), dlqTopic);

    client.close();
}

class DeadLetterQueueTest : public ::testing::TestWithParam<std::tuple<bool, bool, ConsumerType>> {
   public:
    void SetUp() override {
        isProducerBatch_ = std::get<0>(GetParam());
        isMultiConsumer_ = std::get<1>(GetParam());
        consumerType_ = std::get<2>(GetParam());
        producerConf_ = ProducerConfiguration().setBatchingEnabled(isProducerBatch_);
    }

    void TearDown() override { client_.close(); }

    void initTopic(std::string topicName) {
        if (isMultiConsumer_) {
            // call admin api to make it partitioned
            std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
            int res = makePutRequest(url, "5");
            LOG_INFO("res = " << res);
            ASSERT_FALSE(res != 204 && res != 409);
        }
    }

   protected:
    Client client_{lookupUrl};
    ProducerConfiguration producerConf_;
    bool isMultiConsumer_;
    bool isProducerBatch_;
    ConsumerType consumerType_;
};

TEST_P(DeadLetterQueueTest, testSendDLQTriggerByAckTimeOutAndNeAck) {
    Client client(lookupUrl);
    const std::string topic = "testSendDLQTriggerByAckTimeOut-" + std::to_string(time(nullptr)) +
                              std::to_string(isMultiConsumer_) + std::to_string(isProducerBatch_) +
                              std::to_string(consumerType_);
    const std::string subName = "dlq-sub";
    const std::string dlqTopic = topic + "-" + subName + "-DLQ";
    initTopic(topic);

    auto dlqPolicy = DeadLetterPolicyBuilder().maxRedeliverCount(3).deadLetterTopic(dlqTopic).build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    consumerConfig.setConsumerType(consumerType_);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    // Reset the unack timeout
    long unackTimeOut = 200;
    if (isMultiConsumer_) {
        auto multiConsumer = PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
        multiConsumer->unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
            unackTimeOut, PulsarFriend::getClientImplPtr(client), *multiConsumer));
        multiConsumer->consumers_.forEachValue([&client, unackTimeOut](ConsumerImplPtr consumer) {
            consumer->unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
                unackTimeOut, PulsarFriend::getClientImplPtr(client), *consumer));
        });
    } else {
        auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
        consumerImpl.unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
            unackTimeOut, PulsarFriend::getClientImplPtr(client), consumerImpl));
    }

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf_, producer));
    const int num = 100;
    Message msg;
    for (int i = 0; i < num; ++i) {
        msg = MessageBuilder()
                  .setContent(std::to_string(i))
                  .setPartitionKey("p-key")
                  .setOrderingKey("o-key")
                  .setProperty("pk-1", "pv-1")
                  .build();
        producer.sendAsync(msg, [](Result res, const MessageId &msgId) { ASSERT_EQ(res, ResultOk); });
    }

    // receive messages and don't ack.
    for (int i = 0; i < dlqPolicy.getMaxRedeliverCount() * num + num; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        // Randomly specify some messages manually negativeAcknowledge.
        if (rand() % 2 == 0) {
            consumer.negativeAcknowledge(msg);
        }
    }

    // assert dlq msg.
    Consumer deadLetterQueueConsumer;
    ConsumerConfiguration dlqConsumerConfig;
    dlqConsumerConfig.setSubscriptionInitialPosition(InitialPositionEarliest);
    ASSERT_EQ(ResultOk, client.subscribe(dlqTopic, "dlq-sub", dlqConsumerConfig, deadLetterQueueConsumer));
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(ResultOk, deadLetterQueueConsumer.receive(msg));
        ASSERT_FALSE(msg.getDataAsString().empty());
        ASSERT_EQ(msg.getPartitionKey(), "p-key");
        ASSERT_EQ(msg.getOrderingKey(), "o-key");
        ASSERT_EQ(msg.getProperty("pk-1"), "pv-1");
        ASSERT_TRUE(msg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC).find(topic));
        ASSERT_FALSE(msg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID).empty());
    }

    ASSERT_EQ(ResultTimeout, deadLetterQueueConsumer.receive(msg, 200));
}

TEST_P(DeadLetterQueueTest, testSendDLQTriggerByRedeliverUnacknowledgedMessages) {
    Client client(lookupUrl);
    const std::string topic = "testSendDLQTriggerByRedeliverUnacknowledgedMessages-" +
                              std::to_string(time(nullptr)) + std::to_string(isMultiConsumer_) +
                              std::to_string(isProducerBatch_) + std::to_string(consumerType_);
    const std::string subName = "dlq-sub";
    const std::string dlqTopic = topic + subName + "DLQ";
    initTopic(topic);

    auto dlqPolicy = DeadLetterPolicyBuilder().maxRedeliverCount(3).deadLetterTopic(dlqTopic).build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setConsumerType(consumerType_);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf_, producer));

    const int num = 10;
    Message msg;
    for (int i = 0; i < num; ++i) {
        msg = MessageBuilder()
                  .setContent(std::to_string(i))
                  .setPartitionKey("p-key")
                  .setOrderingKey("o-key")
                  .setProperty("pk-1", "pv-1")
                  .build();
        producer.sendAsync(msg, [](Result res, const MessageId &msgId) { ASSERT_EQ(res, ResultOk); });
    }

    // nack all msg.
    for (int i = 1; i <= dlqPolicy.getMaxRedeliverCount() * num + num; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        if (i % num == 0) {
            consumer.redeliverUnacknowledgedMessages();
        }
    }

    // assert dlq msg.
    Consumer deadLetterQueueConsumer;
    ConsumerConfiguration dlqConsumerConfig;
    dlqConsumerConfig.setSubscriptionInitialPosition(InitialPositionEarliest);
    ASSERT_EQ(ResultOk, client.subscribe(dlqTopic, "dlq-sub", dlqConsumerConfig, deadLetterQueueConsumer));
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(ResultOk, deadLetterQueueConsumer.receive(msg));
        ASSERT_FALSE(msg.getDataAsString().empty());
        ASSERT_EQ(msg.getPartitionKey(), "p-key");
        ASSERT_EQ(msg.getOrderingKey(), "o-key");
        ASSERT_EQ(msg.getProperty("pk-1"), "pv-1");
        ASSERT_TRUE(msg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC).find(topic));
        ASSERT_FALSE(msg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID).empty());
    }
    ASSERT_EQ(ResultTimeout, deadLetterQueueConsumer.receive(msg, 200));
}

TEST_P(DeadLetterQueueTest, testSendDLQTriggerByNegativeAcknowledge) {
    Client client(lookupUrl);
    const std::string topic = "testSendDLQTriggerByNegativeAcknowledge-" + std::to_string(time(nullptr)) +
                              std::to_string(isMultiConsumer_) + std::to_string(isProducerBatch_) +
                              std::to_string(consumerType_);
    const std::string subName = "dlq-sub";
    const std::string dlqTopic = topic + subName + "DLQ";
    initTopic(topic);

    auto dlqPolicy = DeadLetterPolicyBuilder().maxRedeliverCount(3).deadLetterTopic(dlqTopic).build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    consumerConfig.setConsumerType(consumerType_);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf_, producer));

    const int num = 10;
    Message msg;
    for (int i = 0; i < num; ++i) {
        msg = MessageBuilder()
                  .setContent(std::to_string(i))
                  .setPartitionKey("p-key")
                  .setOrderingKey("o-key")
                  .setProperty("pk-1", "pv-1")
                  .build();
        producer.sendAsync(msg, [](Result res, const MessageId &msgId) { ASSERT_EQ(res, ResultOk); });
    }

    // nack all msg.
    for (int i = 0; i < dlqPolicy.getMaxRedeliverCount() * num + num; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        consumer.negativeAcknowledge(msg);
    }

    // assert dlq msg.
    Consumer deadLetterQueueConsumer;
    ConsumerConfiguration dlqConsumerConfig;
    dlqConsumerConfig.setSubscriptionInitialPosition(InitialPositionEarliest);
    ASSERT_EQ(ResultOk, client.subscribe(dlqTopic, "dlq-sub", dlqConsumerConfig, deadLetterQueueConsumer));
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(ResultOk, deadLetterQueueConsumer.receive(msg));
        ASSERT_FALSE(msg.getDataAsString().empty());
        ASSERT_EQ(msg.getPartitionKey(), "p-key");
        ASSERT_EQ(msg.getOrderingKey(), "o-key");
        ASSERT_EQ(msg.getProperty("pk-1"), "pv-1");
        ASSERT_TRUE(msg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC).find(topic));
        ASSERT_FALSE(msg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID).empty());
    }
    ASSERT_EQ(ResultTimeout, deadLetterQueueConsumer.receive(msg, 200));
}

TEST_P(DeadLetterQueueTest, testInitSubscription) {
    Client client(lookupUrl);
    const std::string topic = "testInitSubscription-" + std::to_string(time(nullptr)) +
                              std::to_string(isMultiConsumer_) + std::to_string(isProducerBatch_) +
                              std::to_string(consumerType_);
    const std::string subName = "dlq-sub";
    const std::string dlqTopic = topic + subName + "DLQ";
    const std::string dlqInitSub = "dlq-init-sub";
    initTopic(topic);

    auto dlqPolicy = DeadLetterPolicyBuilder()
                         .maxRedeliverCount(3)
                         .initialSubscriptionName(dlqInitSub)
                         .deadLetterTopic(dlqTopic)
                         .build();
    ConsumerConfiguration consumerConfig;
    consumerConfig.setDeadLetterPolicy(dlqPolicy);
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    consumerConfig.setConsumerType(consumerType_);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));

    Consumer deadLetterQueueConsumer;
    ConsumerConfiguration dlqConsumerConfig;
    dlqConsumerConfig.setSubscriptionInitialPosition(InitialPositionEarliest);
    ASSERT_EQ(ResultOk, client.subscribe(dlqTopic, "dlq-sub", dlqConsumerConfig, deadLetterQueueConsumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf_, producer));

    const int num = 10;
    Message msg;
    for (int i = 0; i < num; ++i) {
        msg = MessageBuilder().setContent(std::to_string(i)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // nack all msg.
    for (int i = 0; i < dlqPolicy.getMaxRedeliverCount() * num + num; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        consumer.negativeAcknowledge(msg);
    }

    // Use this subscription to ensure that messages are sent to the DLQ.
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(ResultOk, deadLetterQueueConsumer.receive(msg));
        ASSERT_FALSE(msg.getDataAsString().empty());
        ASSERT_TRUE(msg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC).find(topic));
        ASSERT_FALSE(msg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID).empty());
    }

    // If there is no initial subscription, then the subscription will not receive the DLQ messages sent
    // before the subscription.
    Consumer initDLQConsumer;
    ConsumerConfiguration initDLQConsumerConfig;
    dlqConsumerConfig.setSubscriptionInitialPosition(InitialPositionLatest);
    ASSERT_EQ(ResultOk, client.subscribe(dlqTopic, dlqInitSub, initDLQConsumerConfig, initDLQConsumer));
    for (int i = 0; i < num; i++) {
        ASSERT_EQ(ResultOk, initDLQConsumer.receive(msg, 1000));
        ASSERT_FALSE(msg.getDataAsString().empty());
        ASSERT_TRUE(msg.getProperty(SYSTEM_PROPERTY_REAL_TOPIC).find(topic));
        ASSERT_FALSE(msg.getProperty(PROPERTY_ORIGIN_MESSAGE_ID).empty());
    }
    ASSERT_EQ(ResultTimeout, initDLQConsumer.receive(msg, 200));
}

INSTANTIATE_TEST_SUITE_P(Pulsar, DeadLetterQueueTest,
                         testing::Combine(testing::Values(true, false), testing::Values(true, false),
                                          testing::Values(ConsumerType::ConsumerShared,
                                                          ConsumerType::ConsumerKeyShared)),
                         [](const testing::TestParamInfo<DeadLetterQueueTest::ParamType> &info) {
                             return "isBatch_" + std::to_string(std::get<0>(info.param)) + "_isMultiTopics_" +
                                    std::to_string(std::get<1>(info.param)) + "_subType_" +
                                    std::to_string(std::get<2>(info.param));
                         });

}  // namespace pulsar

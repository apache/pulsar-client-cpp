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

#include <array>
#include <atomic>
#include <chrono>
#include <ctime>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "HttpHelper.h"
#include "NoOpsCryptoKeyReader.h"
#include "PulsarFriend.h"
#include "lib/ClientConnection.h"
#include "lib/Future.h"
#include "lib/LogUtils.h"
#include "lib/MessageIdUtil.h"
#include "lib/MultiTopicsConsumerImpl.h"
#include "lib/TimeUtils.h"
#include "lib/UnAckedMessageTrackerDisabled.h"
#include "lib/UnAckedMessageTrackerEnabled.h"
#include "lib/Utils.h"
#include "lib/stats/ProducerStatsImpl.h"

static const std::string lookupUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

DECLARE_LOG_OBJECT()

namespace pulsar {

class ConsumerStateEventListener : public ConsumerEventListener {
   public:
    ConsumerStateEventListener(std::string name) { name_ = name; }

    void becameActive(Consumer consumer, int partitionId) override {
        LOG_INFO("Received consumer active event, partitionId:" << partitionId << ", name: " << name_);
        activeQueue_.push(partitionId);
    }

    void becameInactive(Consumer consumer, int partitionId) override {
        LOG_INFO("Received consumer inactive event, partitionId:" << partitionId << ", name: " << name_);
        inActiveQueue_.push(partitionId);
    }

    std::queue<int> activeQueue_;
    std::queue<int> inActiveQueue_;
    std::string name_;
};

typedef std::shared_ptr<ConsumerStateEventListener> ConsumerStateEventListenerPtr;

void verifyConsumerNotReceiveAnyStateChanges(ConsumerStateEventListenerPtr listener) {
    ASSERT_EQ(0, listener->activeQueue_.size());
    ASSERT_EQ(0, listener->inActiveQueue_.size());
}

void verifyConsumerActive(ConsumerStateEventListenerPtr listener, int partitionId) {
    ASSERT_NE(0, listener->activeQueue_.size());
    int pid = listener->activeQueue_.front();
    listener->activeQueue_.pop();
    ASSERT_EQ(partitionId, pid);
    ASSERT_EQ(0, listener->inActiveQueue_.size());
}

void verifyConsumerInactive(ConsumerStateEventListenerPtr listener, int partitionId) {
    ASSERT_NE(0, listener->inActiveQueue_.size());
    int pid = listener->inActiveQueue_.front();
    listener->inActiveQueue_.pop();
    ASSERT_EQ(partitionId, pid);
    ASSERT_EQ(0, listener->activeQueue_.size());
}

class ActiveInactiveListenerEvent : public ConsumerEventListener {
   public:
    void becameActive(Consumer consumer, int partitionId) override {
        Lock lock(mutex_);
        activePartitonIds_.emplace(partitionId);
        inactivePartitionIds_.erase(partitionId);
    }

    void becameInactive(Consumer consumer, int partitionId) override {
        Lock lock(mutex_);
        activePartitonIds_.erase(partitionId);
        inactivePartitionIds_.emplace(partitionId);
    }

    typedef std::unique_lock<std::mutex> Lock;
    std::set<int> activePartitonIds_;
    std::set<int> inactivePartitionIds_;
    std::mutex mutex_;
};

TEST(ConsumerTest, testConsumerIndex) {
    Client client(lookupUrl);
    const std::string topicName = "testConsumerIndex-topic-" + std::to_string(time(nullptr));
    const std::string subName = "sub";
    Producer producer;
    Result producerResult = client.createProducer(topicName, producer);
    ASSERT_EQ(producerResult, ResultOk);
    Consumer consumer;
    Result consumerResult = client.subscribe(topicName, subName, consumer);
    ASSERT_EQ(consumerResult, ResultOk);
    const auto msg = MessageBuilder().setContent("testConsumeSuccess").build();
    Result sendResult = producer.send(msg);
    ASSERT_EQ(sendResult, ResultOk);
    Message receivedMsg;
    Result receiveResult = consumer.receive(receivedMsg);
    ASSERT_EQ(receiveResult, ResultOk);
    ASSERT_EQ(receivedMsg.getDataAsString(), "testConsumeSuccess");
    ASSERT_EQ(receivedMsg.getIndex(), -1);
    client.close();
}

typedef std::shared_ptr<ActiveInactiveListenerEvent> ActiveInactiveListenerEventPtr;

TEST(ConsumerTest, testConsumerEventWithoutPartition) {
    Client client(lookupUrl);

    const std::string topicName = "testConsumerEventWithoutPartition-topic-" + std::to_string(time(nullptr));
    const std::string subName = "sub";
    const int waitTimeInMs = 1000;
    // constexpr int unAckedMessagesTimeoutMs = 10000;
    // constexpr int tickDurationInMs = 1000;

    // 1. two consumers on the same subscription
    Consumer consumer1;
    ConsumerConfiguration config1;
    ConsumerStateEventListenerPtr listener1 = std::make_shared<ConsumerStateEventListener>("listener-1");
    config1.setConsumerEventListener(listener1);
    config1.setConsumerName("consumer-1");
    config1.setConsumerType(ConsumerType::ConsumerFailover);

    ASSERT_EQ(pulsar::ResultOk, client.subscribe(topicName, subName, config1, consumer1));
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeInMs * 2));

    Consumer consumer2;
    ConsumerConfiguration config2;
    ConsumerStateEventListenerPtr listener2 = std::make_shared<ConsumerStateEventListener>("listener-2");
    config2.setConsumerEventListener(listener2);
    config2.setConsumerName("consumer-2");
    config2.setConsumerType(ConsumerType::ConsumerFailover);

    ASSERT_EQ(pulsar::ResultOk, client.subscribe(topicName, subName, config2, consumer2));
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeInMs * 2));

    verifyConsumerActive(listener1, -1);
    verifyConsumerInactive(listener2, -1);

    // clear inActiveQueue_
    std::queue<int>().swap(listener2->inActiveQueue_);

    consumer1.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeInMs * 2));
    verifyConsumerActive(listener2, -1);
    verifyConsumerNotReceiveAnyStateChanges(listener1);
}

TEST(ConsumerTest, testConsumerEventWithPartition) {
    Client client(lookupUrl);

    const int numPartitions = 4;
    const std::string partitionedTopic =
        "testConsumerEventWithPartition-topic-" + std::to_string(time(nullptr));
    const std::string subName = "sub";
    const int numOfMessages = 100;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    // two consumers on the same subscription
    Consumer consumer1;
    ConsumerConfiguration config1;
    ActiveInactiveListenerEventPtr listener1 = std::make_shared<ActiveInactiveListenerEvent>();
    config1.setConsumerEventListener(listener1);
    config1.setConsumerName("consumer-1");
    config1.setConsumerType(ConsumerType::ConsumerFailover);
    config1.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    config1.setTickDurationInMs(tickDurationInMs);
    client.subscribe(partitionedTopic, subName, config1, consumer1);

    Consumer consumer2;
    ConsumerConfiguration config2;
    ActiveInactiveListenerEventPtr listener2 = std::make_shared<ActiveInactiveListenerEvent>();
    config2.setConsumerEventListener(listener2);
    config2.setConsumerName("consumer-2");
    config2.setConsumerType(ConsumerType::ConsumerFailover);
    config1.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    config1.setTickDurationInMs(tickDurationInMs);
    client.subscribe(partitionedTopic, subName, config2, consumer2);

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    producer.flush();
    producer.close();

    // receive message and check partitionIds on consumer1
    std::set<int> receivedPartitionIds;
    while (true) {
        Message msg;
        Result rc = consumer1.receive(msg, 1000);
        if (pulsar::ResultOk != rc) {
            break;
        }

        MessageId msgId = msg.getMessageId();
        int32_t partitionIndex = msgId.partition();
        ASSERT_TRUE(partitionIndex < numPartitions);
        consumer1.acknowledge(msgId);
        receivedPartitionIds.insert(partitionIndex);
    }

    std::set<int> result;
    std::set_difference(listener1->activePartitonIds_.begin(), listener1->activePartitonIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());

    std::set<int>().swap(result);
    std::set_difference(listener2->inactivePartitionIds_.begin(), listener2->inactivePartitionIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());

    // receive message and check partitionIds on consumer2
    std::set<int>().swap(receivedPartitionIds);
    while (true) {
        Message msg;
        Result rc = consumer2.receive(msg, 1000);
        if (pulsar::ResultOk != rc) {
            break;
        }
        MessageId msgId = msg.getMessageId();
        int32_t partitionIndex = msgId.partition();
        ASSERT_TRUE(partitionIndex < numPartitions);
        consumer2.acknowledge(msgId);
        receivedPartitionIds.insert(partitionIndex);
    }

    std::set<int>().swap(result);
    std::set_difference(listener2->activePartitonIds_.begin(), listener2->activePartitonIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());

    std::set<int>().swap(result);
    std::set_difference(listener1->inactivePartitionIds_.begin(), listener1->inactivePartitionIds_.end(),
                        receivedPartitionIds.begin(), receivedPartitionIds.end(),
                        std::inserter(result, result.end()));
    ASSERT_EQ(0, result.size());
}

TEST(ConsumerTest, testAcknowledgeCumulativeWithPartition) {
    Client client(lookupUrl);

    const std::string topic = "testAcknowledgeCumulativeWithPartition-" + std::to_string(time(nullptr));
    const std::string subName = "sub";

    int res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + topic + "/partitions",
                             std::to_string(2));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfiguration;
    consumerConfiguration.setUnAckedMessagesTimeoutMs(10000);
    ASSERT_EQ(ResultOk, client.subscribe(topic, "t-sub", consumerConfiguration, consumer));

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setBatchingEnabled(false);
    producerConfiguration.setPartitionsRoutingMode(
        ProducerConfiguration::PartitionsRoutingMode::RoundRobinDistribution);
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    const int numMessages = 100;
    for (int i = 0; i < numMessages; ++i) {
        Message msg = MessageBuilder().setContent(std::to_string(i)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    Message msg;
    std::array<MessageId, 2> latestMsgIds;
    for (int i = 0; i < numMessages; i++) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        // The last message of each partition topic be ACK
        latestMsgIds[msg.getMessageId().partition()] = msg.getMessageId();
    }
    ASSERT_EQ(ResultTimeout, consumer.receive(msg, 2000));
    for (auto&& msgId : latestMsgIds) {
        consumer.acknowledgeCumulative(msgId);
    }

    // Assert that there is no message in the tracker.
    auto multiConsumerImpl = PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
    auto tracker =
        static_cast<UnAckedMessageTrackerEnabled*>(multiConsumerImpl->unAckedMessageTrackerPtr_.get());
    ASSERT_EQ(0, tracker->size());

    client.close();
}

TEST(ConsumerTest, consumerNotInitialized) {
    Consumer consumer;

    ASSERT_TRUE(consumer.getTopic().empty());
    ASSERT_TRUE(consumer.getSubscriptionName().empty());

    Message msg;
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.receive(msg));
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.receive(msg, 3000));

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledge(msg));

    MessageId msgId;
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledge(msgId));

    Result result;
    {
        Promise<bool, Result> promise;
        consumer.acknowledgeAsync(msg, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeAsync(msgId, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledgeCumulative(msg));
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledgeCumulative(msgId));

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeCumulativeAsync(msg, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeCumulativeAsync(msgId, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.close());

    {
        Promise<bool, Result> promise;
        consumer.closeAsync(WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.unsubscribe());

    {
        Promise<bool, Result> promise;
        consumer.unsubscribeAsync(WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }
}

TEST(ConsumerTest, testPartitionIndex) {
    Client client(lookupUrl);

    const std::string nonPartitionedTopic =
        "ConsumerTestPartitionIndex-topic-" + std::to_string(time(nullptr));
    const std::string partitionedTopic1 =
        "ConsumerTestPartitionIndex-par-topic1-" + std::to_string(time(nullptr));
    const std::string partitionedTopic2 =
        "ConsumerTestPartitionIndex-par-topic2-" + std::to_string(time(nullptr));
    constexpr int numPartitions = 3;

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic1 + "/partitions", "1");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic2 + "/partitions",
                         std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    auto sendMessageToTopic = [&client](const std::string& topic) {
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producer));

        Message msg = MessageBuilder().setContent("hello").build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    };

    // consumers
    //   [0] subscribes a non-partitioned topic
    //   [1] subscribes a partition of a partitioned topic
    //   [2] subscribes a partitioned topic
    Consumer consumers[3];
    ASSERT_EQ(ResultOk, client.subscribe(nonPartitionedTopic, "sub", consumers[0]));
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic1 + "-partition-0", "sub", consumers[1]));
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic2, "sub", consumers[2]));

    sendMessageToTopic(nonPartitionedTopic);
    sendMessageToTopic(partitionedTopic1);
    for (int i = 0; i < numPartitions; i++) {
        sendMessageToTopic(partitionedTopic2 + "-partition-" + std::to_string(i));
    }

    Message msg;
    ASSERT_EQ(ResultOk, consumers[0].receive(msg, 5000));
    ASSERT_EQ(msg.getMessageId().partition(), -1);

    ASSERT_EQ(ResultOk, consumers[1].receive(msg, 5000));
    ASSERT_EQ(msg.getMessageId().partition(), 0);

    std::set<int> partitionIndexes;
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(ResultOk, consumers[2].receive(msg, 5000));
        partitionIndexes.emplace(msg.getMessageId().partition());
    }
    ASSERT_EQ(partitionIndexes, (std::set<int>{0, 1, 2}));

    client.close();
}

TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery) {
    Client client(lookupUrl);
    const std::string partitionedTopic =
        "testPartitionedConsumerUnAckedMessageRedelivery" + std::to_string(time(nullptr));
    std::string subName = "sub-partition-consumer-un-acked-msg-redelivery";
    constexpr int numPartitions = 3;
    constexpr int numOfMessages = 15;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, subName, consumerConfig, consumer));

    MultiTopicsConsumerImplPtr partitionedConsumerImplPtr =
        PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
    ASSERT_EQ(numPartitions, partitionedConsumerImplPtr->consumers_.size());

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    producer.close();

    // receive message and don't acknowledge
    std::set<MessageId> messageIds[numPartitions];
    for (auto i = 0; i < numOfMessages; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));

        MessageId msgId = msg.getMessageId();
        int32_t partitionIndex = msgId.partition();
        ASSERT_TRUE(partitionIndex < numPartitions);
        messageIds[msgId.partition()].emplace(msgId);
    }

    auto partitionedTracker = static_cast<UnAckedMessageTrackerEnabled*>(
        partitionedConsumerImplPtr->unAckedMessageTrackerPtr_.get());
    ASSERT_EQ(numOfMessages, partitionedTracker->size());
    ASSERT_FALSE(partitionedTracker->isEmpty());
    for (auto i = 0; i < numPartitions; i++) {
        auto topicName =
            "persistent://public/default/" + partitionedTopic + "-partition-" + std::to_string(i);
        ASSERT_EQ(numOfMessages / numPartitions, messageIds[i].size());
        auto subConsumerPtr = partitionedConsumerImplPtr->consumers_.find(topicName).value();
        auto tracker =
            static_cast<UnAckedMessageTrackerEnabled*>(subConsumerPtr->unAckedMessageTrackerPtr_.get());
        ASSERT_EQ(0, tracker->size());
        ASSERT_TRUE(tracker->isEmpty());
    }

    // timeout and send redeliver message
    std::this_thread::sleep_for(std::chrono::milliseconds(unAckedMessagesTimeoutMs + tickDurationInMs * 2));
    ASSERT_EQ(0, partitionedTracker->size());
    ASSERT_TRUE(partitionedTracker->isEmpty());

    for (auto i = 0; i < numOfMessages; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(1, partitionedTracker->size());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg.getMessageId()));
        ASSERT_EQ(0, partitionedTracker->size());
    }
    ASSERT_EQ(0, partitionedTracker->size());
    ASSERT_TRUE(partitionedTracker->isEmpty());
    partitionedTracker = NULL;

    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
    consumer.close();
    client.close();
}

TEST(ConsumerTest, testPartitionedConsumerUnexpectedAckTimeout) {
    ClientConfiguration clientConfig;
    clientConfig.setMessageListenerThreads(1);
    Client client(lookupUrl, clientConfig);

    const std::string partitionedTopic =
        "testPartitionedConsumerUnexpectedAckTimeout" + std::to_string(time(nullptr));
    std::string subName = "sub";
    constexpr int numPartitions = 2;
    constexpr int numOfMessages = 3;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;
    pulsar::Latch latch(numOfMessages);
    std::vector<Message> messages;
    std::mutex mtx;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setConsumerType(ConsumerShared);
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    consumerConfig.setMessageListener([&](Consumer cons, const Message& msg) {
        // acknowledge received messages immediately, so no ack timeout is expected
        ASSERT_EQ(ResultOk, cons.acknowledge(msg.getMessageId()));
        ASSERT_EQ(0, msg.getRedeliveryCount());

        {
            std::lock_guard<std::mutex> lock(mtx);
            messages.emplace_back(msg);
        }

        if (latch.getCount() > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(unAckedMessagesTimeoutMs + tickDurationInMs * 2));
            latch.countdown();
        }
    });
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, subName, consumerConfig, consumer));

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    producer.close();

    bool wasUnblocked = latch.wait(
        std::chrono::milliseconds((unAckedMessagesTimeoutMs + tickDurationInMs * 2) * numOfMessages + 5000));
    ASSERT_TRUE(wasUnblocked);

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    // messages are expected not to be redelivered
    ASSERT_EQ(numOfMessages, messages.size());

    consumer.close();
    client.close();
}

TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery) {
    Client client(lookupUrl);
    const std::string nonPartitionedTopic =
        "testMultiTopicsConsumerUnAckedMessageRedelivery-topic-" + std::to_string(time(nullptr));
    const std::string partitionedTopic1 =
        "testMultiTopicsConsumerUnAckedMessageRedelivery-par-topic1-" + std::to_string(time(nullptr));
    const std::string partitionedTopic2 =
        "testMultiTopicsConsumerUnAckedMessageRedelivery-par-topic2-" + std::to_string(time(nullptr));
    std::string subName = "sub-multi-topics-consumer-un-acked-msg-redelivery";
    constexpr int numPartitions = 3;
    constexpr int numOfMessages = 15;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic1 + "/partitions", "1");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic2 + "/partitions",
                         std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    const std::vector<std::string> topics = {nonPartitionedTopic, partitionedTopic1, partitionedTopic2};
    ASSERT_EQ(ResultOk, client.subscribe(topics, subName, consumerConfig, consumer));
    MultiTopicsConsumerImplPtr multiTopicsConsumerImplPtr =
        PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
    ASSERT_EQ(numPartitions + 2 /* nonPartitionedTopic + partitionedTopic1 */,
              multiTopicsConsumerImplPtr->consumers_.size());

    // send messages
    auto sendMessageToTopic = [&client](const std::string& topic) {
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producer));

        Message msg = MessageBuilder().setContent("hello").build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    };
    for (int i = 0; i < numOfMessages; i++) {
        sendMessageToTopic(nonPartitionedTopic);
        sendMessageToTopic(partitionedTopic1);
        sendMessageToTopic(partitionedTopic2);
    }

    // receive message and don't acknowledge
    for (auto i = 0; i < numOfMessages * 3; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        MessageId msgId = msg.getMessageId();
    }

    auto multiTopicsTracker = static_cast<UnAckedMessageTrackerEnabled*>(
        multiTopicsConsumerImplPtr->unAckedMessageTrackerPtr_.get());
    ASSERT_EQ(numOfMessages * 3, multiTopicsTracker->size());
    ASSERT_FALSE(multiTopicsTracker->isEmpty());

    std::vector<UnAckedMessageTrackerEnabled*> trackers;
    multiTopicsConsumerImplPtr->consumers_.forEach(
        [&trackers](const std::string& name, const ConsumerImplPtr& consumer) {
            trackers.emplace_back(
                static_cast<UnAckedMessageTrackerEnabled*>(consumer->unAckedMessageTrackerPtr_.get()));
        });
    for (const auto& tracker : trackers) {
        ASSERT_EQ(0, tracker->size());
        ASSERT_TRUE(tracker->isEmpty());
    }

    // timeout and send redeliver message
    std::this_thread::sleep_for(std::chrono::milliseconds(unAckedMessagesTimeoutMs + tickDurationInMs * 2));
    ASSERT_EQ(0, multiTopicsTracker->size());
    ASSERT_TRUE(multiTopicsTracker->isEmpty());

    for (auto i = 0; i < numOfMessages * 3; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(1, multiTopicsTracker->size());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg.getMessageId()));
        ASSERT_EQ(0, multiTopicsTracker->size());
    }
    ASSERT_EQ(0, multiTopicsTracker->size());
    ASSERT_TRUE(multiTopicsTracker->isEmpty());
    multiTopicsTracker = NULL;

    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
    consumer.close();
    client.close();
}

TEST(ConsumerTest, testBatchUnAckedMessageTracker) {
    Client client(lookupUrl);
    const std::string topic = "testBatchUnAckedMessageTracker" + std::to_string(time(nullptr));
    std::string subName = "sub-batch-un-acked-msg-tracker";
    constexpr int numOfMessages = 50;
    constexpr int batchSize = 5;
    constexpr int batchCount = numOfMessages / batchSize;
    constexpr int unAckedMessagesTimeoutMs = 10000;
    constexpr int tickDurationInMs = 1000;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    consumerConfig.setTickDurationInMs(tickDurationInMs);
    ASSERT_EQ(ResultOk, client.subscribe(topic, subName, consumerConfig, consumer));
    auto consumerImplPtr = PulsarFriend::getConsumerImplPtr(consumer);
    auto tracker =
        static_cast<UnAckedMessageTrackerEnabled*>(consumerImplPtr->unAckedMessageTrackerPtr_.get());

    // send messages
    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(true);
    producerConfig.setBlockIfQueueFull(true);
    producerConfig.setBatchingMaxMessages(batchSize);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfig, producer));
    std::string prefix = "message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg = MessageBuilder().setContent(messageContent).build();
        producer.sendAsync(msg, NULL);
    }
    producer.close();

    std::map<MessageId, std::vector<MessageId>> msgIdInBatchMap;
    std::vector<MessageId> messageIds;
    for (auto i = 0; i < numOfMessages; ++i) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        MessageId msgId = msg.getMessageId();
        msgIdInBatchMap[discardBatch(msgId)].emplace_back(msgId);
    }

    ASSERT_EQ(batchCount, msgIdInBatchMap.size());
    ASSERT_EQ(batchCount, tracker->size());
    for (const auto& iter : msgIdInBatchMap) {
        ASSERT_EQ(iter.second.size(), batchSize);
    }

    int ackedBatchCount = 0;
    for (auto iter = msgIdInBatchMap.begin(); iter != msgIdInBatchMap.end(); ++iter) {
        ASSERT_EQ(batchSize, iter->second.size());
        for (auto i = 0; i < iter->second.size(); ++i) {
            ASSERT_EQ(ResultOk, consumer.acknowledge(iter->second[i]));
        }
        ackedBatchCount++;
        ASSERT_EQ(batchCount - ackedBatchCount, tracker->size());
    }
    ASSERT_EQ(0, tracker->size());
    ASSERT_TRUE(tracker->isEmpty());

    consumer.close();
    client.close();
}

TEST(ConsumerTest, testGetTopicNameFromReceivedMessage) {
    // topic1 and topic2 are non-partitioned topics, topic3 is a partitioned topic
    const std::string topic1 = "testGetTopicNameFromReceivedMessage1-" + std::to_string(time(nullptr));
    const std::string topic2 = "testGetTopicNameFromReceivedMessage2-" + std::to_string(time(nullptr));
    const std::string topic3 = "testGetTopicNameFromReceivedMessage3-" + std::to_string(time(nullptr));
    int res = makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + topic3 + "/partitions", "3");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Client client(lookupUrl);

    auto sendMessage = [&client](const std::string& topic, bool enabledBatching) {
        const auto producerConf = ProducerConfiguration().setBatchingEnabled(enabledBatching);
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf, producer));
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("hello").build()));
        LOG_INFO("Send 'hello' to " << topic);
    };
    auto validateTopicName = [](Consumer& consumer, const std::string& topic) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));

        const auto fullTopic = "persistent://public/default/" + topic;
        ASSERT_EQ(msg.getTopicName(), fullTopic);
        ASSERT_EQ(msg.getMessageId().getTopicName(), fullTopic);
    };

    // 1. ConsumerImpl
    Consumer consumer1;
    ASSERT_EQ(ResultOk, client.subscribe(topic1, "sub-1", consumer1));

    // 2. MultiTopicsConsumerImpl
    Consumer consumer2;
    ASSERT_EQ(ResultOk, client.subscribe(std::vector<std::string>{topic1, topic2}, "sub-2", consumer2));

    sendMessage(topic1, true);
    validateTopicName(consumer1, topic1);
    validateTopicName(consumer2, topic1);
    sendMessage(topic1, false);
    validateTopicName(consumer1, topic1);
    validateTopicName(consumer2, topic1);

    // 3. PartitionedConsumerImpl
    Consumer consumer3;
    ASSERT_EQ(ResultOk, client.subscribe(topic3, "sub-3", consumer3));
    const auto partition = topic3 + "-partition-0";
    sendMessage(partition, true);
    validateTopicName(consumer3, partition);
    sendMessage(partition, false);
    validateTopicName(consumer3, partition);

    client.close();
}

TEST(ConsumerTest, testIsConnected) {
    Client client(lookupUrl);
    const std::string nonPartitionedTopic1 =
        "testConsumerIsConnectedNonPartitioned1-" + std::to_string(time(nullptr));
    const std::string nonPartitionedTopic2 =
        "testConsumerIsConnectedNonPartitioned2-" + std::to_string(time(nullptr));
    const std::string partitionedTopic =
        "testConsumerIsConnectedPartitioned-" + std::to_string(time(nullptr));
    const std::string subName = "sub";

    Consumer consumer;
    ASSERT_FALSE(consumer.isConnected());
    // ConsumerImpl
    ASSERT_EQ(ResultOk, client.subscribe(nonPartitionedTopic1, subName, consumer));
    ASSERT_TRUE(consumer.isConnected());
    ASSERT_EQ(ResultOk, consumer.close());
    ASSERT_FALSE(consumer.isConnected());

    // MultiTopicsConsumerImpl
    ASSERT_EQ(ResultOk, client.subscribe(std::vector<std::string>{nonPartitionedTopic1, nonPartitionedTopic2},
                                         subName, consumer));
    ASSERT_TRUE(consumer.isConnected());
    ASSERT_EQ(ResultOk, consumer.close());
    ASSERT_FALSE(consumer.isConnected());

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    // PartitionedConsumerImpl
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, subName, consumer));
    ASSERT_TRUE(consumer.isConnected());
    ASSERT_EQ(ResultOk, consumer.close());
    ASSERT_FALSE(consumer.isConnected());
}

TEST(ConsumerTest, testPartitionsWithCloseUnblock) {
    Client client(lookupUrl);
    const std::string partitionedTopic = "testPartitionsWithCloseUnblock" + std::to_string(time(nullptr));
    constexpr int numPartitions = 2;

    int res =
        makePutRequest(adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions",
                       std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopic, "SubscriptionName", consumerConfig, consumer));

    // send messages
    ProducerConfiguration producerConfig;
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfig, producer));
    Message msg = MessageBuilder().setContent("message").build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    producer.close();

    // receive message on another thread
    pulsar::Latch latch(1);
    auto thread = std::thread([&]() {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 10 * 1000));
        consumer.acknowledge(msg.getMessageId());
        ASSERT_EQ(ResultAlreadyClosed, consumer.receive(msg, 10 * 1000));
        latch.countdown();
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));

    consumer.close();

    bool wasUnblocked = latch.wait(std::chrono::milliseconds(100));

    ASSERT_TRUE(wasUnblocked);
    thread.join();
}

TEST(ConsumerTest, testGetLastMessageId) {
    Client client(lookupUrl);
    const std::string topic = "testGetLastMessageId-" + std::to_string(time(nullptr));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "test-sub", consumer));

    MessageId msgId;
    ASSERT_EQ(ResultOk, consumer.getLastMessageId(msgId));
    ASSERT_EQ(msgId, MessageId(-1, -1, -1, -1));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    Message msg = MessageBuilder().setContent("message").build();
    ASSERT_EQ(ResultOk, producer.send(msg));

    ASSERT_EQ(ResultOk, consumer.getLastMessageId(msgId));
    ASSERT_NE(msgId, MessageId(-1, -1, -1, -1));

    client.close();
}

TEST(ConsumerTest, testGetLastMessageIdBlockWhenConnectionDisconnected) {
    int operationTimeout = 5;
    ClientConfiguration clientConfiguration;
    clientConfiguration.setOperationTimeoutSeconds(operationTimeout);

    Client client(lookupUrl, clientConfiguration);
    const std::string topic =
        "testGetLastMessageIdBlockWhenConnectionDisconnected-" + std::to_string(time(nullptr));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "test-sub", consumer));

    ConsumerImpl& consumerImpl = PulsarFriend::getConsumerImpl(consumer);
    ClientConnectionWeakPtr conn = PulsarFriend::getClientConnection(consumerImpl);

    PulsarFriend::setClientConnection(consumerImpl, std::weak_ptr<ClientConnection>());

    pulsar::Latch latch(1);
    auto start = TimeUtils::now();

    consumerImpl.getLastMessageIdAsync([&latch](Result r, const GetLastMessageIdResponse&) -> void {
        ASSERT_EQ(r, ResultNotConnected);
        latch.countdown();
    });

    ASSERT_TRUE(latch.wait(std::chrono::seconds(20)));
    auto elapsed = TimeUtils::now() - start;

    // getLastMessageIdAsync should be blocked until operationTimeout when the connection is disconnected.
    ASSERT_GE(elapsed.seconds(), operationTimeout);
}

TEST(ConsumerTest, testRedeliveryOfDecryptionFailedMessages) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "testRedeliveryOfDecryptionFailedMessages" + std::to_string(time(nullptr));
    std::string subName = "sub-test";

    std::string PUBLIC_CERT_FILE_PATH = "../test-conf/public-key.client-rsa.pem";
    std::string PRIVATE_CERT_FILE_PATH = "../test-conf/private-key.client-rsa.pem";
    std::shared_ptr<pulsar::DefaultCryptoKeyReader> keyReader =
        std::make_shared<pulsar::DefaultCryptoKeyReader>(PUBLIC_CERT_FILE_PATH, PRIVATE_CERT_FILE_PATH);

    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.addEncryptionKey("client-rsa.pem");
    conf.setCryptoKeyReader(keyReader);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, conf, producer));

    ConsumerConfiguration consConfig1;
    consConfig1.setCryptoKeyReader(keyReader);
    consConfig1.setConsumerType(pulsar::ConsumerShared);
    consConfig1.setCryptoFailureAction(ConsumerCryptoFailureAction::FAIL);
    Consumer consumer1;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consConfig1, consumer1));

    ConsumerConfiguration consConfig2;
    consConfig2.setCryptoKeyReader(std::make_shared<NoOpsCryptoKeyReader>());
    consConfig2.setConsumerType(pulsar::ConsumerShared);
    consConfig2.setCryptoFailureAction(ConsumerCryptoFailureAction::FAIL);
    consConfig2.setUnAckedMessagesTimeoutMs(10000);
    Consumer consumer2;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consConfig2, consumer2));
    auto consumer2ImplPtr = PulsarFriend::getConsumerImplPtr(consumer2);
    consumer2ImplPtr->unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
        100, 100, PulsarFriend::getClientImplPtr(client), static_cast<ConsumerImplBase&>(*consumer2ImplPtr)));

    ConsumerConfiguration consConfig3;
    consConfig3.setConsumerType(pulsar::ConsumerShared);
    consConfig3.setCryptoFailureAction(ConsumerCryptoFailureAction::FAIL);
    consConfig3.setUnAckedMessagesTimeoutMs(10000);
    Consumer consumer3;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consConfig3, consumer3));
    auto consumer3ImplPtr = PulsarFriend::getConsumerImplPtr(consumer3);
    consumer3ImplPtr->unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
        100, 100, PulsarFriend::getClientImplPtr(client), static_cast<ConsumerImplBase&>(*consumer3ImplPtr)));

    int numberOfMessages = 20;
    std::string msgContent = "msg-content";
    std::set<std::string> valuesSent;
    Message msg;
    for (int i = 0; i < numberOfMessages; i++) {
        auto value = msgContent + std::to_string(i);
        valuesSent.emplace(value);
        msg = MessageBuilder().setContent(value).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // Consuming from consumer 2 and 3
    // no message should be returned since they can't decrypt the message
    ASSERT_EQ(ResultTimeout, consumer2.receive(msg, 1000));
    ASSERT_EQ(ResultTimeout, consumer3.receive(msg, 1000));

    // All messages would be received by consumer 1
    std::set<std::string> valuesReceived;
    for (int i = 0; i < numberOfMessages; i++) {
        ASSERT_EQ(ResultOk, consumer1.receive(msg, 2000));
        ASSERT_EQ(ResultOk, consumer1.acknowledge(msg));
        valuesReceived.emplace(msg.getDataAsString());
    }
    ASSERT_EQ(valuesSent, valuesReceived);

    // Consuming from consumer 2 and 3 again just to be sure
    // no message should be returned since they can't decrypt the message
    ASSERT_EQ(ResultTimeout, consumer2.receive(msg, 1000));
    ASSERT_EQ(ResultTimeout, consumer3.receive(msg, 1000));

    ASSERT_EQ(ResultOk, client.close());
}

TEST(ConsumerTest, testPatternSubscribeTopic) {
    Client client(lookupUrl);
    auto topicName = "testPatternSubscribeTopic" + std::to_string(time(nullptr));
    std::string topicName1 = "persistent://public/default/" + topicName + "1";
    std::string topicName2 = "persistent://public/default/" + topicName + "2";
    std::string topicName3 = "non-persistent://public/default/" + topicName + "3np";
    // This will not match pattern
    std::string topicName4 = "persistent://public/default/noMatch" + topicName;

    // 0. trigger create topic
    Producer producer1;
    Result result = client.createProducer(topicName1, producer1);
    ASSERT_EQ(ResultOk, result);
    Producer producer2;
    result = client.createProducer(topicName2, producer2);
    ASSERT_EQ(ResultOk, result);
    Producer producer3;
    result = client.createProducer(topicName3, producer3);
    ASSERT_EQ(ResultOk, result);
    Producer producer4;
    result = client.createProducer(topicName4, producer4);
    ASSERT_EQ(ResultOk, result);

    // verify sub persistent and non-persistent topic
    {
        // 1. Use pattern to sub topic1, topic2, topic3
        ConsumerConfiguration consConfig;
        consConfig.setConsumerType(ConsumerShared);
        consConfig.setRegexSubscriptionMode(RegexSubscriptionMode::AllTopics);
        Consumer consumer;
        std::string pattern = "public/default/" + topicName + ".*";
        ASSERT_EQ(ResultOk, client.subscribeWithRegex(pattern, "sub-all", consConfig, consumer));
        auto multiConsumerImplPtr = PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
        ASSERT_EQ(multiConsumerImplPtr->consumers_.size(), 3);
        ASSERT_TRUE(multiConsumerImplPtr->consumers_.find(topicName1));
        ASSERT_TRUE(multiConsumerImplPtr->consumers_.find(topicName2));
        ASSERT_TRUE(multiConsumerImplPtr->consumers_.find(topicName3));
        ASSERT_FALSE(multiConsumerImplPtr->consumers_.find(topicName4));

        // 2. send msg to topic1, topic2, topic3, topic4
        int messageNumber = 10;
        for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
            auto content = "msg-content" + std::to_string(msgNum);
            ASSERT_EQ(ResultOk, producer1.send(MessageBuilder().setContent(content).build()));
            ASSERT_EQ(ResultOk, producer2.send(MessageBuilder().setContent(content).build()));
            ASSERT_EQ(ResultOk, producer3.send(MessageBuilder().setContent(content).build()));
            ASSERT_EQ(ResultOk, producer4.send(MessageBuilder().setContent(content).build()));
        }

        // 3. receive msg from topic1, topic2, topic3
        Message m;
        for (int i = 0; i < 3 * messageNumber; i++) {
            ASSERT_EQ(ResultOk, consumer.receive(m, 1000));
            ASSERT_EQ(ResultOk, consumer.acknowledge(m));
        }
        // verify no more to receive, because producer4 not match pattern
        ASSERT_EQ(ResultTimeout, consumer.receive(m, 1000));
        ASSERT_EQ(ResultOk, consumer.unsubscribe());
    }

    // verify only sub persistent topic
    {
        ConsumerConfiguration consConfig;
        consConfig.setConsumerType(ConsumerShared);
        consConfig.setRegexSubscriptionMode(RegexSubscriptionMode::PersistentOnly);
        Consumer consumer;
        std::string pattern = "public/default/" + topicName + ".*";
        ASSERT_EQ(ResultOk, client.subscribeWithRegex(pattern, "sub-persistent", consConfig, consumer));
        auto multiConsumerImplPtr = PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
        ASSERT_EQ(multiConsumerImplPtr->consumers_.size(), 2);
        ASSERT_TRUE(multiConsumerImplPtr->consumers_.find(topicName1));
        ASSERT_TRUE(multiConsumerImplPtr->consumers_.find(topicName2));
        ASSERT_FALSE(multiConsumerImplPtr->consumers_.find(topicName3));
        ASSERT_FALSE(multiConsumerImplPtr->consumers_.find(topicName4));
        ASSERT_EQ(ResultOk, consumer.unsubscribe());
    }

    // verify only sub non-persistent topic
    {
        ConsumerConfiguration consConfig;
        consConfig.setConsumerType(ConsumerShared);
        consConfig.setRegexSubscriptionMode(RegexSubscriptionMode::NonPersistentOnly);
        Consumer consumer;
        std::string pattern = "public/default/" + topicName + ".*";
        ASSERT_EQ(ResultOk, client.subscribeWithRegex(pattern, "sub-non-persistent", consConfig, consumer));
        auto multiConsumerImplPtr = PulsarFriend::getMultiTopicsConsumerImplPtr(consumer);
        ASSERT_EQ(multiConsumerImplPtr->consumers_.size(), 1);
        ASSERT_FALSE(multiConsumerImplPtr->consumers_.find(topicName1));
        ASSERT_FALSE(multiConsumerImplPtr->consumers_.find(topicName2));
        ASSERT_TRUE(multiConsumerImplPtr->consumers_.find(topicName3));
        ASSERT_FALSE(multiConsumerImplPtr->consumers_.find(topicName4));
        ASSERT_EQ(ResultOk, consumer.unsubscribe());
    }

    client.close();
}

class ConsumerSeekTest : public ::testing::TestWithParam<bool> {
   public:
    void SetUp() override { producerConf_ = ProducerConfiguration().setBatchingEnabled(GetParam()); }

    void TearDown() override { client_.close(); }

   protected:
    Client client_{lookupUrl};
    ProducerConfiguration producerConf_;
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

TEST(ConsumerTest, testNegativeAcksTrackerClose) {
    Client client(lookupUrl);
    auto topicName = "testNegativeAcksTrackerClose";

    ConsumerConfiguration consumerConfig;
    consumerConfig.setNegativeAckRedeliveryDelayMs(100);
    Consumer consumer;
    client.subscribe(topicName, "test-sub", consumerConfig, consumer);

    Producer producer;
    client.createProducer(topicName, producer);

    for (int i = 0; i < 10; ++i) {
        producer.send(MessageBuilder().setContent(std::to_string(i)).build());
    }

    Message msg;
    PulsarFriend::setNegativeAckEnabled(consumer, false);
    for (int i = 0; i < 10; ++i) {
        consumer.receive(msg);
        consumer.negativeAcknowledge(msg);
    }

    consumer.close();
    auto consumerImplPtr = PulsarFriend::getConsumerImplPtr(consumer);
    ASSERT_TRUE(consumerImplPtr->negativeAcksTracker_.nackedMessages_.empty());

    client.close();
}

TEST(ConsumerTest, testAckNotPersistentTopic) {
    Client client(lookupUrl);
    auto topicName = "non-persistent://public/default/testAckNotPersistentTopic";

    Consumer consumer;
    client.subscribe(topicName, "test-sub", consumer);

    Producer producer;
    client.createProducer(topicName, producer);

    for (int i = 0; i < 10; ++i) {
        producer.send(MessageBuilder().setContent(std::to_string(i)).build());
    }

    Message msg;
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg));
    }

    client.close();
}

INSTANTIATE_TEST_CASE_P(Pulsar, ConsumerSeekTest, ::testing::Values(true, false));

class InterceptorForNegAckDeadlock : public ConsumerInterceptor {
   public:
    Message beforeConsume(const Consumer& consumer, const Message& message) override { return message; }

    void onAcknowledge(const Consumer& consumer, Result result, const MessageId& messageID) override {}

    void onAcknowledgeCumulative(const Consumer& consumer, Result result,
                                 const MessageId& messageID) override {}

    void onNegativeAcksSend(const Consumer& consumer, const std::set<MessageId>& messageIds) override {
        duringNegativeAck_ = true;
        // Wait for the next time Consumer::negativeAcknowledge is called
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::lock_guard<std::mutex> lock{mutex_};
        LOG_INFO("onNegativeAcksSend is called for " << consumer.getTopic());
        duringNegativeAck_ = false;
    }

    static std::mutex mutex_;
    static std::atomic_bool duringNegativeAck_;
};

std::mutex InterceptorForNegAckDeadlock::mutex_;
std::atomic_bool InterceptorForNegAckDeadlock::duringNegativeAck_{false};

// For https://github.com/apache/pulsar-client-cpp/issues/265
TEST(ConsumerTest, testNegativeAckDeadlock) {
    const std::string topic = "test-negative-ack-deadlock";
    Client client{lookupUrl};
    ConsumerConfiguration conf;
    conf.setNegativeAckRedeliveryDelayMs(500);
    conf.intercept({std::make_shared<InterceptorForNegAckDeadlock>()});
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", conf, consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    producer.send(MessageBuilder().setContent("msg").build());

    Message msg;
    ASSERT_EQ(ResultOk, consumer.receive(msg));

    auto& duringNegativeAck = InterceptorForNegAckDeadlock::duringNegativeAck_;
    duringNegativeAck = false;
    consumer.negativeAcknowledge(msg);  // schedule the negative ack timer
    // Wait until the negative ack timer is triggered and onNegativeAcksSend will be called
    for (int i = 0; !duringNegativeAck && i < 100; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(duringNegativeAck);

    {
        std::lock_guard<std::mutex> lock{InterceptorForNegAckDeadlock::mutex_};
        consumer.negativeAcknowledge(msg);
    }
    for (int i = 0; duringNegativeAck && i < 100; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_FALSE(duringNegativeAck);

    client.close();
}

}  // namespace pulsar

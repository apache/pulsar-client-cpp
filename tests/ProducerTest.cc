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

#include <thread>

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "lib/Future.h"
#include "lib/Latch.h"
#include "lib/LogUtils.h"
#include "lib/ProducerImpl.h"
#include "lib/Utils.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

// See the `maxMessageSize` config in test-conf/standalone-ssl.conf
static constexpr size_t maxMessageSize = 1024000;

TEST(ProducerTest, producerNotInitialized) {
    Producer producer;

    Message msg = MessageBuilder().setContent("test").build();

    ASSERT_EQ(ResultProducerNotInitialized, producer.send(msg));

    Promise<Result, MessageId> promise;
    producer.sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    MessageId mi;
    ASSERT_EQ(ResultProducerNotInitialized, promise.getFuture().get(mi));

    ASSERT_EQ(ResultProducerNotInitialized, producer.close());

    Promise<bool, Result> promiseClose;
    producer.closeAsync(WaitForCallback(promiseClose));

    Result result;
    promiseClose.getFuture().get(result);
    ASSERT_EQ(ResultProducerNotInitialized, result);

    ASSERT_TRUE(producer.getTopic().empty());
}

TEST(ProducerTest, exactlyOnceWithProducerNameSpecified) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/exactlyOnceWithProducerNameSpecified";

    Producer producer1;
    ProducerConfiguration producerConfiguration1;
    producerConfiguration1.setProducerName("p-name-1");

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration1, producer1));

    Producer producer2;
    ProducerConfiguration producerConfiguration2;
    producerConfiguration2.setProducerName("p-name-2");
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration2, producer2));

    Producer producer3;
    Result result = client.createProducer(topicName, producerConfiguration2, producer3);
    ASSERT_EQ(ResultProducerBusy, result);
}

TEST(ProducerTest, testSynchronouslySend) {
    Client client(serviceUrl);
    const std::string topic = "ProducerTestSynchronouslySend";

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub-name", consumer));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    MessageId messageId;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("hello").build(), messageId));
    LOG_INFO("Send message to " << messageId);

    Message receivedMessage;
    ASSERT_EQ(ResultOk, consumer.receive(receivedMessage, 3000));
    LOG_INFO("Received message from " << receivedMessage.getMessageId());
    ASSERT_EQ(receivedMessage.getMessageId(), messageId);
    ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMessage));

    client.close();
}

TEST(ProducerTest, testIsConnected) {
    Client client(serviceUrl);
    const std::string nonPartitionedTopic =
        "testProducerIsConnectedNonPartitioned-" + std::to_string(time(nullptr));
    const std::string partitionedTopic =
        "testProducerIsConnectedPartitioned-" + std::to_string(time(nullptr));

    Producer producer;
    ASSERT_FALSE(producer.isConnected());
    // ProducerImpl
    ASSERT_EQ(ResultOk, client.createProducer(nonPartitionedTopic, producer));
    ASSERT_TRUE(producer.isConnected());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_FALSE(producer.isConnected());

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    // PartitionedProducerImpl
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producer));
    ASSERT_TRUE(producer.isConnected());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_FALSE(producer.isConnected());

    client.close();
}

TEST(ProducerTest, testSendAsyncAfterCloseAsyncWithLazyProducers) {
    Client client(serviceUrl);
    const std::string partitionedTopic =
        "testProducerIsConnectedPartitioned-" + std::to_string(time(nullptr));

    int res = makePutRequest(
        adminUrl + "admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "10");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setLazyStartPartitionedProducers(true);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopic, producerConfiguration, producer));

    Message msg = MessageBuilder().setContent("test").build();

    Promise<bool, Result> promiseClose;
    producer.closeAsync(WaitForCallback(promiseClose));

    Promise<Result, MessageId> promise;
    producer.sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    MessageId mi;
    ASSERT_EQ(ResultAlreadyClosed, promise.getFuture().get(mi));

    Result result;
    promiseClose.getFuture().get(result);
    ASSERT_EQ(ResultOk, result);
}

TEST(ProducerTest, testGetNumOfChunks) {
    ASSERT_EQ(ProducerImpl::getNumOfChunks(11, 5), 3);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(10, 5), 2);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(8, 5), 2);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(4, 5), 1);
    ASSERT_EQ(ProducerImpl::getNumOfChunks(1, 0), 1);
}

TEST(ProducerTest, testBacklogQuotasExceeded) {
    std::string ns = "public/test-backlog-quotas";
    std::string topic = ns + "/testBacklogQuotasExceeded" + std::to_string(time(nullptr));

    int res = makePutRequest(adminUrl + "admin/v2/persistent/" + topic + "/partitions", "5");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    LOG_INFO("Created topic " << topic << " with 5 partitions");

    auto setBacklogPolicy = [&ns](const std::string& policy, int limitSize) {
        const auto body = R"({"policy":")" + policy + R"(","limitSize":)" + std::to_string(limitSize) + "}";
        int res = makePostRequest(adminUrl + "admin/v2/namespaces/" + ns + "/backlogQuota", body);
        LOG_INFO(res << " | Change the backlog policy to: " << body);
        ASSERT_TRUE(res == 204 || res == 409);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    };

    Client client(serviceUrl);

    // Create a topic with backlog size that is greater than 1024
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumer));  // create a cursor
    Producer producer;

    const auto partition = topic + "-partition-0";
    ASSERT_EQ(ResultOk, client.createProducer(partition, producer));
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(std::string(1024L, 'a')).build()));
    ASSERT_EQ(ResultOk, producer.close());

    setBacklogPolicy("producer_request_hold", 1024);
    ASSERT_EQ(ResultProducerBlockedQuotaExceededError, client.createProducer(topic, producer));
    ASSERT_EQ(ResultProducerBlockedQuotaExceededError, client.createProducer(partition, producer));

    setBacklogPolicy("producer_exception", 1024);
    ASSERT_EQ(ResultProducerBlockedQuotaExceededException, client.createProducer(topic, producer));
    ASSERT_EQ(ResultProducerBlockedQuotaExceededException, client.createProducer(partition, producer));

    setBacklogPolicy("consumer_backlog_eviction", 1024);
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    ASSERT_EQ(ResultOk, client.createProducer(partition, producer));

    client.close();
}

class ProducerTest : public ::testing::TestWithParam<bool> {};

TEST_P(ProducerTest, testMaxMessageSize) {
    Client client(serviceUrl);

    const auto topic = std::string("ProducerTest-NoBatchMaxMessageSize-") +
                       (GetParam() ? "batch-" : "-no-batch-") + std::to_string(time(nullptr));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumer));

    Producer producer;
    ProducerConfiguration conf;
    conf.setBatchingEnabled(GetParam());
    ASSERT_EQ(ResultOk, client.createProducer(topic, conf, producer));

    std::string msg = std::string(maxMessageSize / 2, 'a');
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(msg).build()));
    Message message;
    ASSERT_EQ(ResultOk, consumer.receive(message));
    ASSERT_EQ(msg, message.getDataAsString());

    std::string orderKey = std::string(maxMessageSize, 'a');
    ASSERT_EQ(ResultMessageTooBig, producer.send(MessageBuilder().setOrderingKey(orderKey).build()));

    ASSERT_EQ(ResultMessageTooBig,
              producer.send(MessageBuilder().setContent(std::string(maxMessageSize, 'b')).build()));

    client.close();
}

TEST(ProducerTest, testChunkingMaxMessageSize) {
    Client client(serviceUrl);

    const auto topic = std::string("ProducerTest-ChunkingMaxMessageSize-") + std::to_string(time(nullptr));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumer));

    Producer producer;
    ProducerConfiguration conf;
    conf.setBatchingEnabled(false);
    conf.setChunkingEnabled(true);
    ASSERT_EQ(ResultOk, client.createProducer(topic, conf, producer));

    std::string orderKey = std::string(maxMessageSize, 'a');
    ASSERT_EQ(ResultMessageTooBig, producer.send(MessageBuilder().setOrderingKey(orderKey).build()));

    std::string msg = std::string(2 * maxMessageSize + 10, 'b');
    Message message;
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(msg).build()));
    ASSERT_EQ(ResultOk, consumer.receive(message));
    ASSERT_EQ(msg, message.getDataAsString());
    ASSERT_LE(1L, message.getMessageId().entryId());

    client.close();
}

TEST(ProducerTest, testExclusiveProducer) {
    Client client(serviceUrl);

    std::string topicName =
        "persistent://public/default/testExclusiveProducer" + std::to_string(time(nullptr));

    Producer producer1;
    ProducerConfiguration producerConfiguration1;
    producerConfiguration1.setProducerName("p-name-1");
    producerConfiguration1.setAccessMode(ProducerConfiguration::Exclusive);

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration1, producer1));

    Producer producer2;
    ProducerConfiguration producerConfiguration2;
    producerConfiguration2.setProducerName("p-name-2");
    producerConfiguration2.setAccessMode(ProducerConfiguration::Exclusive);
    ASSERT_EQ(ResultProducerFenced, client.createProducer(topicName, producerConfiguration2, producer2));

    Producer producer3;
    ProducerConfiguration producerConfiguration3;
    producerConfiguration3.setProducerName("p-name-3");
    ASSERT_EQ(ResultProducerBusy, client.createProducer(topicName, producerConfiguration3, producer3));
}

TEST(ProducerTest, testWaitForExclusiveProducer) {
    Client client(serviceUrl);

    std::string topicName =
        "persistent://public/default/testWaitForExclusiveProducer" + std::to_string(time(nullptr));

    Producer producer1;
    ProducerConfiguration producerConfiguration1;
    producerConfiguration1.setProducerName("p-name-1");
    producerConfiguration1.setAccessMode(ProducerConfiguration::Exclusive);

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration1, producer1));

    ASSERT_EQ(ResultOk, producer1.send(MessageBuilder().setContent("content").build()));

    Producer producer2;
    ProducerConfiguration producerConfiguration2;
    producerConfiguration2.setProducerName("p-name-2");
    producerConfiguration2.setAccessMode(ProducerConfiguration::WaitForExclusive);

    Latch latch(1);
    client.createProducerAsync(topicName, producerConfiguration2,
                               [&latch, &producer2](Result res, Producer producer) {
                                   ASSERT_EQ(ResultOk, res);
                                   producer2 = producer;
                                   latch.countdown();
                               });

    // when p1 close, p2 success created.
    producer1.close();
    latch.wait();
    ASSERT_EQ(ResultOk, producer2.send(MessageBuilder().setContent("content").build()));

    producer2.close();
}

TEST(ProducerTest, testExclusiveWithFencingProducer) {
    Client client(serviceUrl);

    std::string topicName =
        "persistent://public/default/testExclusiveWithFencingProducer" + std::to_string(time(nullptr));

    Producer producer1;
    ProducerConfiguration producerConfiguration1;
    producerConfiguration1.setProducerName("p-name-1");
    producerConfiguration1.setAccessMode(ProducerConfiguration::Exclusive);

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration1, producer1));
    producer1.send(MessageBuilder().setContent("content").build());

    Producer producer2;
    ProducerConfiguration producerConfiguration2;
    producerConfiguration2.setProducerName("p-name-2");
    producerConfiguration2.setAccessMode(ProducerConfiguration::WaitForExclusive);

    Latch latch(1);
    client.createProducerAsync(topicName, producerConfiguration2,
                               [&latch, &producer2](Result res, Producer producer) {
                                   // producer2 will be fenced
                                   ASSERT_EQ(ResultProducerFenced, res);
                                   latch.countdown();
                                   producer2 = producer;
                               });
    // wait for all the Producers to be enqueued in order to prevent races
    sleep(1);

    // producer3 will create success.
    Producer producer3;
    ProducerConfiguration producerConfiguration3;
    producerConfiguration3.setProducerName("p-name-3");
    producerConfiguration3.setAccessMode(ProducerConfiguration::ExclusiveWithFencing);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration3, producer3));
    ASSERT_EQ(ResultOk, producer3.send(MessageBuilder().setContent("content").build()));

    latch.wait();
    // producer1 will be fenced
    ASSERT_EQ(ResultProducerFenced, producer1.send(MessageBuilder().setContent("content").build()));

    // Again create producer4 with WaitForExclusive
    Producer producer4;
    ProducerConfiguration producerConfiguration4;
    producerConfiguration2.setProducerName("p-name-4");
    producerConfiguration2.setAccessMode(ProducerConfiguration::WaitForExclusive);
    Latch latch2(1);
    client.createProducerAsync(topicName, producerConfiguration2,
                               [&latch2, &producer4](Result res, Producer producer) {
                                   // producer4 will be success
                                   ASSERT_EQ(ResultOk, res);
                                   producer4 = producer;
                                   latch2.countdown();
                               });
    ASSERT_EQ(ResultProducerNotInitialized, producer4.send(MessageBuilder().setContent("content").build()));

    // When producer3 is close, producer4 will be create success
    producer3.close();
    latch2.wait();
    ASSERT_EQ(ResultOk, producer4.send(MessageBuilder().setContent("content").build()));

    client.close();
}

TEST_P(ProducerTest, testFlushNoBatch) {
    Client client(serviceUrl);

    auto partitioned = GetParam();
    const auto topicName = std::string("testFlushNoBatch") +
                           (partitioned ? "partitioned-" : "-no-partitioned-") +
                           std::to_string(time(nullptr));

    if (partitioned) {
        // call admin api to make it partitioned
        std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
        int res = makePutRequest(url, "5");
        LOG_INFO("res = " << res);
        ASSERT_FALSE(res != 204 && res != 409);
    }

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setBatchingEnabled(false);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration, producer));

    std::atomic_int needCallBack(100);
    auto cb = [&needCallBack](Result code, const MessageId& msgId) {
        ASSERT_EQ(code, ResultOk);
        needCallBack.fetch_sub(1);
    };

    for (int i = 0; i < 100; ++i) {
        Message msg = MessageBuilder().setContent("content").build();
        producer.sendAsync(msg, cb);
    }

    producer.flush();
    ASSERT_EQ(needCallBack.load(), 0);
    producer.close();

    client.close();
}

TEST_P(ProducerTest, testFlushBatch) {
    Client client(serviceUrl);

    auto partitioned = GetParam();
    const auto topicName = std::string("testFlushNoBatch") +
                           (partitioned ? "partitioned-" : "-no-partitioned-") +
                           std::to_string(time(nullptr));

    if (partitioned) {
        // call admin api to make it partitioned
        std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
        int res = makePutRequest(url, "5");
        LOG_INFO("res = " << res);
        ASSERT_FALSE(res != 204 && res != 409);
    }

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setBatchingEnabled(true);
    producerConfiguration.setBatchingMaxMessages(10);
    producerConfiguration.setBatchingMaxPublishDelayMs(1000);
    producerConfiguration.setBatchingMaxAllowedSizeInBytes(4 * 1024 * 1024);

    // test all messages in batch has been sent
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration, producer));

    std::atomic_int needCallBack(100);
    auto cb = [&needCallBack](Result code, const MessageId& msgId) {
        ASSERT_EQ(code, ResultOk);
        needCallBack.fetch_sub(1);
    };

    for (int i = 0; i < 100; ++i) {
        Message msg = MessageBuilder().setContent("content").build();
        producer.sendAsync(msg, cb);
    }

    producer.flush();
    ASSERT_EQ(needCallBack.load(), 0);
    producer.close();

    // test remain messages in batch not send
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration, producer));

    std::atomic_int needCallBack2(105);
    auto cb2 = [&needCallBack2](Result code, const MessageId& msgId) {
        ASSERT_EQ(code, ResultOk);
        needCallBack2.fetch_sub(1);
    };

    for (int i = 0; i < 105; ++i) {
        Message msg = MessageBuilder().setContent("content").build();
        producer.sendAsync(msg, cb2);
    }

    producer.flush();
    ASSERT_EQ(needCallBack2.load(), 0);
    producer.close();

    client.close();
}

TEST(ProducerTest, testCloseSubProducerWhenFail) {
    Client client(serviceUrl);

    std::string ns = "test-close-sub-producer-when-fail";
    std::string localName = std::string("testCloseSubProducerWhenFail") + std::to_string(time(nullptr));
    std::string topicName = "persistent://public/" + ns + '/' + localName;
    const int maxProducersPerTopic = 10;
    const int partitionNum = 5;

    // call admin api to create namespace with max prodcuer limit
    std::string url = adminUrl + "admin/v2/namespaces/public/" + ns;
    int res =
        makePutRequest(url, "{\"max_producers_per_topic\": " + std::to_string(maxProducersPerTopic) + "}");
    ASSERT_TRUE(res == 204 || res == 409) << "res:" << res;

    // call admin api to create partitioned topic
    res = makePutRequest(adminUrl + "admin/v2/persistent/public/" + ns + "/" + localName + "/partitions",
                         std::to_string(partitionNum));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setBatchingEnabled(false);

    // create producers for partition-0 up to max producer limit
    std::vector<Producer> producers;
    for (int i = 0; i < maxProducersPerTopic; ++i) {
        Producer producer;
        ASSERT_EQ(ResultOk,
                  client.createProducer(topicName + "-partition-0", producerConfiguration, producer));
        producers.push_back(producer);
    }

    // create partitioned producer, should fail because partition-0 already reach max producer limit
    for (int i = 0; i < maxProducersPerTopic; ++i) {
        Producer producer;
        ASSERT_EQ(ResultProducerBusy, client.createProducer(topicName, producer));
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // create producer for partition-1, should succeed
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName + "-partition-1", producerConfiguration, producer));
    producers.push_back(producer);

    for (auto& producer : producers) {
        producer.close();
    }
    client.close();
}

TEST(ProducerTest, testCloseProducerBeforeCreated) {
    Client client(serviceUrl);

    std::string ns = "test-close-producer-before-created";
    std::string localName = std::string("testCloseProducerBeforeCreated") + std::to_string(time(nullptr));
    std::string topicName = "persistent://public/" + ns + '/' + localName;
    const int maxProducersPerTopic = 10;
    const int partitionNum = 5;

    // call admin api to create namespace with max prodcuer limit
    std::string url = adminUrl + "admin/v2/namespaces/public/" + ns;
    int res =
        makePutRequest(url, "{\"max_producers_per_topic\": " + std::to_string(maxProducersPerTopic) + "}");
    ASSERT_TRUE(res == 204 || res == 409) << "res:" << res;

    // call admin api to create partitioned topic
    res = makePutRequest(adminUrl + "admin/v2/persistent/public/" + ns + "/" + localName + "/partitions",
                         std::to_string(partitionNum));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setLazyStartPartitionedProducers(true);
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    producerConfiguration.setBatchingEnabled(false);

    Message msg = MessageBuilder().setContent("test").build();
    for (int i = 0; i < maxProducersPerTopic * 100; ++i) {
        Producer producer;
        ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration, producer));
        // trigger lazy producer creation
        for (int j = 0; j < partitionNum; ++j) {
            producer.sendAsync(msg, [](pulsar::Result, const pulsar::MessageId&) {});
        }
        producer.close();
    }

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, {}, producer));
    producer.close();

    client.close();
}

TEST(ProducerTest, testNoDeadlockWhenClosingPartitionedProducerAfterPartitionsUpdate) {
    const std::string topic = "public/default/partition-test" + std::to_string(time(nullptr));
    std::string topicOperateUrl = adminUrl + "admin/v2/persistent/" + topic + "/partitions";

    int res = makePutRequest(topicOperateUrl, "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    ClientConfiguration clientConf;
    clientConf.setPartititionsUpdateInterval(1);
    Client client(serviceUrl, clientConf);
    ProducerConfiguration conf;
    Producer producer;
    client.createProducer(topic, conf, producer);
    PartitionedProducerImpl& partitionedProducer = PulsarFriend::getPartitionedProducerImpl(producer);

    // TODO: Replace by producer interceptor to reproduce the issue then we can remove
    // PulsarFriend::updatePartitions
    PulsarFriend::updatePartitions(partitionedProducer, 3);

    producer.close();
    client.close();
}

TEST(ProducerTest, testReconnectMultiConnectionsPerBroker) {
    ClientConfiguration conf;
    conf.setConnectionsPerBroker(10);

    Client client(serviceUrl, conf);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer("producer-test-reconnect-twice", producer));

    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(PulsarFriend::reconnect(producer)) << "i: " << i;
    }

    client.close();
}

INSTANTIATE_TEST_CASE_P(Pulsar, ProducerTest, ::testing::Values(true, false));

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
// Run `docker-compose up -d` to set up the test environment for this test.
#include <gtest/gtest.h>

#include <thread>
#include <unordered_set>

#include "include/pulsar/Client.h"
#include "lib/LogUtils.h"
#include "lib/Semaphore.h"
#include "lib/TimeUtils.h"
#include "tests/HttpHelper.h"
#include "tests/PulsarFriend.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

bool checkTime() {
    const static auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return duration < 300 * 1000;
}

TEST(ExtensibleLoadManagerTest, testPubSubWhileUnloading) {
    constexpr auto maxWaitTime = std::chrono::seconds(5);
    constexpr long maxWaitTimeMs = maxWaitTime.count() * 1000L;

    const static std::string blueAdminUrl = "http://localhost:8080/";
    const static std::string greenAdminUrl = "http://localhost:8081/";
    const static std::string topicNameSuffix = std::to_string(time(NULL));
    const static std::string topicName = "persistent://public/unload-test/topic-" + topicNameSuffix;

    ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
        std::string url = blueAdminUrl + "admin/v2/clusters/cluster-a/migrate?migrated=false";
        int res = makePostRequest(url, R"(
                    {
                       "serviceUrl": "http://localhost:8081",
                       "serviceUrlTls":"https://localhost:8085",
                       "brokerServiceUrl": "pulsar://localhost:6651",
                       "brokerServiceUrlTls": "pulsar+ssl://localhost:6655"
                    })");
        LOG_INFO("res:" << res);
        return res == 200;
    }));

    ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
        std::string url = blueAdminUrl + "admin/v2/namespaces/public/unload-test?bundles=1";
        int res = makePutRequest(url, "");
        return res == 204 || res == 409;
    }));

    ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
        std::string url = greenAdminUrl + "admin/v2/namespaces/public/unload-test?bundles=1";
        int res = makePutRequest(url, "");
        return res == 204 || res == 409;
    }));

    ClientConfiguration conf;
    conf.setIOThreads(8);
    Client client{"pulsar://localhost:6650", conf};
    Producer producer;
    ProducerConfiguration producerConfiguration;
    Result producerResult = client.createProducer(topicName, producerConfiguration, producer);
    ASSERT_EQ(producerResult, ResultOk);
    Consumer consumer;
    Result consumerResult = client.subscribe(topicName, "sub", consumer);
    ASSERT_EQ(consumerResult, ResultOk);

    Semaphore unloadSemaphore(0);
    Semaphore pubWaitSemaphore(0);
    Semaphore migrationSemaphore(0);

    const int msgCount = 20;
    SynchronizedHashMap<int, int> producedMsgs;
    auto produce = [&]() {
        int i = 0;
        while (i < msgCount && checkTime()) {
            if (i == 3 || i == 8 || i == 17) {
                unloadSemaphore.acquire();
            }

            if (i == 5 || i == 15) {
                pubWaitSemaphore.release();
            }

            if (i == 12) {
                migrationSemaphore.acquire();
            }

            std::string content = std::to_string(i);
            const auto msg = MessageBuilder().setContent(content).build();

            auto start = TimeUtils::currentTimeMillis();
            Result sendResult = producer.send(msg);
            auto elapsed = TimeUtils::currentTimeMillis() - start;
            LOG_INFO("produce i: " << i << " " << elapsed << " ms");
            ASSERT_EQ(sendResult, ResultOk);
            ASSERT_TRUE(elapsed < maxWaitTimeMs);

            producedMsgs.put(i, i);
            i++;
        }
        LOG_INFO("producer finished");
    };
    std::atomic<bool> stopConsumer(false);
    SynchronizedHashMap<int, int> consumedMsgs;
    auto consume = [&]() {
        Message receivedMsg;
        while (checkTime()) {
            if (stopConsumer && producedMsgs.size() == msgCount && consumedMsgs.size() == msgCount) {
                break;
            }
            auto start = TimeUtils::currentTimeMillis();
            Result receiveResult = consumer.receive(receivedMsg, maxWaitTimeMs);
            auto elapsed = TimeUtils::currentTimeMillis() - start;
            int i = std::stoi(receivedMsg.getDataAsString());
            LOG_INFO("receive i: " << i << " " << elapsed << " ms");
            ASSERT_EQ(receiveResult, ResultOk);
            ASSERT_TRUE(elapsed < maxWaitTimeMs);

            start = TimeUtils::currentTimeMillis();
            Result ackResult = consumer.acknowledge(receivedMsg);
            elapsed = TimeUtils::currentTimeMillis() - start;
            LOG_INFO("acked i:" << i << " " << elapsed << " ms");
            ASSERT_TRUE(elapsed < maxWaitTimeMs);
            ASSERT_EQ(ackResult, ResultOk);
            consumedMsgs.put(i, i);
        }
        LOG_INFO("consumer finished");
    };

    std::thread produceThread(produce);
    std::thread consumeThread(consume);

    auto unload = [&](bool migrated) {
        const std::string &adminUrl = migrated ? greenAdminUrl : blueAdminUrl;
        auto clientImplPtr = PulsarFriend::getClientImplPtr(client);
        auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
        auto &producerImpl = PulsarFriend::getProducerImpl(producer);
        uint64_t lookupCountBeforeUnload;
        std::string destinationBroker;
        while (checkTime()) {
            // make sure producers and consumers are ready
            ASSERT_TRUE(waitUntil(maxWaitTime,
                                  [&] { return consumerImpl.isConnected() && producerImpl.isConnected(); }));

            std::string url =
                adminUrl + "lookup/v2/topic/persistent/public/unload-test/topic" + topicNameSuffix;
            std::string responseDataBeforeUnload;
            int res = makeGetRequest(url, responseDataBeforeUnload);
            if (res != 200) {
                continue;
            }
            std::string prefix = migrated ? "green-" : "";
            destinationBroker =
                prefix + (responseDataBeforeUnload.find("broker-2") == std::string::npos ? "broker-2:8080"
                                                                                         : "broker-1:8080");
            lookupCountBeforeUnload = clientImplPtr->getLookupCount();
            ASSERT_TRUE(lookupCountBeforeUnload > 0);

            url = adminUrl +
                  "admin/v2/namespaces/public/unload-test/0x00000000_0xffffffff/unload?destinationBroker=" +
                  destinationBroker;
            LOG_INFO("before lookup responseData:" << responseDataBeforeUnload << ",unload url:" << url
                                                   << ",lookupCountBeforeUnload:" << lookupCountBeforeUnload);
            res = makePutRequest(url, "");
            LOG_INFO("unload res:" << res);
            if (res != 204) {
                continue;
            }

            // make sure producers and consumers are ready
            ASSERT_TRUE(waitUntil(maxWaitTime,
                                  [&] { return consumerImpl.isConnected() && producerImpl.isConnected(); }));
            std::string responseDataAfterUnload;
            ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
                url = adminUrl + "lookup/v2/topic/persistent/public/unload-test/topic" + topicNameSuffix;
                res = makeGetRequest(url, responseDataAfterUnload);
                return res == 200 && responseDataAfterUnload.find(destinationBroker) != std::string::npos;
            }));
            LOG_INFO("after lookup responseData:" << responseDataAfterUnload << ",res:" << res);

            auto lookupCountAfterUnload = clientImplPtr->getLookupCount();
            if (lookupCountBeforeUnload != lookupCountAfterUnload) {
                continue;
            }
            break;
        }
    };
    LOG_INFO("#### starting first unload ####");
    unload(false);
    unloadSemaphore.release();
    pubWaitSemaphore.acquire();
    LOG_INFO("#### starting second unload ####");
    unload(false);
    unloadSemaphore.release();

    LOG_INFO("#### migrating the cluster ####");
    migrationSemaphore.release();
    ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
        std::string url = blueAdminUrl + "admin/v2/clusters/cluster-a/migrate?migrated=true";
        int res = makePostRequest(url, R"({
                                               "serviceUrl": "http://localhost:8081",
                                               "serviceUrlTls":"https://localhost:8085",
                                               "brokerServiceUrl": "pulsar://localhost:6651",
                                               "brokerServiceUrlTls": "pulsar+ssl://localhost:6655"
                                            })");
        LOG_INFO("res:" << res);
        return res == 200;
    }));
    ASSERT_TRUE(waitUntil(maxWaitTime, [&] {
        auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
        auto &producerImpl = PulsarFriend::getProducerImpl(producer);
        auto consumerConnAddress = PulsarFriend::getConnectionPhysicalAddress(consumerImpl);
        auto producerConnAddress = PulsarFriend::getConnectionPhysicalAddress(producerImpl);
        return consumerImpl.isConnected() && producerImpl.isConnected() &&
               consumerConnAddress.find("6651") != std::string::npos &&
               producerConnAddress.find("6651") != std::string::npos;
    }));
    pubWaitSemaphore.acquire();
    LOG_INFO("#### starting third unload after migration ####");
    unload(true);
    unloadSemaphore.release();

    stopConsumer = true;
    consumeThread.join();
    produceThread.join();
    ASSERT_EQ(producedMsgs.size(), msgCount);
    ASSERT_EQ(consumedMsgs.size(), msgCount);
    for (int i = 0; i < msgCount; i++) {
        producedMsgs.remove(i);
        consumedMsgs.remove(i);
    }
    ASSERT_EQ(producedMsgs.size(), 0);
    ASSERT_EQ(consumedMsgs.size(), 0);

    client.close();

    ASSERT_TRUE(checkTime()) << "timed out";
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

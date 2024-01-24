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

#include "include/pulsar/Client.h"
#include "lib/LogUtils.h"
#include "lib/Semaphore.h"
#include "tests/HttpHelper.h"
#include "tests/PulsarFriend.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

bool checkTime() {
    const static auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return duration < 180 * 1000;
}

TEST(ExtensibleLoadManagerTest, testPubSubWhileUnloading) {
    const static std::string adminUrl = "http://localhost:8080/";
    const static std::string topicName =
        "persistent://public/unload-test/topic-1" + std::to_string(time(NULL));

    ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
        std::string url = adminUrl + "admin/v2/namespaces/public/unload-test?bundles=1";
        int res = makePutRequest(url, "");
        return res == 204 || res == 409;
    }));

    Client client{"pulsar://localhost:6650"};
    Producer producer;
    ProducerConfiguration producerConfiguration;
    Result producerResult = client.createProducer(topicName, producerConfiguration, producer);
    ASSERT_EQ(producerResult, ResultOk);
    Consumer consumer;
    Result consumerResult = client.subscribe(topicName, "sub", consumer);
    ASSERT_EQ(consumerResult, ResultOk);

    Semaphore firstUnloadSemaphore(0);
    Semaphore secondUnloadSemaphore(0);
    Semaphore halfPubWaitSemaphore(0);
    const int msgCount = 10;
    int produced = 0;
    auto produce = [&]() {
        int i = 0;
        while (i < msgCount && checkTime()) {
            if (i == 3) {
                firstUnloadSemaphore.acquire();
            }

            if (i == 5) {
                halfPubWaitSemaphore.release();
            }

            if (i == 8) {
                secondUnloadSemaphore.acquire();
            }

            std::string content = std::to_string(i);
            const auto msg = MessageBuilder().setContent(content).build();

            ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
                Result sendResult = producer.send(msg);
                return sendResult == ResultOk;
            }));

            LOG_INFO("produced index:" << i);
            produced++;
            i++;
        }
        LOG_INFO("producer finished");
    };

    int consumed = 0;
    auto consume = [&]() {
        Message receivedMsg;
        int i = 0;
        while (i < msgCount && checkTime()) {
            ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
                Result receiveResult =
                    consumer.receive(receivedMsg, 1000);  // Assumed that we wait 1000 ms for each message
                return receiveResult == ResultOk;
            }));
            LOG_INFO("received index:" << i);

            int id = std::stoi(receivedMsg.getDataAsString());
            if (id < i) {
                continue;
            }
            ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
                Result ackResult = consumer.acknowledge(receivedMsg);
                return ackResult == ResultOk;
            }));
            LOG_INFO("acked index:" << i);

            consumed++;
            i++;
        }
        LOG_INFO("consumer finished");
    };

    std::thread produceThread(produce);
    std::thread consumeThread(consume);

    auto unload = [&] {
        auto clientImplPtr = PulsarFriend::getClientImplPtr(client);
        auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
        auto &producerImpl = PulsarFriend::getProducerImpl(producer);
        uint64_t lookupCountBeforeUnload;
        std::string destinationBroker;
        while (checkTime()) {
            // make sure producers and consumers are ready
            ASSERT_TRUE(waitUntil(std::chrono::seconds(30),
                                  [&] { return consumerImpl.isConnected() && producerImpl.isConnected(); }));

            std::string url = adminUrl + "lookup/v2/topic/persistent/public/unload-test/topic-1";
            std::string responseDataBeforeUnload;
            int res = makeGetRequest(url, responseDataBeforeUnload);
            if (res != 200) {
                continue;
            }
            destinationBroker = responseDataBeforeUnload.find("broker-2") == std::string::npos
                                    ? "broker-2:8080"
                                    : "broker-1:8080";
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
            ASSERT_TRUE(waitUntil(std::chrono::seconds(30),
                                  [&] { return consumerImpl.isConnected() && producerImpl.isConnected(); }));
            std::string responseDataAfterUnload;
            ASSERT_TRUE(waitUntil(std::chrono::seconds(60), [&] {
                url = adminUrl + "lookup/v2/topic/persistent/public/unload-test/topic-1";
                res = makeGetRequest(url, responseDataAfterUnload);
                return res == 200 && responseDataAfterUnload.find(destinationBroker) != std::string::npos;
            }));
            LOG_INFO("after lookup responseData:" << responseDataAfterUnload << ",res:" << res);

            // TODO: check lookup counter after pip-307 is released
            auto lookupCountAfterUnload = clientImplPtr->getLookupCount();
            ASSERT_TRUE(lookupCountBeforeUnload < lookupCountAfterUnload);
            break;
        }
    };
    LOG_INFO("starting first unload");
    unload();
    firstUnloadSemaphore.release();
    halfPubWaitSemaphore.acquire();
    LOG_INFO("starting second unload");
    unload();
    secondUnloadSemaphore.release();

    produceThread.join();
    consumeThread.join();
    ASSERT_EQ(consumed, msgCount);
    ASSERT_EQ(produced, msgCount);
    ASSERT_TRUE(checkTime()) << "timed out";
    client.close();
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

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
#include "lib/Semaphore.h"
#include "tests/HttpHelper.h"
#include "tests/PulsarFriend.h"

using namespace pulsar;

bool checkTime() {
    const static auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return duration < 180 * 1000;
}

TEST(ExtensibleLoadManagerTest, testConsumeSuccess) {
    const static std::string adminUrl = "http://localhost:8080/";
    const static std::string topicName =
        "persistent://public/unload-test/topic-1" + std::to_string(time(NULL));
    while (checkTime()) {
        std::string url = adminUrl + "admin/v2/namespaces/public/unload-test?bundles=1";
        int res = makePutRequest(url, "");
        if (res != 204 && res != 409) {
            continue;
        }
        break;
    }

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
    auto produce = [&producer, &produced, &firstUnloadSemaphore, &secondUnloadSemaphore,
                    &halfPubWaitSemaphore]() {
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
            while (checkTime()) {
                Result sendResult = producer.send(msg);
                if (sendResult != ResultOk) {
                    continue;
                }
                break;
            }
            std::cout << "produced index:" << i << std::endl;
            produced++;
            i++;
        }
        std::cout << "producer finished" << std::endl;
    };

    int consumed = 0;
    auto consume = [&consumer, &consumed]() {
        Message receivedMsg;
        int i = 0;
        while (i < msgCount && checkTime()) {
            while (checkTime()) {
                Result receiveResult =
                    consumer.receive(receivedMsg, 1000);  // Assumed that we wait 1000 ms for each message
                if (receiveResult != ResultOk) {
                    continue;
                }
                std::cout << "received index:" << i << std::endl;
                break;
            }
            int id = std::stoi(receivedMsg.getDataAsString());
            if (id < i) {
                continue;
            }
            while (checkTime()) {
                Result ackResult = consumer.acknowledge(receivedMsg);
                if (ackResult != ResultOk) {
                    continue;
                }
                std::cout << "acked index:" << i << std::endl;
                break;
            }
            consumed++;
            i++;
        }
        std::cout << "consumer finished" << std::endl;
    };

    std::thread produceThread(produce);
    std::thread consumeThread(consume);

    auto unload = [&client, &consumer, &producer] {
        auto clientImplPtr = PulsarFriend::getClientImplPtr(client);
        auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
        auto &producerImpl = PulsarFriend::getProducerImpl(producer);
        uint64_t lookupCountBeforeUnload;
        std::string destinationBroker;
        while (checkTime()) {
            // make sure producers and consumers are ready
            while (checkTime() && !consumerImpl.isConnected() && !producerImpl.isConnected()) {
                sleep(1);
            }

            std::string url = adminUrl + "lookup/v2/topic/persistent/public/unload-test/topic-1";
            std::string responseData;
            int res = makeGetRequest(url, responseData);
            if (res != 200) {
                continue;
            }
            destinationBroker =
                responseData.find("broker-2") == std::string::npos ? "broker-2:8080" : "broker-1:8080";
            lookupCountBeforeUnload = clientImplPtr->getLookupCount();
            ASSERT_TRUE(lookupCountBeforeUnload > 0);
            url = adminUrl +
                  "admin/v2/namespaces/public/unload-test/0x00000000_0xffffffff/unload?destinationBroker=" +
                  destinationBroker;
            std::cout << "before lookup responseData:" << responseData << ",unload url:" << url
                      << ",lookupCountBeforeUnload:" << lookupCountBeforeUnload << std::endl;
            res = makePutRequest(url, "");
            std::cout << "unload res:" << res << std::endl;
            if (res != 204) {
                continue;
            }

            // make sure producers and consumers are ready
            while (checkTime() && !consumerImpl.isConnected() && !producerImpl.isConnected()) {
                sleep(1);
            }

            url = adminUrl + "lookup/v2/topic/persistent/public/unload-test/topic-1";
            res = makeGetRequest(url, responseData);
            std::cout << "after lookup responseData:" << responseData << ",url:" << url << ",res:" << res
                      << std::endl;
            if (res != 200) {
                continue;
            }

            ASSERT_TRUE(responseData.find(destinationBroker) != std::string::npos);

            // TODO: check lookup counter after pip-307 is released
            auto lookupCountAfterUnload = clientImplPtr->getLookupCount();
            if (lookupCountBeforeUnload < lookupCountAfterUnload) {
                continue;
            }
            break;
        }
    };
    std::cout << "starting first unload" << std::endl;
    unload();
    firstUnloadSemaphore.release();
    halfPubWaitSemaphore.acquire();
    std::cout << "starting second unload" << std::endl;
    unload();
    secondUnloadSemaphore.release();

    produceThread.join();
    consumeThread.join();
    ASSERT_EQ(consumed, msgCount);
    ASSERT_EQ(produced, msgCount);
    if (!checkTime()) {
        FAIL() << "timed out";
    }
    client.close();
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

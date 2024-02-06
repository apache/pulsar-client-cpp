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
#include <pulsar/Client.h>

#include "lib/Latch.h"

using namespace pulsar;

TEST(BrokerMetadataTest, testConsumeSuccess) {
    Client client{"pulsar://localhost:6650"};
    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setBatchingEnabled(false);
    Result producerResult =
        client.createProducer("persistent://public/default/topic-non-batch", producerConfiguration, producer);
    ASSERT_EQ(producerResult, ResultOk);
    Consumer consumer;
    Result consumerResult = client.subscribe("persistent://public/default/topic-non-batch", "sub", consumer);
    ASSERT_EQ(consumerResult, ResultOk);
    for (int i = 0; i < 10; i++) {
        std::string content = "testConsumeSuccess" + std::to_string(i);
        const auto msg = MessageBuilder().setContent(content).build();
        Result sendResult = producer.send(msg);
        ASSERT_EQ(sendResult, ResultOk);
    }

    Message receivedMsg;
    for (int i = 0; i < 10; i++) {
        Result receiveResult =
            consumer.receive(receivedMsg, 1000);  // Assumed that we wait 1000 ms for each message
        printf("receive index: %d\n", i);
        ASSERT_EQ(receiveResult, ResultOk);
        ASSERT_EQ(receivedMsg.getDataAsString(), "testConsumeSuccess" + std::to_string(i));
        ASSERT_EQ(receivedMsg.getIndex(), i);
        Result ackResult = consumer.acknowledge(receivedMsg);
        ASSERT_EQ(ackResult, ResultOk);
    }
    client.close();
}

TEST(BrokerMetadataTest, testConsumeBatchSuccess) {
    Client client{"pulsar://localhost:6650"};
    Producer producer;
    Result producerResult = client.createProducer("persistent://public/default/topic-batch", producer);
    ASSERT_EQ(producerResult, ResultOk);
    Consumer consumer;
    Result consumerResult = client.subscribe("persistent://public/default/topic-batch", "sub", consumer);
    ASSERT_EQ(consumerResult, ResultOk);

    Latch latch(10);
    auto sendCallback = [&latch](Result result, const MessageId& id) {
        ASSERT_EQ(result, ResultOk);
        latch.countdown();
    };

    for (int i = 0; i < 10; i++) {
        std::string content = "testConsumeSuccess" + std::to_string(i);
        const auto msg = MessageBuilder().setContent(content).build();
        producer.sendAsync(msg, sendCallback);
    }

    latch.wait();

    Message receivedMsg;
    for (int i = 0; i < 10; i++) {
        Result receiveResult =
            consumer.receive(receivedMsg, 1000);  // Assumed that we wait 1000 ms for each message
        ASSERT_EQ(receiveResult, ResultOk);
        ASSERT_EQ(receivedMsg.getDataAsString(), "testConsumeSuccess" + std::to_string(i));
        ASSERT_EQ(receivedMsg.getIndex(), i);
    }
    client.close();
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

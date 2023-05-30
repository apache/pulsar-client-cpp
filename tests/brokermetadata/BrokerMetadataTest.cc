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

using namespace pulsar;

TEST(BrokerMetadataTest, testConsumeSuccess) {
    Client client{"pulsar://localhost:6650"};
    Producer producer;
    Result producerResult = client.createProducer("persistent://public/default/testConsumeSuccess", producer);
    ASSERT_EQ(producerResult, ResultOk);
    Consumer consumer;
    Result consumerResult =
        client.subscribe("persistent://public/default/testConsumeSuccess", "testConsumeSuccess", consumer);
    ASSERT_EQ(consumerResult, ResultOk);
    const auto msg = MessageBuilder().setContent("testConsumeSuccess").build();
    Result sendResult = producer.send(msg);
    ASSERT_EQ(sendResult, ResultOk);
    Message receivedMsg;
    Result receiveResult = consumer.receive(receivedMsg);
    ASSERT_EQ(receiveResult, ResultOk);
    ASSERT_EQ(receivedMsg.getDataAsString(), "testConsumeSuccess");
    client.close();
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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
#include <atomic>
#include <ctime>
#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include "lib/ClientImpl.h"
#include "HttpHelper.h"
#include "PulsarFriend.h"

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";

enum class EndToEndType : uint8_t
{
    SINGLE_TOPIC,
    MULTI_TOPICS,
    REGEX_TOPICS
};

class ShutdownTest : public ::testing::TestWithParam<EndToEndType> {
   public:
    void SetUp() override {
        topic_ = topic_ + std::to_string(id_++) + "-" + std::to_string(time(nullptr));
        if (GetParam() != EndToEndType::SINGLE_TOPIC) {
            int res = makePutRequest(
                "http://localhost:8080/admin/v2/persistent/public/default/" + topic_ + "/partitions", "2");
            ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
        }
    }

   protected:
    Client client_{lookupUrl};
    decltype(PulsarFriend::getProducers(client_)) producers_{PulsarFriend::getProducers(client_)};
    decltype(PulsarFriend::getConsumers(client_)) consumers_{PulsarFriend::getConsumers(client_)};
    std::string topic_ = "shutdown-test-";

    static std::atomic_int id_;

    Result subscribe(Consumer &consumer) {
        if (GetParam() == EndToEndType::REGEX_TOPICS) {
            // NOTE: Currently the regex subscription requires the complete namespace prefix
            return client_.subscribeWithRegex("persistent://public/default/" + topic_ + ".*", "sub",
                                              consumer);
        } else {
            return client_.subscribe(topic_, "sub", consumer);
        }
    }

    void assertConnectionsEmpty() {
        auto connections = PulsarFriend::getConnections(client_);
        for (const auto &cnx : PulsarFriend::getConnections(client_)) {
            EXPECT_TRUE(PulsarFriend::getProducers(*cnx).empty());
            EXPECT_TRUE(PulsarFriend::getConsumers(*cnx).empty());
        }
    }
};

std::atomic_int ShutdownTest::id_{0};

TEST_P(ShutdownTest, testClose) {
    Producer producer;
    ASSERT_EQ(ResultOk, client_.createProducer(topic_, producer));
    EXPECT_EQ(producers_.size(), 1);
    ASSERT_EQ(ResultOk, producer.close());
    EXPECT_EQ(producers_.size(), 0);

    Consumer consumer;
    ASSERT_EQ(ResultOk, subscribe(consumer));
    EXPECT_EQ(consumers_.size(), 1);
    ASSERT_EQ(ResultOk, consumer.close());
    EXPECT_EQ(consumers_.size(), 0);

    ASSERT_EQ(ResultOk, subscribe(consumer));
    EXPECT_EQ(consumers_.size(), 1);
    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    EXPECT_EQ(consumers_.size(), 0);

    assertConnectionsEmpty();
    ASSERT_EQ(ResultOk, client_.close());
}

TEST_P(ShutdownTest, testDestructor) {
    {
        Producer producer;
        ASSERT_EQ(ResultOk, client_.createProducer(topic_, producer));
        EXPECT_EQ(producers_.size(), 1);
    }
    EXPECT_EQ(producers_.size(), 0);

    {
        Consumer consumer;
        ASSERT_EQ(ResultOk, subscribe(consumer));
        EXPECT_EQ(consumers_.size(), 1);
    }
    EXPECT_EQ(consumers_.size(), 0);

    assertConnectionsEmpty();
    client_.close();
}

INSTANTIATE_TEST_SUITE_P(Pulsar, ShutdownTest,
                         ::testing::Values(EndToEndType::SINGLE_TOPIC, EndToEndType::MULTI_TOPICS,
                                           EndToEndType::REGEX_TOPICS));

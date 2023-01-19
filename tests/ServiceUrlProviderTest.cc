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

using namespace pulsar;

static const std::string lookupUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

class ServiceUrlProviderTest : public ::testing::TestWithParam<std::string> {
   public:
    void SetUp() override { serviceUrl = GetParam(); }

    std::string serviceUrl;
};

TEST(ServiceUrlProviderTest, testClientClose) {
    const std::string topicName = "testClientClose-" + std::to_string(time(nullptr));
    Client client([]() -> const std::string& { return lookupUrl; });
    client.close();
    ASSERT_EQ(ResultAlreadyClosed, client.updateServiceUrl(lookupUrl));
    std::map<std::string, std::string> testMap;
}

TEST_P(ServiceUrlProviderTest, testBasicUpdateUrl) {
    const std::string topicName = "basicUpdateUrl-" + std::to_string(time(nullptr));
    Client client([this]() -> const std::string& { return serviceUrl; });

    Producer producer1;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer1));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, "test-sub", consumer));

    // Update service url.
    ASSERT_EQ(ResultOk, client.updateServiceUrl(serviceUrl));
    Producer producer2;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer2));

    // Assert that both producer1 and producer2 are available
    int sendNum = 10;
    for (int i = 0; i < sendNum; ++i) {
        ASSERT_EQ(ResultOk, producer1.send(MessageBuilder().setContent("test").build()));
        ASSERT_EQ(ResultOk, producer2.send(MessageBuilder().setContent("test").build()));
    }

    Message msg;
    for (int i = 0; i < 2 * sendNum; ++i) {
        ASSERT_EQ(ResultOk, consumer.receive(msg));
    }

    client.close();
}

TEST(ServiceUrlProviderTest, testInvalidServiceUrl) {
    const std::string invalidServiceUrl = "invalid://localhost:6650";

    // Assert invalid url throw exception when create client.
    {
        ASSERT_THROW(
            Client client([&invalidServiceUrl]() -> const std::string& { return invalidServiceUrl; }),
            std::invalid_argument);
    }

    // Assert return ResultInvalidUrl when client.updateServiceUrl(invalidServiceUrl);
    {
        Client client([]() -> const std::string& { return lookupUrl; });
        ASSERT_EQ(ResultInvalidUrl, client.updateServiceUrl(invalidServiceUrl));
    }
}

INSTANTIATE_TEST_SUITE_P(BasicEndToEndTest, ServiceUrlProviderTest, testing::Values(lookupUrl, adminUrl));

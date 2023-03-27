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
#include <BinaryProtoLookupService.h>
#include <Future.h>
#include <HTTPLookupService.h>
#include <Utils.h>
#include <gtest/gtest.h>
#include <pulsar/Authentication.h>
#include <pulsar/Client.h>

#include <algorithm>
#include <boost/exception/all.hpp>

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "lib/ClientConnection.h"
#include "lib/ConnectionPool.h"
#include "lib/LogUtils.h"
#include "lib/RetryableLookupService.h"
#include "lib/TimeUtils.h"

using namespace pulsar;

DECLARE_LOG_OBJECT()

static std::string binaryLookupUrl = "pulsar://localhost:6650";
static std::string httpLookupUrl = "http://localhost:8080";

TEST(LookupServiceTest, basicLookup) {
    ExecutorServiceProviderPtr service = std::make_shared<ExecutorServiceProvider>(1);
    AuthenticationPtr authData = AuthFactory::Disabled();
    std::string url = "pulsar://localhost:6650";
    ClientConfiguration conf;
    ExecutorServiceProviderPtr ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(1));
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, true);
    ServiceNameResolver serviceNameResolver(url);
    BinaryProtoLookupService lookupService(serviceNameResolver, pool_, conf);

    TopicNamePtr topicName = TopicName::get("topic");

    Future<Result, LookupDataResultPtr> partitionFuture = lookupService.getPartitionMetadataAsync(topicName);
    LookupDataResultPtr lookupData;
    Result result = partitionFuture.get(lookupData);
    ASSERT_TRUE(lookupData != NULL);
    ASSERT_EQ(0, lookupData->getPartitions());

    const auto topicNamePtr = TopicName::get("topic");
    auto future = lookupService.getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    result = future.get(lookupResult);

    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(url, lookupResult.logicalAddress);
    ASSERT_EQ(url, lookupResult.physicalAddress);
}

static void testMultiAddresses(LookupService& lookupService) {
    std::vector<Result> results;
    constexpr int numRequests = 6;

    auto verifySuccessCount = [&results] {
        // Only half of them succeeded
        ASSERT_EQ(std::count(results.cbegin(), results.cend(), ResultOk), numRequests / 2);
        ASSERT_EQ(std::count(results.cbegin(), results.cend(), ResultRetryable), numRequests / 2);
    };

    for (int i = 0; i < numRequests; i++) {
        const auto topicNamePtr = TopicName::get("topic");
        LookupService::LookupResult lookupResult;
        const auto result = lookupService.getBroker(*topicNamePtr).get(lookupResult);
        LOG_INFO("getBroker [" << i << "] " << result << ", " << lookupResult);
        results.emplace_back(result);
    }
    verifySuccessCount();

    results.clear();
    for (int i = 0; i < numRequests; i++) {
        LookupDataResultPtr data;
        const auto result = lookupService.getPartitionMetadataAsync(TopicName::get("topic")).get(data);
        LOG_INFO("getPartitionMetadataAsync [" << i << "] " << result);
        results.emplace_back(result);
    }
    verifySuccessCount();

    results.clear();
    for (int i = 0; i < numRequests; i++) {
        NamespaceTopicsPtr data;
        const auto result =
            lookupService.getTopicsOfNamespaceAsync(TopicName::get("topic")->getNamespaceName()).get(data);
        LOG_INFO("getTopicsOfNamespaceAsync [" << i << "] " << result);
        results.emplace_back(result);
    }
    verifySuccessCount();
}

TEST(LookupServiceTest, testMultiAddresses) {
    ConnectionPool pool({}, std::make_shared<ExecutorServiceProvider>(1), AuthFactory::Disabled(), true);
    ServiceNameResolver serviceNameResolver("pulsar://localhost,localhost:9999");
    ClientConfiguration conf;
    BinaryProtoLookupService binaryLookupService(serviceNameResolver, pool, conf);
    testMultiAddresses(binaryLookupService);

    // HTTPLookupService calls shared_from_this() internally, we must create a shared pointer to test
    ServiceNameResolver serviceNameResolverForHttp("http://localhost,localhost:9999");
    auto httpLookupServicePtr = std::make_shared<HTTPLookupService>(
        std::ref(serviceNameResolverForHttp), ClientConfiguration{}, AuthFactory::Disabled());
    testMultiAddresses(*httpLookupServicePtr);
}
TEST(LookupServiceTest, testRetry) {
    auto executorProvider = std::make_shared<ExecutorServiceProvider>(1);
    ConnectionPool pool({}, executorProvider, AuthFactory::Disabled(), true);
    ServiceNameResolver serviceNameResolver("pulsar://localhost:9999,localhost");
    ClientConfiguration conf;

    auto lookupService = RetryableLookupService::create(
        std::make_shared<BinaryProtoLookupService>(serviceNameResolver, pool, conf), 30 /* seconds */,
        executorProvider);

    PulsarFriend::setServiceUrlIndex(serviceNameResolver, 0);
    auto topicNamePtr = TopicName::get("lookup-service-test-retry");
    auto future1 = lookupService->getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    ASSERT_EQ(ResultOk, future1.get(lookupResult));
    LOG_INFO("getBroker returns logicalAddress: " << lookupResult.logicalAddress
                                                  << ", physicalAddress: " << lookupResult.physicalAddress);

    PulsarFriend::setServiceUrlIndex(serviceNameResolver, 0);
    auto future2 = lookupService->getPartitionMetadataAsync(topicNamePtr);
    LookupDataResultPtr lookupDataResultPtr;
    ASSERT_EQ(ResultOk, future2.get(lookupDataResultPtr));
    LOG_INFO("getPartitionMetadataAsync returns " << lookupDataResultPtr->getPartitions() << " partitions");

    PulsarFriend::setServiceUrlIndex(serviceNameResolver, 0);
    auto future3 = lookupService->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName());
    NamespaceTopicsPtr namespaceTopicsPtr;
    ASSERT_EQ(ResultOk, future3.get(namespaceTopicsPtr));
    LOG_INFO("getTopicPartitionName Async returns " << namespaceTopicsPtr->size() << " topics");

    std::atomic_int retryCount{0};
    constexpr int totalRetryCount = 3;
    auto future4 = lookupService->executeAsync<int>("key", [&retryCount]() -> Future<Result, int> {
        Promise<Result, int> promise;
        if (++retryCount < totalRetryCount) {
            LOG_INFO("Retry count: " << retryCount);
            promise.setFailed(ResultRetryable);
        } else {
            LOG_INFO("Retry done with " << retryCount << " times");
            promise.setValue(100);
        }
        return promise.getFuture();
    });
    int customResult = 0;
    ASSERT_EQ(ResultOk, future4.get(customResult));
    ASSERT_EQ(customResult, 100);
    ASSERT_EQ(retryCount.load(), totalRetryCount);

    ASSERT_EQ(PulsarFriend::getNumberOfPendingTasks(*lookupService), 0);
}

TEST(LookupServiceTest, testTimeout) {
    auto executorProvider = std::make_shared<ExecutorServiceProvider>(1);
    ConnectionPool pool({}, executorProvider, AuthFactory::Disabled(), true);
    ServiceNameResolver serviceNameResolver("pulsar://localhost:9990,localhost:9902,localhost:9904");
    ClientConfiguration conf;

    constexpr int timeoutInSeconds = 2;
    auto lookupService = RetryableLookupService::create(
        std::make_shared<BinaryProtoLookupService>(serviceNameResolver, pool, conf), timeoutInSeconds,
        executorProvider);
    auto topicNamePtr = TopicName::get("lookup-service-test-retry");

    decltype(std::chrono::high_resolution_clock::now()) startTime;
    auto beforeMethod = [&startTime] { startTime = std::chrono::high_resolution_clock::now(); };
    auto afterMethod = [&startTime](const std::string& name) {
        auto timeInterval = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::high_resolution_clock::now() - startTime)
                                .count();
        LOG_INFO(name << " took " << timeInterval << " seconds");
        ASSERT_TRUE(timeInterval >= timeoutInSeconds * 1000L);
    };

    beforeMethod();
    auto future1 = lookupService->getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    ASSERT_EQ(ResultTimeout, future1.get(lookupResult));
    afterMethod("getBroker");

    beforeMethod();
    auto future2 = lookupService->getPartitionMetadataAsync(topicNamePtr);
    LookupDataResultPtr lookupDataResultPtr;
    ASSERT_EQ(ResultTimeout, future2.get(lookupDataResultPtr));
    afterMethod("getPartitionMetadataAsync");

    beforeMethod();
    auto future3 = lookupService->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName());
    NamespaceTopicsPtr namespaceTopicsPtr;
    ASSERT_EQ(ResultTimeout, future3.get(namespaceTopicsPtr));
    afterMethod("getTopicsOfNamespaceAsync");

    ASSERT_EQ(PulsarFriend::getNumberOfPendingTasks(*lookupService), 0);
}

class LookupServiceTest : public ::testing::TestWithParam<std::string> {
   public:
    void TearDown() override { client_.close(); }

   protected:
    Client client_{GetParam()};
};

TEST_P(LookupServiceTest, basicGetNamespaceTopics) {
    Result result;

    auto nsName = NamespaceName::get("public", GetParam().substr(0, 4) + std::to_string(time(nullptr)));
    std::string topicName1 = "persistent://" + nsName->toString() + "/basicGetNamespaceTopics1";
    std::string topicName2 = "persistent://" + nsName->toString() + "/basicGetNamespaceTopics2";
    std::string topicName3 = "non-persistent://" + nsName->toString() + "/basicGetNamespaceTopics3";

    // 0. create a namespace
    auto createNsUrl = httpLookupUrl + "/admin/v2/namespaces/" + nsName->toString();
    auto res = makePutRequest(createNsUrl, "");
    ASSERT_FALSE(res != 204 && res != 409);

    // 1. trigger auto create topic
    Producer producer1;
    result = client_.createProducer(topicName1, producer1);
    ASSERT_EQ(ResultOk, result);
    Producer producer2;
    result = client_.createProducer(topicName2, producer2);
    ASSERT_EQ(ResultOk, result);
    Producer producer3;
    result = client_.createProducer(topicName3, producer3);
    ASSERT_EQ(ResultOk, result);

    // 2. verify getTopicsOfNamespace by regex mode.
    auto lookupServicePtr = PulsarFriend::getClientImplPtr(client_)->getLookup();
    auto verifyGetTopics = [&](CommandGetTopicsOfNamespace_Mode mode,
                               const std::set<std::string>& expectedTopics) {
        Future<Result, NamespaceTopicsPtr> getTopicsFuture =
            lookupServicePtr->getTopicsOfNamespaceAsync(nsName, mode);
        NamespaceTopicsPtr topicsData;
        result = getTopicsFuture.get(topicsData);
        ASSERT_EQ(ResultOk, result);
        ASSERT_TRUE(topicsData != NULL);
        std::set<std::string> actualTopics(topicsData->begin(), topicsData->end());
        ASSERT_EQ(expectedTopics, actualTopics);
    };
    verifyGetTopics(CommandGetTopicsOfNamespace_Mode_PERSISTENT, {topicName1, topicName2});
    verifyGetTopics(CommandGetTopicsOfNamespace_Mode_NON_PERSISTENT, {topicName3});
    verifyGetTopics(CommandGetTopicsOfNamespace_Mode_ALL, {topicName1, topicName2, topicName3});

    client_.close();
}

TEST_P(LookupServiceTest, testGetSchema) {
    const std::string topic = "testGetSchema" + std::to_string(time(nullptr)) + GetParam().substr(0, 4);
    std::string jsonSchema =
        R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";

    StringMap properties;
    properties.emplace("key1", "value1");
    properties.emplace("key2", "value2");

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setSchema(SchemaInfo(SchemaType::JSON, "json", jsonSchema, properties));
    Producer producer;
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producerConfiguration, producer));

    auto clientImplPtr = PulsarFriend::getClientImplPtr(client_);
    auto lookup = clientImplPtr->getLookup();

    boost::optional<SchemaInfo> schemaInfo;
    auto future = lookup->getSchema(TopicName::get(topic));
    ASSERT_EQ(ResultOk, future.get(schemaInfo));
    ASSERT_EQ(jsonSchema, schemaInfo->getSchema());
    ASSERT_EQ(SchemaType::JSON, schemaInfo->getSchemaType());
    ASSERT_EQ(properties, schemaInfo->getProperties());
}

TEST_P(LookupServiceTest, testGetSchemaNotFund) {
    const std::string topic =
        "testGetSchemaNotFund" + std::to_string(time(nullptr)) + GetParam().substr(0, 4);

    Producer producer;
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producer));

    auto clientImplPtr = PulsarFriend::getClientImplPtr(client_);
    auto lookup = clientImplPtr->getLookup();

    boost::optional<SchemaInfo> schemaInfo;
    auto future = lookup->getSchema(TopicName::get(topic));
    ASSERT_EQ(ResultOk, future.get(schemaInfo));
    ASSERT_FALSE(schemaInfo);
}

TEST_P(LookupServiceTest, testGetKeyValueSchema) {
    const std::string topic =
        "testGetKeyValueSchema" + std::to_string(time(nullptr)) + GetParam().substr(0, 4);
    StringMap properties;
    properties.emplace("key1", "value1");
    properties.emplace("key2", "value2");
    std::string jsonSchema =
        R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
    SchemaInfo keySchema(JSON, "key-json", jsonSchema, properties);
    SchemaInfo valueSchema(JSON, "value-json", jsonSchema, properties);
    SchemaInfo keyValueSchema(keySchema, valueSchema, KeyValueEncodingType::INLINE);

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setSchema(keyValueSchema);
    Producer producer;
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producerConfiguration, producer));

    auto clientImplPtr = PulsarFriend::getClientImplPtr(client_);
    auto lookup = clientImplPtr->getLookup();

    boost::optional<SchemaInfo> schemaInfo;
    auto future = lookup->getSchema(TopicName::get(topic));
    ASSERT_EQ(ResultOk, future.get(schemaInfo));
    ASSERT_EQ(keyValueSchema.getSchema(), schemaInfo->getSchema());
    ASSERT_EQ(SchemaType::KEY_VALUE, schemaInfo->getSchemaType());
    ASSERT_FALSE(schemaInfo->getProperties().empty());
}

INSTANTIATE_TEST_CASE_P(Pulsar, LookupServiceTest, ::testing::Values(binaryLookupUrl, httpLookupUrl));

class BinaryProtoLookupServiceRedirectTestHelper : public BinaryProtoLookupService {
   public:
    BinaryProtoLookupServiceRedirectTestHelper(ServiceNameResolver& serviceNameResolver, ConnectionPool& pool,
                                               const ClientConfiguration& clientConfiguration)
        : BinaryProtoLookupService(serviceNameResolver, pool, clientConfiguration) {}

    LookupResultFuture findBroker(const std::string& address, bool authoritative, const std::string& topic,
                                  size_t redirectCount) {
        return BinaryProtoLookupService::findBroker(address, authoritative, topic, redirectCount);
    }
};  // class BinaryProtoLookupServiceRedirectTestHelper

TEST(LookupServiceTest, testRedirectionLimit) {
    const auto redirect_limit = 5;
    AuthenticationPtr authData = AuthFactory::Disabled();
    ClientConfiguration conf;
    conf.setMaxLookupRedirects(redirect_limit);
    ExecutorServiceProviderPtr ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(1));
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, true);
    std::string url = "pulsar://localhost:6650";
    ServiceNameResolver serviceNameResolver(url);
    BinaryProtoLookupServiceRedirectTestHelper lookupService(serviceNameResolver, pool_, conf);

    const auto topicNamePtr = TopicName::get("topic");
    for (auto idx = 0; idx < redirect_limit + 5; ++idx) {
        auto future =
            lookupService.findBroker(serviceNameResolver.resolveHost(), false, topicNamePtr->toString(), idx);
        LookupService::LookupResult lookupResult;
        auto result = future.get(lookupResult);

        if (idx <= redirect_limit) {
            ASSERT_EQ(ResultOk, result);
            ASSERT_EQ(url, lookupResult.logicalAddress);
            ASSERT_EQ(url, lookupResult.physicalAddress);
        } else {
            ASSERT_EQ(ResultTooManyLookupRequestException, result);
        }
    }
}

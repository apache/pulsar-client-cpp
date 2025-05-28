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
#include <pulsar/Authentication.h>
#include <pulsar/Client.h>

#include <algorithm>
#include <boost/exception/all.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "PulsarWrapper.h"
#include "lib/BinaryProtoLookupService.h"
#include "lib/ClientConnection.h"
#include "lib/ConnectionPool.h"
#include "lib/Future.h"
#include "lib/HTTPLookupService.h"
#include "lib/LogUtils.h"
#include "lib/RetryableLookupService.h"
#include "lib/TimeUtils.h"
#include "lib/Utils.h"

DECLARE_LOG_OBJECT()

static std::string binaryLookupUrl = "pulsar://localhost:6650";
static std::string httpLookupUrl = "http://localhost:8080";

extern std::string unique_str();

namespace pulsar {

class LookupServiceTest : public ::testing::TestWithParam<std::string> {
   public:
    void SetUp() override {
        serviceUrl_ = GetParam();
        client_ = Client{serviceUrl_};
    }
    void TearDown() override { client_.close(); }

    template <typename T>
    static bool isEmpty(const RetryableOperationCache<T>& cache) {
        std::lock_guard<std::mutex> lock{cache.mutex_};
        return cache.operations_.empty();
    }

    static size_t isEmpty(const RetryableLookupService& service) {
        return isEmpty(*service.lookupCache_) && isEmpty(*service.partitionLookupCache_) &&
               isEmpty(*service.namespaceLookupCache_) && isEmpty(*service.getSchemaCache_);
    }

   protected:
    std::string serviceUrl_;
    Client client_{httpLookupUrl};
};

}  // namespace pulsar

using namespace pulsar;

TEST(LookupServiceTest, basicLookup) {
    ExecutorServiceProviderPtr service = std::make_shared<ExecutorServiceProvider>(1);
    AuthenticationPtr authData = AuthFactory::Disabled();
    std::string url = "pulsar://localhost:6650";
    ClientConfiguration conf;
    ExecutorServiceProviderPtr ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(1));
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, "");
    BinaryProtoLookupService lookupService(url, pool_, conf);

    TopicNamePtr topicName = TopicName::get("topic");

    Future<Result, LookupDataResultPtr> partitionFuture = lookupService.getPartitionMetadataAsync(topicName);
    LookupDataResultPtr lookupData;
    partitionFuture.get(lookupData);
    ASSERT_TRUE(lookupData != NULL);
    ASSERT_EQ(0, lookupData->getPartitions());

    const auto topicNamePtr = TopicName::get("topic");
    auto future = lookupService.getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    auto result = future.get(lookupResult);

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
        const auto result = lookupService
                                .getTopicsOfNamespaceAsync(TopicName::get("topic")->getNamespaceName(),
                                                           CommandGetTopicsOfNamespace_Mode_PERSISTENT)
                                .get(data);
        LOG_INFO("getTopicsOfNamespaceAsync [" << i << "] " << result);
        results.emplace_back(result);
    }
    verifySuccessCount();
}

TEST(LookupServiceTest, testMultiAddresses) {
    ConnectionPool pool({}, std::make_shared<ExecutorServiceProvider>(1), AuthFactory::Disabled(), "");
    ClientConfiguration conf;
    BinaryProtoLookupService binaryLookupService("pulsar://localhost,localhost:9999", pool, conf);
    testMultiAddresses(binaryLookupService);

    // HTTPLookupService calls shared_from_this() internally, we must create a shared pointer to test
    auto httpLookupServicePtr = std::make_shared<HTTPLookupService>(
        "http://localhost,localhost:9999", ClientConfiguration{}, AuthFactory::Disabled());
    testMultiAddresses(*httpLookupServicePtr);
}
TEST(LookupServiceTest, testRetry) {
    auto executorProvider = std::make_shared<ExecutorServiceProvider>(1);
    ConnectionPool pool({}, executorProvider, AuthFactory::Disabled(), "");
    ClientConfiguration conf;

    auto lookupService = RetryableLookupService::create(
        std::make_shared<BinaryProtoLookupService>("pulsar://localhost:9999,localhost", pool, conf),
        std::chrono::seconds(30), executorProvider);
    ServiceNameResolver& serviceNameResolver = lookupService->getServiceNameResolver();

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
    auto future3 = lookupService->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName(),
                                                            CommandGetTopicsOfNamespace_Mode_PERSISTENT);
    NamespaceTopicsPtr namespaceTopicsPtr;
    ASSERT_EQ(ResultOk, future3.get(namespaceTopicsPtr));
    LOG_INFO("getTopicPartitionName Async returns " << namespaceTopicsPtr->size() << " topics");

    ASSERT_TRUE(LookupServiceTest::isEmpty(*lookupService));
}

TEST(LookupServiceTest, testTimeout) {
    auto executorProvider = std::make_shared<ExecutorServiceProvider>(1);
    ConnectionPool pool({}, executorProvider, AuthFactory::Disabled(), "");
    ClientConfiguration conf;

    constexpr int timeoutInSeconds = 2;
    auto lookupService = RetryableLookupService::create(
        std::make_shared<BinaryProtoLookupService>("pulsar://localhost:9990,localhost:9902,localhost:9904",
                                                   pool, conf),
        std::chrono::seconds(timeoutInSeconds), executorProvider);
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
    auto future3 = lookupService->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName(),
                                                            CommandGetTopicsOfNamespace_Mode_PERSISTENT);
    NamespaceTopicsPtr namespaceTopicsPtr;
    ASSERT_EQ(ResultTimeout, future3.get(namespaceTopicsPtr));
    afterMethod("getTopicsOfNamespaceAsync");

    ASSERT_TRUE(LookupServiceTest::isEmpty(*lookupService));
}

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

    SchemaInfo schemaInfo;
    auto future = lookup->getSchema(TopicName::get(topic));
    ASSERT_EQ(ResultOk, future.get(schemaInfo));
    ASSERT_EQ(jsonSchema, schemaInfo.getSchema());
    ASSERT_EQ(SchemaType::JSON, schemaInfo.getSchemaType());
    ASSERT_EQ(properties, schemaInfo.getProperties());
}

TEST_P(LookupServiceTest, testGetSchemaNotFound) {
    const std::string topic =
        "testGetSchemaNotFund" + std::to_string(time(nullptr)) + GetParam().substr(0, 4);

    Producer producer;
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producer));

    auto clientImplPtr = PulsarFriend::getClientImplPtr(client_);
    auto lookup = clientImplPtr->getLookup();

    SchemaInfo schemaInfo;
    auto future = lookup->getSchema(TopicName::get(topic));
    ASSERT_EQ(ResultTopicNotFound, future.get(schemaInfo));
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

    SchemaInfo schemaInfo;
    auto future = lookup->getSchema(TopicName::get(topic));
    ASSERT_EQ(ResultOk, future.get(schemaInfo));
    ASSERT_EQ(keyValueSchema.getSchema(), schemaInfo.getSchema());
    ASSERT_EQ(SchemaType::KEY_VALUE, schemaInfo.getSchemaType());
    ASSERT_FALSE(schemaInfo.getProperties().empty());
}

TEST_P(LookupServiceTest, testGetSchemaByVersion) {
    const auto topic = "testGetSchemaByVersion" + unique_str() + GetParam().substr(0, 4);
    const std::string schema1 = R"({
  "type": "record",
  "name": "User",
  "namespace": "test",
  "fields": [
    {"name": "name", "type": ["null", "string"]},
    {"name": "age", "type": "int"}
  ]
})";
    const std::string schema2 = R"({
  "type": "record",
  "name": "User",
  "namespace": "test",
  "fields": [
    {"name": "age", "type": "int"},
    {"name": "name", "type": ["null", "string"]}
  ]
})";
    ProducerConfiguration producerConf1;
    producerConf1.setSchema(SchemaInfo{AVRO, "Avro", schema1});
    Producer producer1;
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producerConf1, producer1));
    ProducerConfiguration producerConf2;
    producerConf2.setSchema(SchemaInfo{AVRO, "Avro", schema2});
    Producer producer2;
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producerConf2, producer2));

    // Though these messages are invalid, the C++ client can send them successfully
    producer1.send(MessageBuilder().setContent("msg0").build());
    producer2.send(MessageBuilder().setContent("msg1").build());

    ConsumerConfiguration consumerConf;
    consumerConf.setSubscriptionInitialPosition(InitialPositionEarliest);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client_.subscribe(topic, "sub", consumerConf, consumer));

    Message msg1;
    ASSERT_EQ(ResultOk, consumer.receive(msg1, 3000));
    Message msg2;
    ASSERT_EQ(ResultOk, consumer.receive(msg2, 3000));

    auto getSchemaInfo = [this](const std::string& topic, int64_t version) {
        std::promise<SchemaInfo> p;
        client_.getSchemaInfoAsync(topic, version, [&p](Result result, const SchemaInfo& info) {
            if (result == ResultOk) {
                p.set_value(info);
            } else {
                p.set_exception(std::make_exception_ptr(std::runtime_error(strResult(result))));
            }
        });
        return p.get_future().get();
    };
    {
        ASSERT_EQ(msg1.getLongSchemaVersion(), 0);
        const auto info = getSchemaInfo(topic, 0);
        ASSERT_EQ(info.getSchemaType(), SchemaType::AVRO);
        ASSERT_EQ(info.getSchema(), schema1);
    }
    {
        ASSERT_EQ(msg2.getLongSchemaVersion(), 1);
        const auto info = getSchemaInfo(topic, 1);
        ASSERT_EQ(info.getSchemaType(), SchemaType::AVRO);
        ASSERT_EQ(info.getSchema(), schema2);
    }
    {
        const auto info = getSchemaInfo(topic, -1);
        ASSERT_EQ(info.getSchemaType(), SchemaType::AVRO);
        ASSERT_EQ(info.getSchema(), schema2);
    }
    try {
        getSchemaInfo(topic, 2);
        FAIL();
    } catch (const std::runtime_error& e) {
        ASSERT_EQ(std::string{e.what()}, strResult(ResultTopicNotFound));
    }
    try {
        getSchemaInfo(topic + "-not-exist", 0);
        FAIL();
    } catch (const std::runtime_error& e) {
        ASSERT_EQ(std::string{e.what()}, strResult(ResultTopicNotFound));
    }

    consumer.close();
    producer1.close();
    producer2.close();
}

TEST_P(LookupServiceTest, testGetSchemaTimeout) {
    if (serviceUrl_.find("pulsar://") == std::string::npos) {
        // HTTP request timeout can only be configured with seconds
        return;
    }
    const auto topic = "lookup-service-test-get-schema-timeout";
    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setSchema(SchemaInfo(STRING, "", ""));
    ASSERT_EQ(ResultOk, client_.createProducer(topic, producerConf, producer));
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("msg").build()));
    client_.close();

    ClientConfiguration conf;
    PulsarWrapper::deref(conf).operationTimeout = std::chrono::nanoseconds(1);
    client_ = Client{serviceUrl_, conf};
    auto promise = std::make_shared<std::promise<Result>>();
    client_.getSchemaInfoAsync(topic, 0L,
                               [promise](Result result, const SchemaInfo&) { promise->set_value(result); });
    auto future = promise->get_future();
    ASSERT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
    ASSERT_EQ(future.get(), ResultTimeout);
    client_.close();
}

INSTANTIATE_TEST_SUITE_P(Pulsar, LookupServiceTest, ::testing::Values(binaryLookupUrl, httpLookupUrl));

class BinaryProtoLookupServiceRedirectTestHelper : public BinaryProtoLookupService {
   public:
    BinaryProtoLookupServiceRedirectTestHelper(const std::string& serviceUrl, ConnectionPool& pool,
                                               const ClientConfiguration& clientConfiguration)
        : BinaryProtoLookupService(serviceUrl, pool, clientConfiguration) {}

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
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, "");
    string url = "pulsar://localhost:6650";
    BinaryProtoLookupServiceRedirectTestHelper lookupService(url, pool_, conf);

    const auto topicNamePtr = TopicName::get("topic");
    for (auto idx = 0; idx < redirect_limit + 5; ++idx) {
        auto future = lookupService.findBroker(lookupService.getServiceNameResolver().resolveHost(), false,
                                               topicNamePtr->toString(), idx);
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

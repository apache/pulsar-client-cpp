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

#include <chrono>
#include <future>

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "WaitUtils.h"
#include "lib/LogUtils.h"
#include "lib/TopicName.h"

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";

DECLARE_LOG_OBJECT()

TEST(TableViewTest, testCreateTableView) {
    const std::string topic = "testCreateTableView" + std::to_string(time(nullptr));
    Client client(lookupUrl);

    static const std::string jsonSchema =
        R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
    SchemaInfo schemaInfo(JSON, "test-json", jsonSchema);
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setSchema(schemaInfo);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    // Create table view failed, The schema is not compatible
    TableViewConfiguration tableViewConfiguration{.schemaInfo = SchemaInfo(AVRO, "", "")};
    TableView tableView;
    ASSERT_EQ(ResultIncompatibleSchema, client.createTableView(topic, tableViewConfiguration, tableView));
    ASSERT_EQ(ResultConsumerNotInitialized, tableView.close());

    // Create table view success.
    ASSERT_EQ(ResultOk, client.createTableView(topic, {.schemaInfo = schemaInfo}, tableView));
    ASSERT_EQ(ResultOk, tableView.close());

    client.close();
}

TEST(TableViewTest, testSimpleTableView) {
    const std::string topic = "testSimpleTableView" + std::to_string(time(nullptr));
    Client client(lookupUrl);

    ProducerConfiguration producerConfiguration;
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    auto count = 20;
    for (int i = 0; i < count; ++i) {
        auto msg = MessageBuilder()
                       .setPartitionKey("key" + std::to_string(i))
                       .setContent("value" + std::to_string(i))
                       .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // Create table view and assert size.
    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, {}, tableView));
    ASSERT_EQ(tableView.size(), count);

    // Send some more messages, The 0 ~ count message key/value is duplicated send.
    for (int i = 0; i < count * 2; ++i) {
        auto msg = MessageBuilder()
                       .setPartitionKey("key" + std::to_string(i))
                       .setContent("value" + std::to_string(i))
                       .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    waitUntil(
        std::chrono::seconds(2), [&] { return tableView.size() == count * 2; }, 1000);
    ASSERT_EQ(tableView.size(), count * 2);

    // assert interfaces.
    std::string value;
    ASSERT_TRUE(tableView.containsKey("key1"));
    ASSERT_TRUE(tableView.getValue("key1", value));
    ASSERT_EQ(value, "value1");

    // Test value update
    ASSERT_EQ(ResultOk,
              producer.send(MessageBuilder().setPartitionKey("key1").setContent("value1-update").build()));
    ASSERT_TRUE(waitUntil(std::chrono::seconds(2), [&tableView]() {
        std::string value;
        tableView.getValue("key1", value);
        return value == "value1-update";
    }));

    // retrieveValue will remove the key/value from the table view.
    ASSERT_TRUE(tableView.retrieveValue("key1", value));
    ASSERT_EQ(value, "value1-update");
    ASSERT_FALSE(tableView.containsKey("key1"));
    ASSERT_EQ(tableView.snapshot().size(), count * 2 - 1);
    ASSERT_EQ(tableView.size(), 0);

    client.close();
}

TEST(TableViewTest, testPublishEmptyValue) {
    const std::string topic = "testPublishEmptyValue" + std::to_string(time(nullptr));
    Client client(lookupUrl);

    ProducerConfiguration producerConfiguration;
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    auto count = 20;
    for (int i = 0; i < count; ++i) {
        auto msg = MessageBuilder()
                       .setPartitionKey("key" + std::to_string(i))
                       .setContent("value" + std::to_string(i))
                       .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // Create table view failed, The schema is not compatible
    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, {}, tableView));
    ASSERT_EQ(tableView.size(), count);

    // Set the v of k1 is empty
    auto msg = MessageBuilder().setPartitionKey("key1").setContent("").build();
    ASSERT_EQ(ResultOk, producer.send(msg));
    waitUntil(
        std::chrono::seconds(2), [&] { return tableView.size() == count - 1; }, 1000);
    ASSERT_EQ(tableView.size(), count - 1);

    // assert interfaces.
    std::string value;
    ASSERT_TRUE(!tableView.containsKey("key1"));
    ASSERT_TRUE(!tableView.getValue("key1", value));
    ASSERT_TRUE(value.empty());

    client.close();
}

TEST(TableViewTest, testNullValueTombstone) {
    const std::string topic = "testNullValueTombstone" + std::to_string(time(nullptr));
    Client client(lookupUrl);

    ProducerConfiguration producerConfiguration;
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    // Send initial messages with keys
    auto count = 10;
    for (int i = 0; i < count; ++i) {
        auto msg = MessageBuilder()
                       .setPartitionKey("key" + std::to_string(i))
                       .setContent("value" + std::to_string(i))
                       .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // Create table view and verify all keys are present
    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, {}, tableView));
    ASSERT_EQ(tableView.size(), count);

    std::string value;
    ASSERT_TRUE(tableView.containsKey("key5"));
    ASSERT_TRUE(tableView.getValue("key5", value));
    ASSERT_EQ(value, "value5");

    // Send a null value (tombstone) for key5 using setNullValue()
    auto tombstone = MessageBuilder().setPartitionKey("key5").setNullValue().build();
    ASSERT_TRUE(tombstone.hasNullValue());
    ASSERT_EQ(ResultOk, producer.send(tombstone));

    // Wait for table view to process the tombstone and remove the key
    waitUntil(
        std::chrono::seconds(2), [&] { return !tableView.containsKey("key5"); }, 100);

    // Verify key5 was removed by the tombstone
    ASSERT_FALSE(tableView.containsKey("key5"));
    ASSERT_EQ(tableView.size(), count - 1);

    // Verify other keys are still present
    ASSERT_TRUE(tableView.containsKey("key0"));
    ASSERT_TRUE(tableView.containsKey("key9"));

    client.close();
}

TEST(TableViewTest, testNullValueVsEmptyString) {
    const std::string topic = "testNullValueVsEmptyString" + std::to_string(time(nullptr));
    Client client(lookupUrl);

    ProducerConfiguration producerConfiguration;
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    // Send messages for two keys
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setPartitionKey("keyA").setContent("valueA").build()));
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setPartitionKey("keyB").setContent("valueB").build()));

    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, {}, tableView));
    ASSERT_EQ(tableView.size(), 2);

    // Send empty string for keyA - this should also remove it from TableView
    // (TableView treats empty payload as deletion)
    auto emptyMsg = MessageBuilder().setPartitionKey("keyA").setContent("").build();
    ASSERT_FALSE(emptyMsg.hasNullValue());
    ASSERT_EQ(ResultOk, producer.send(emptyMsg));

    // Send null value (tombstone) for keyB using setNullValue()
    auto nullMsg = MessageBuilder().setPartitionKey("keyB").setNullValue().build();
    ASSERT_TRUE(nullMsg.hasNullValue());
    ASSERT_EQ(ResultOk, producer.send(nullMsg));

    // Wait for both to be processed
    waitUntil(
        std::chrono::seconds(2), [&] { return tableView.size() == 0; }, 100);

    // Both keys should be removed
    ASSERT_FALSE(tableView.containsKey("keyA"));
    ASSERT_FALSE(tableView.containsKey("keyB"));
    ASSERT_EQ(tableView.size(), 0);

    client.close();
}

TEST(TableViewTest, testNotSupportNonPersistentTopic) {
    const std::string topic = TopicDomain::NonPersistent +
                              "://public/default/testNotSupportNonPersistentTopic" +
                              std::to_string(time(nullptr));
    Client client(lookupUrl);

    TableView tableView;
    ASSERT_EQ(ResultNotAllowedError, client.createTableView(topic, {}, tableView));
    client.close();
}

TEST(TableViewTest, testMultiTopicAndAutoUpdatePartitions) {
    std::string uniqueTimeStr = std::to_string(time(nullptr));
    std::string topic = "persistent://public/default/testMultiTopicAndAutoUpdatePartitions" + uniqueTimeStr;
    ClientConfiguration clientConfiguration;
    clientConfiguration.setPartititionsUpdateInterval(1);
    Client client(lookupUrl, clientConfiguration);

    // create partition is 5
    {
        std::string url = adminUrl +
                          "admin/v2/persistent/public/default/testMultiTopicAndAutoUpdatePartitions" +
                          uniqueTimeStr + "/partitions";
        int res = makePutRequest(url, "5");
        LOG_INFO("res = " << res);
        ASSERT_FALSE(res != 204 && res != 409);
    }

    ProducerConfiguration producerConfiguration;
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfiguration, producer));

    auto count = 20;
    for (int i = 0; i < count; ++i) {
        auto msg = MessageBuilder()
                       .setPartitionKey("key" + std::to_string(i))
                       .setContent("value" + std::to_string(i))
                       .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, {}, tableView));
    ASSERT_EQ(tableView.size(), count);

    // update partitions is 10
    {
        std::string url = adminUrl +
                          "admin/v2/persistent/public/default/testMultiTopicAndAutoUpdatePartitions" +
                          uniqueTimeStr + "/partitions";
        int res = makePostRequest(url, "10");
        LOG_INFO("res = " << res);
        ASSERT_FALSE(res != 204 && res != 409);
    }
    waitUntil(
        std::chrono::seconds(5), [&] { return PulsarFriend::getPartitionProducerSize(producer) == 10; }, 200);
    ASSERT_EQ(PulsarFriend::getPartitionProducerSize(producer), 10);

    for (int i = count; i < count * 2; ++i) {
        auto msg = MessageBuilder()
                       .setPartitionKey("key" + std::to_string(i))
                       .setContent("value" + std::to_string(i))
                       .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    waitUntil(
        std::chrono::seconds(10), [&] { return tableView.size() == count * 2; }, 200);
    ASSERT_EQ(tableView.size(), count * 2);

    client.close();
}

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
    TableViewConfiguration tableViewConfiguration;
    tableViewConfiguration.setSchemaInfo(SchemaInfo(AVRO, "", ""));
    TableView tableView;
    ASSERT_EQ(ResultIncompatibleSchema, client.createTableView(topic, tableViewConfiguration, tableView));
    ASSERT_EQ(ResultConsumerNotInitialized, tableView.close());

    // Create table view success.
    tableViewConfiguration.setSchemaInfo(schemaInfo);
    ASSERT_EQ(ResultOk, client.createTableView(topic, tableViewConfiguration, tableView));
    ASSERT_EQ(ResultOk, tableView.close());

    // Test async create and close the client during the process.
    Latch latch(1);
    client.createTableViewAsync(
        topic, tableViewConfiguration, [&latch](Result result, const TableView& tableView) {
            latch.countdown();
            ASSERT_TRUE(result == ResultConnectError || result == ResultAlreadyClosed);
        });
    client.close();
    latch.wait();
}

TEST(TableViewTest, testSimpleTableView) {
    const std::string topic = "testTableView" + std::to_string(time(nullptr));
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
    TableViewConfiguration tableViewConfiguration;
    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, tableViewConfiguration, tableView));
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

    // assert interfaces.
    std::string value;
    ASSERT_TRUE(tableView.getValue("key1", value));
    ASSERT_EQ(value, "value1");
    ASSERT_TRUE(tableView.retrieveValue("key1", value));
    ASSERT_EQ(value, "value1");
    ASSERT_FALSE(tableView.containsKey("key1"));
    ASSERT_EQ(tableView.snapshot().size(), count * 2 - 1);
    ASSERT_EQ(tableView.size(), 0);

    client.close();
}

TEST(TableViewTest, testPublishNullValue) {
    const std::string topic = "testTableView" + std::to_string(time(nullptr));
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
    TableViewConfiguration tableViewConfiguration;
    TableView tableView;
    ASSERT_EQ(ResultOk, client.createTableView(topic, tableViewConfiguration, tableView));
    ASSERT_EQ(tableView.size(), count);

    // Set the v of k1 is empty
    auto msg = MessageBuilder().setPartitionKey("key1").setContent("").build();
    ASSERT_EQ(ResultOk, producer.send(msg));
    waitUntil(
        std::chrono::seconds(2), [&] { return tableView.size() == count - 1; }, 1000);

    // assert interfaces.
    std::string value;
    ASSERT_TRUE(!tableView.containsKey("key1"));
    ASSERT_TRUE(!tableView.getValue("key1", value));
    ASSERT_TRUE(value.empty());

    client.close();
}

TEST(TableViewTest, testNotSupportNonPersistentTopic) {
    const std::string topic = TopicDomain::NonPersistent +
                              "://public/default/testNotSupportNonPersistentTopic" +
                              std::to_string(time(nullptr));
    Client client(lookupUrl);

    // Create table view failed, The schema is not compatible
    TableViewConfiguration tableViewConfiguration;
    TableView tableView;
    ASSERT_EQ(ResultNotAllowedError, client.createTableView(topic, tableViewConfiguration, tableView));
    client.close();
}

TEST(TableViewTest, testMultiTopicAndAutoUpdatePartitions) {
    std::string uniqueTimeStr = std::to_string(time(nullptr));
    std::string topic = "persistent://public/default/testMultiTopicAndAutoUpdatePartitions" + uniqueTimeStr;
    Client client(lookupUrl);

    std::string url = adminUrl + "admin/v2/persistent/public/default/testMultiTopicAndAutoUpdatePartitions" +
                      uniqueTimeStr + "/partitions";
    int res = makePutRequest(url, "5");
    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

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

    TableViewConfiguration tableViewConfiguration;
    TableView tableView;
    // TODO need support multiReader first.
    ASSERT_EQ(ResultOperationNotSupported, client.createTableView(topic, tableViewConfiguration, tableView));

    client.close();
}

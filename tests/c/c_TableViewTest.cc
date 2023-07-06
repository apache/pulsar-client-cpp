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
#include <pulsar/c/client.h>
#include <pulsar/c/table_view.h>
#include <string.h>

#include <future>

static const char *lookup_url = "pulsar://localhost:6650";

struct tv_create_result {
    pulsar_result result;
    pulsar_table_view_t *tableView;
};

static void create_tv_callback(pulsar_result result, pulsar_table_view_t *tableView, void *ctx) {
    std::promise<tv_create_result> *create_promise = (std::promise<tv_create_result> *)ctx;
    create_promise->set_value({result, tableView});
}

TEST(c_TableViewTest, testCreateTableViewAsync) {
    const char *topic_name = "persistent://public/default/test-create-tv-async";
    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create(lookup_url, conf);

    // Create table view.
    pulsar_table_view_configuration_t *table_view_conf = pulsar_table_view_configuration_create();
    pulsar_table_view_configuration_set_subscription_name(table_view_conf, "sub-name");
    std::promise<tv_create_result> create_promise;
    std::future<tv_create_result> create_future = create_promise.get_future();
    pulsar_client_create_table_view_async(client, topic_name, table_view_conf, create_tv_callback,
                                          &create_promise);
    tv_create_result tvResult = create_future.get();
    ASSERT_EQ(pulsar_result_Ok, tvResult.result);

    pulsar_table_view_free(tvResult.tableView);
    pulsar_client_close(client);
}

struct tv_action_ctx {
    char *expect_data;
    int size;
    int expect_size;
    std::promise<bool> *listen_promise;
};

static void tv_action(const char *key, const void *value, size_t value_size, void *ctx) {
    tv_action_ctx *context = (tv_action_ctx *)ctx;
    context->size++;
    ASSERT_EQ(memcmp(value, context->expect_data, value_size), 0);
    if (context->size == context->expect_size) {
        context->listen_promise->set_value(true);
    }
}

TEST(c_TableViewTest, testSimpleTableView) {
    const char *topic_name = "persistent://public/default/1test-table_view";
    const char *sub_name = "my-sub-name";

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create(lookup_url, conf);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    // Send messages
    const int num = 10;
    const char *key1 = "key1";
    const char *key2 = "key2";
    size_t data_size = 4;
    char *data = (char *)malloc(data_size);
    data[0] = 0x01;
    data[1] = 0x00;
    data[2] = 0x02;
    data[3] = 0x00;
    for (int i = 0; i < num; i++) {
        pulsar_message_t *message = pulsar_message_create();
        if (i % 2 == 0) {
            pulsar_message_set_partition_key(message, key1);
        } else {
            pulsar_message_set_partition_key(message, key2);
        }
        pulsar_message_set_content(message, data, data_size);
        pulsar_result res = pulsar_producer_send(producer, message);
        ASSERT_EQ(pulsar_result_Ok, res);
        pulsar_message_free(message);
    }

    // Create table view.
    pulsar_table_view_configuration_t *table_view_conf = pulsar_table_view_configuration_create();
    pulsar_table_view_configuration_set_subscription_name(table_view_conf, sub_name);
    pulsar_table_view_t *table_view;
    result = pulsar_client_create_table_view(client, topic_name, table_view_conf, &table_view);
    ASSERT_EQ(pulsar_result_Ok, result);

    // test get value
    void *v1;
    size_t v1_size;
    ASSERT_EQ(pulsar_table_view_size(table_view), 2);
    ASSERT_TRUE(pulsar_table_view_get_value(table_view, "key1", &v1, &v1_size));
    ASSERT_EQ(v1_size, data_size);
    ASSERT_EQ(memcmp(v1, data, data_size), 0);
    free(v1);

    // test for each.
    tv_action_ctx ctx;
    ctx.expect_data = data;
    ctx.size = 0;
    pulsar_table_view_for_each(table_view, tv_action, &ctx);
    ASSERT_EQ(ctx.size, 2);

    // test for each and listen
    std::promise<bool> listen_promise;
    std::future<bool> listen_future = listen_promise.get_future();
    tv_action_ctx ctx2;
    ctx2.expect_data = data;
    ctx2.size = 0;
    ctx2.expect_size = 3;
    ctx2.listen_promise = &listen_promise;
    pulsar_table_view_for_each_add_listen(table_view, tv_action, &ctx2);
    ASSERT_EQ(ctx.size, 2);
    // send more message.
    pulsar_message_t *message = pulsar_message_create();
    pulsar_message_set_partition_key(message, "key3");
    pulsar_message_set_content(message, data, data_size);
    pulsar_result res = pulsar_producer_send(producer, message);
    ASSERT_EQ(pulsar_result_Ok, res);
    pulsar_message_free(message);
    // wait for message.
    ASSERT_TRUE(listen_future.get());
    ASSERT_EQ(ctx2.size, ctx2.expect_size);

    // test retrieve value
    void *v2;
    size_t v2_size;
    ASSERT_TRUE(pulsar_table_view_retrieve_value(table_view, "key2", &v2, &v2_size));
    ASSERT_EQ(v2_size, data_size);
    ASSERT_EQ(memcmp(v2, data, data_size), 0);
    free(v2);

    // test table view size
    ASSERT_FALSE(pulsar_table_view_contain_key(table_view, "key2"));
    ASSERT_EQ(pulsar_table_view_size(table_view), 2);

    free(data);
    pulsar_producer_close(producer);
    pulsar_table_view_close(table_view);
    pulsar_client_close(client);
    pulsar_table_view_free(table_view);
    pulsar_table_view_configuration_free(table_view_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}

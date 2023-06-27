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
#include <string.h>

#include "pulsar/c/table_view.h"

static const char *lookup_url = "pulsar://localhost:6650";

TEST(c_TableViewTest, testSimpleTableView) {
    const char *topic_name = "persistent://public/default/test-table_view20";
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
    const char *value = "content";
    for (int i = 0; i < num; i++) {
        pulsar_message_t *message = pulsar_message_create();
        if (i % 2 == 0) {
            pulsar_message_set_partition_key(message, key1);
        } else {
            pulsar_message_set_partition_key(message, key2);
        }
        pulsar_message_set_content(message, value, strlen(value));
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

    char *v1;
    ASSERT_EQ(pulsar_table_view_size(table_view), 2);
    ASSERT_TRUE(pulsar_table_view_get_value(table_view, "key1", &v1));
    ASSERT_STREQ(v1, "content");
    delete v1;

    char *v2;
    ASSERT_TRUE(pulsar_table_view_retrieve_value(table_view, "key2", &v2));
    ASSERT_STREQ(v2, "content");
    delete v2;

    ASSERT_FALSE(pulsar_table_view_contain_key(table_view, "key2"));
    ASSERT_EQ(pulsar_table_view_size(table_view), 1);

    pulsar_string_map_t *pMap = pulsar_table_view_snapshot(table_view);
    ASSERT_EQ(pulsar_table_view_size(table_view), 0);
    ASSERT_EQ(pulsar_string_map_size(pMap), 1);
    pulsar_string_map_free(pMap);

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

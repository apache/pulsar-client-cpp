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
const char *lookup_url = "pulsar://localhost:6650";

TEST(C_ConsumerConfigurationTest, testCBatchReceive) {
    const char *topic_name = "persistent://public/default/test-c-batch-receive5";
    const char *sub_name = "my-sub-name";

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create(lookup_url, conf);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    const int batch_receive_max_size = 10;

    pulsar_consumer_batch_receive_policy_t batch_receive_policy{batch_receive_max_size, -1, -1};
    pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, &batch_receive_policy);

    pulsar_consumer_t *consumer;
    result = pulsar_client_subscribe(client, topic_name, sub_name, consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    // Send messages
    const char *data = "my-content";
    for (int i = 0; i < batch_receive_max_size; i++) {
        pulsar_message_t *message = pulsar_message_create();
        pulsar_message_set_content(message, data, strlen(data));
        pulsar_result res = pulsar_producer_send(producer, message);
        ASSERT_EQ(pulsar_result_Ok, res);
        pulsar_message_free(message);
    }

    // Batch receive messages
    pulsar_messages_t *msgs = pulsar_messages_create();
    pulsar_result res = pulsar_consumer_batch_receive(consumer, &msgs);
    ASSERT_EQ(pulsar_result_Ok, res);
    ASSERT_EQ(batch_receive_max_size, pulsar_messages_size(msgs));
    for (int i = 0; i < pulsar_messages_size(msgs); i++) {
        pulsar_message_t *msg = pulsar_messages_get(msgs, i);
        const char *msg_data = (const char *)pulsar_message_get_data(msg);
        ASSERT_STREQ(data, msg_data);
    }
    pulsar_messages_free(msgs);

    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_unsubscribe(consumer));
    ASSERT_EQ(pulsar_result_AlreadyClosed, pulsar_consumer_close(consumer));
    ASSERT_EQ(pulsar_result_Ok, pulsar_producer_close(producer));
    ASSERT_EQ(pulsar_result_Ok, pulsar_client_close(client));

    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}

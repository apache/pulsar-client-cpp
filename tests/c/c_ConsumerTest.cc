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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static const char *lookup_url = "pulsar://localhost:6650";

TEST(c_ConsumerTest, testBatchReceive) {
    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create(lookup_url, conf);

    char topic[128];
    snprintf(topic, sizeof(topic), "c-consumer-test-batch-receive-%ld", time(NULL));

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_t *consumer;
    result = pulsar_client_subscribe(client, topic, "sub", consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    const int num_messages = 10;
    for (int i = 0; i < num_messages; i++) {
        pulsar_message_t *msg = pulsar_message_create();
        char buf[128];
        snprintf(buf, sizeof(buf), "msg-%d", i);
        pulsar_message_set_content(msg, buf, strlen(buf) + 1);
        ASSERT_EQ(pulsar_result_Ok, pulsar_producer_send(producer, msg));
        pulsar_message_free(msg);
    }

    pulsar_messages_t *msgs = NULL;
    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_batch_receive(consumer, &msgs));
    ASSERT_EQ(pulsar_messages_size(msgs), num_messages);
    for (int i = 0; i < num_messages; i++) {
        pulsar_message_t *msg = pulsar_messages_get(msgs, i);
        size_t length = pulsar_message_get_length(msg);
        char *str = (char *)malloc(pulsar_message_get_length(msg));
        strncpy(str, (const char *)pulsar_message_get_data(msg), length);

        char expected_str[128];
        snprintf(expected_str, sizeof(expected_str), "msg-%d", i);
        printf("%d received: %s (%zd), expected: %s (%zd)\n", i, str, strlen(str), expected_str,
               strlen(expected_str));
        ASSERT_EQ(strcmp(str, expected_str), 0);

        free(str);
    }

    pulsar_client_close(client);
    pulsar_messages_free(msgs);
    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}

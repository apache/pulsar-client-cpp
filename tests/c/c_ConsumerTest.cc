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

#include <future>

static const char *lookup_url = "pulsar://localhost:6650";

struct batch_receive_ctx {
    pulsar_consumer_t *consumer;
    std::promise<pulsar_result> *promise;
    int expect_receive_num;
};

static void batch_receive_callback(pulsar_result async_result, pulsar_messages_t *msgs, void *ctx) {
    struct batch_receive_ctx *receive_ctx = (struct batch_receive_ctx *)ctx;
    receive_ctx->promise->set_value(async_result);
    if (async_result == pulsar_result_Ok) {
        ASSERT_EQ(pulsar_messages_size(msgs), receive_ctx->expect_receive_num);
        for (int i = 0; i < pulsar_messages_size(msgs); i++) {
            pulsar_message_t *msg = pulsar_messages_get(msgs, i);
            size_t length = pulsar_message_get_length(msg);
            char *str = (char *)malloc(pulsar_message_get_length(msg));
            strncpy(str, (const char *)pulsar_message_get_data(msg), length);

            char expected_str[128];
            snprintf(expected_str, sizeof(expected_str), "msg-%d", 10 + i);
            printf("%d received: %s (%zd), expected: %s (%zd)\n", i, str, strlen(str), expected_str,
                   strlen(expected_str));
            ASSERT_EQ(strcmp(str, expected_str), 0);
            free(str);
        }
        pulsar_messages_free(msgs);
    }
}

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

    const int batch_receive_max_size = 10;
    pulsar_consumer_batch_receive_policy_t batch_receive_policy{batch_receive_max_size, -1, -1};
    pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, &batch_receive_policy);

    result = pulsar_client_subscribe(client, topic, "sub", consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    // Sending two more messages proves that the batch_receive_policy works.
    for (int i = 0; i < batch_receive_max_size * 2; i++) {
        pulsar_message_t *msg = pulsar_message_create();
        char buf[128];
        snprintf(buf, sizeof(buf), "msg-%d", i);
        pulsar_message_set_content(msg, buf, strlen(buf) + 1);
        ASSERT_EQ(pulsar_result_Ok, pulsar_producer_send(producer, msg));
        pulsar_message_free(msg);
    }

    pulsar_messages_t *msgs = NULL;
    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_batch_receive(consumer, &msgs));
    ASSERT_EQ(pulsar_messages_size(msgs), batch_receive_max_size);
    for (int i = 0; i < batch_receive_max_size; i++) {
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
    pulsar_messages_free(msgs);

    std::promise<pulsar_result> receive_promise;
    std::future<pulsar_result> receive_future = receive_promise.get_future();
    struct batch_receive_ctx batch_receive_ctx = {consumer, &receive_promise, batch_receive_max_size};
    pulsar_consumer_batch_receive_async(consumer, batch_receive_callback, &batch_receive_ctx);
    pulsar_client_close(client);
    ASSERT_EQ(pulsar_result_Ok, receive_future.get());

    pulsar_client_close(client);
    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}

TEST(c_ConsumerTest, testCDeadLetterTopic) {
    const char *topic_name = "persistent://public/default/test-c-dlq-topic";
    const char *dlq_topic_name = "persistent://public/default/c-dlq-topic";
    const char *sub_name = "my-sub-name";

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create(lookup_url, conf);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_configuration_set_consumer_type(consumer_conf, pulsar_ConsumerShared);
    const int max_redeliver_count = 3;
    pulsar_consumer_config_dead_letter_policy_t dlq_policy{dlq_topic_name, max_redeliver_count,
                                                           "init_sub-name"};
    pulsar_consumer_configuration_set_dlq_policy(consumer_conf, &dlq_policy);
    pulsar_consumer_t *consumer;
    result = pulsar_client_subscribe(client, topic_name, sub_name, consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    // Send messages
    const int num = 10;
    const char *data = "my-content";
    for (int i = 0; i < num; i++) {
        pulsar_message_t *message = pulsar_message_create();
        pulsar_message_set_content(message, data, strlen(data));
        pulsar_result res = pulsar_producer_send(producer, message);
        ASSERT_EQ(pulsar_result_Ok, res);
        pulsar_message_free(message);
    }

    // Redelivery all messages
    for (int i = 1; i <= max_redeliver_count * num + num; ++i) {
        pulsar_message_t *message = NULL;
        pulsar_result res = pulsar_consumer_receive(consumer, &message);
        ASSERT_EQ(pulsar_result_Ok, res);
        if (i % num == 0) {
            pulsar_consumer_redeliver_unacknowledged_messages(consumer);
        }
        pulsar_message_free(message);
    }

    // Consumer dlq topic
    pulsar_consumer_t *dlq_consumer;
    pulsar_consumer_configuration_t *dlq_consumer_conf = pulsar_consumer_configuration_create();
    result = pulsar_client_subscribe(client, dlq_topic_name, sub_name, dlq_consumer_conf, &dlq_consumer);
    ASSERT_EQ(pulsar_result_Ok, result);
    for (int i = 0; i < num; ++i) {
        pulsar_message_t *message = NULL;
        pulsar_result res = pulsar_consumer_receive(dlq_consumer, &message);
        ASSERT_EQ(pulsar_result_Ok, res);
        pulsar_message_free(message);
    }
    pulsar_message_t *message = NULL;
    pulsar_result res = pulsar_consumer_receive_with_timeout(dlq_consumer, &message, 200);
    ASSERT_EQ(pulsar_result_Timeout, res);
    pulsar_message_free(message);

    ASSERT_EQ(pulsar_result_Ok, pulsar_client_close(client));

    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_consumer_free(dlq_consumer);
    pulsar_consumer_configuration_free(dlq_consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}

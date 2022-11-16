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

#include <TimeUtils.h>
#include <gtest/gtest.h>
#include <pulsar/c/client.h>

#include <future>

struct seek_ctx {
    std::promise<pulsar_result> *promise;
};

static void seek_callback(pulsar_result async_result, void *ctx) {
    auto *seek_ctx = (struct seek_ctx *)ctx;
    seek_ctx->promise->set_value(async_result);
}

void prepare_client(pulsar_client_t **client) {
    const char *lookup_url = "pulsar://localhost:6650";
    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    *client = pulsar_client_create(lookup_url, conf);
    pulsar_client_configuration_free(conf);
}

TEST(c_SeekTest, testConsumerSeekMessageId) {
    auto topic_name_str = "test-c-seek-msgid-" + std::to_string(time(nullptr));
    const char *topic_name = topic_name_str.c_str();

    pulsar_client_t *client;
    prepare_client(&client);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_t *consumer;
    result = pulsar_client_subscribe(client, topic_name, "seek-time", consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_message_t *seek_message = nullptr;

    for (int i = 0; i < 10; i++) {
        char content[10];
        sprintf(content, "msg-%d", i);
        pulsar_message_t *msg = pulsar_message_create();
        pulsar_message_set_content(msg, content, strlen(content));
        pulsar_producer_send(producer, msg);
        if (i == 5) {
            seek_message = msg;
        } else {
            pulsar_message_free(msg);
        }
    }

    pulsar_consumer_seek(consumer, pulsar_message_get_message_id(seek_message));

    pulsar_message_t *message;
    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_receive_with_timeout(consumer, &message, 1000));
    ASSERT_STREQ((const char *)pulsar_message_get_data(message), "msg-6");
    pulsar_message_free(message);

    // Test seek asynchronously
    std::promise<pulsar_result> seek_promise;
    std::future<pulsar_result> seek_future = seek_promise.get_future();
    struct seek_ctx seek_ctx = {&seek_promise};
    pulsar_consumer_seek_async(consumer, pulsar_message_get_message_id(seek_message), seek_callback,
                               &seek_ctx);
    ASSERT_EQ(pulsar_result_Ok, seek_future.get());
    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_receive_with_timeout(consumer, &message, 1000));
    ASSERT_STREQ((const char *)pulsar_message_get_data(message), "msg-6");

    if (seek_message != NULL) {
        pulsar_message_free(seek_message);
    }
    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
}

TEST(c_SeekTest, testConsumerSeekTime) {
    auto topic_name_str = "test-c-seek-time-" + std::to_string(time(nullptr));
    const char *topic_name = topic_name_str.c_str();

    pulsar_client_t *client;
    prepare_client(&client);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_t *consumer;
    result = pulsar_client_subscribe(client, topic_name, "seek-time", consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    for (int i = 0; i < 10; i++) {
        char content[10];
        sprintf(content, "msg-%d", i);
        pulsar_message_t *msg = pulsar_message_create();
        pulsar_message_set_content(msg, content, strlen(content));
        pulsar_producer_send(producer, msg);
        pulsar_message_free(msg);
    }

    uint64_t currentTime = pulsar::TimeUtils::currentTimeMillis();

    pulsar_consumer_seek_by_timestamp(consumer, currentTime);

    pulsar_message_t *message;
    ASSERT_EQ(pulsar_result_Timeout, pulsar_consumer_receive_with_timeout(consumer, &message, 1000));

    pulsar_consumer_seek_by_timestamp(consumer, currentTime - 100000);  // Seek to 100 seconds ago

    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_receive_with_timeout(consumer, &message, 1000));
    ASSERT_STREQ((const char *)pulsar_message_get_data(message), "msg-0");

    // Test seek asynchronously
    std::promise<pulsar_result> seek_promise;
    std::future<pulsar_result> seek_future = seek_promise.get_future();
    struct seek_ctx seek_ctx = {&seek_promise};
    pulsar_consumer_seek_by_timestamp_async(consumer, currentTime, seek_callback, &seek_ctx);
    ASSERT_EQ(pulsar_result_Ok, seek_future.get());
    ASSERT_EQ(pulsar_result_Timeout, pulsar_consumer_receive_with_timeout(consumer, &message, 1000));

    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
}

TEST(c_SeekTest, testReaderSeekMessageId) {
    auto topic_name_str = "test-c-reader-seek-msgid-" + std::to_string(time(nullptr));
    const char *topic_name = topic_name_str.c_str();

    pulsar_client_t *client;
    prepare_client(&client);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_reader_configuration_t *reader_conf = pulsar_reader_configuration_create();
    pulsar_reader_t *reader;
    result =
        pulsar_client_create_reader(client, topic_name, pulsar_message_id_earliest(), reader_conf, &reader);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_message_t *seek_message = nullptr;

    for (int i = 0; i < 10; i++) {
        char content[10];
        sprintf(content, "msg-%d", i);
        pulsar_message_t *msg = pulsar_message_create();
        pulsar_message_set_content(msg, content, strlen(content));
        pulsar_producer_send(producer, msg);
        if (i == 5) {
            seek_message = msg;
        } else {
            pulsar_message_free(msg);
        }
    }

    pulsar_reader_seek(reader, pulsar_message_get_message_id(seek_message));

    pulsar_message_t *message;
    ASSERT_EQ(pulsar_result_Ok, pulsar_reader_read_next_with_timeout(reader, &message, 1000));
    ASSERT_STREQ((const char *)pulsar_message_get_data(message), "msg-6");
    pulsar_message_free(message);

    // Test seek asynchronously
    std::promise<pulsar_result> seek_promise;
    std::future<pulsar_result> seek_future = seek_promise.get_future();
    struct seek_ctx seek_ctx = {&seek_promise};
    pulsar_reader_seek_async(reader, pulsar_message_get_message_id(seek_message), seek_callback, &seek_ctx);
    ASSERT_EQ(pulsar_result_Ok, seek_future.get());
    ASSERT_EQ(pulsar_result_Ok, pulsar_reader_read_next_with_timeout(reader, &message, 1000));
    ASSERT_STREQ((const char *)pulsar_message_get_data(message), "msg-6");

    if (seek_message != NULL) {
        pulsar_message_free(seek_message);
    }
    pulsar_reader_free(reader);
    pulsar_reader_configuration_free(reader_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
}

TEST(c_SeekTest, testReaderSeekTime) {
    auto topic_name_str = "test-c-reader-seek-time-" + std::to_string(time(nullptr));
    const char *topic_name = topic_name_str.c_str();

    pulsar_client_t *client;
    prepare_client(&client);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_reader_configuration_t *reader_conf = pulsar_reader_configuration_create();
    pulsar_reader_t *reader;
    result =
        pulsar_client_create_reader(client, topic_name, pulsar_message_id_earliest(), reader_conf, &reader);
    ASSERT_EQ(pulsar_result_Ok, result);

    for (int i = 0; i < 10; i++) {
        char content[10];
        sprintf(content, "msg-%d", i);
        pulsar_message_t *msg = pulsar_message_create();
        pulsar_message_set_content(msg, content, strlen(content));
        pulsar_producer_send(producer, msg);
        pulsar_message_free(msg);
    }

    uint64_t currentTime = pulsar::TimeUtils::currentTimeMillis();

    pulsar_reader_seek_by_timestamp(reader, currentTime);

    pulsar_message_t *message;
    ASSERT_EQ(pulsar_result_Timeout, pulsar_reader_read_next_with_timeout(reader, &message, 1000));

    pulsar_reader_seek_by_timestamp(reader, currentTime - 100000);  // Seek to 100 seconds ago

    ASSERT_EQ(pulsar_result_Ok, pulsar_reader_read_next_with_timeout(reader, &message, 1000));
    ASSERT_STREQ((const char *)pulsar_message_get_data(message), "msg-0");

    // Test seek asynchronously
    std::promise<pulsar_result> seek_promise;
    std::future<pulsar_result> seek_future = seek_promise.get_future();
    struct seek_ctx seek_ctx = {&seek_promise};
    pulsar_reader_seek_by_timestamp_async(reader, currentTime, seek_callback, &seek_ctx);
    ASSERT_EQ(pulsar_result_Ok, seek_future.get());
    ASSERT_EQ(pulsar_result_Timeout, pulsar_reader_read_next_with_timeout(reader, &message, 1000));

    pulsar_reader_free(reader);
    pulsar_reader_configuration_free(reader_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
}

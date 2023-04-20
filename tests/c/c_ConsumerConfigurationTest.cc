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
#include <pulsar/c/consumer_configuration.h>

TEST(C_ConsumerConfigurationTest, testCApiConfig) {
    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();

    ASSERT_EQ(pulsar_consumer_configuration_get_max_pending_chunked_message(consumer_conf), 10);
    pulsar_consumer_configuration_set_max_pending_chunked_message(consumer_conf, 100);
    ASSERT_EQ(pulsar_consumer_configuration_get_max_pending_chunked_message(consumer_conf), 100);

    ASSERT_EQ(pulsar_consumer_configuration_is_auto_ack_oldest_chunked_message_on_queue_full(consumer_conf),
              0);
    pulsar_consumer_configuration_set_auto_ack_oldest_chunked_message_on_queue_full(consumer_conf, 1);
    ASSERT_EQ(pulsar_consumer_configuration_is_auto_ack_oldest_chunked_message_on_queue_full(consumer_conf),
              1);

    pulsar_consumer_configuration_set_start_message_id_inclusive(consumer_conf, 1);
    ASSERT_EQ(pulsar_consumer_configuration_is_start_message_id_inclusive(consumer_conf), 1);

    pulsar_consumer_configuration_set_batch_index_ack_enabled(consumer_conf, 1);
    ASSERT_EQ(pulsar_consumer_configuration_is_batch_index_ack_enabled(consumer_conf), 1);

    pulsar_consumer_configuration_set_regex_subscription_mode(
        consumer_conf, pulsar_consumer_regex_sub_mode_NonPersistentOnly);
    ASSERT_EQ(pulsar_consumer_configuration_get_regex_subscription_mode(consumer_conf),
              pulsar_consumer_regex_sub_mode_NonPersistentOnly);

    pulsar_consumer_batch_receive_policy_t batch_receive_policy;
    pulsar_consumer_configuration_get_batch_receive_policy(consumer_conf, &batch_receive_policy);
    ASSERT_EQ(batch_receive_policy.maxNumMessages, -1);
    ASSERT_EQ(batch_receive_policy.maxNumBytes, 10 * 1024 * 1024L);
    ASSERT_EQ(batch_receive_policy.timeoutMs, 100L);

    pulsar_consumer_batch_receive_policy_t new_batch_receive_policy{-1, -1, -1};
    ASSERT_EQ(-1, pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, NULL));
    ASSERT_EQ(
        -1, pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, &new_batch_receive_policy));

    new_batch_receive_policy.maxNumMessages = 100;
    ASSERT_EQ(
        0, pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, &new_batch_receive_policy));
    pulsar_consumer_configuration_get_batch_receive_policy(consumer_conf, &batch_receive_policy);
    ASSERT_EQ(batch_receive_policy.maxNumMessages, 100);

    new_batch_receive_policy.maxNumBytes = 100L * 1024 * 1024 * 1024;
    ASSERT_EQ(
        0, pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, &new_batch_receive_policy));
    pulsar_consumer_configuration_get_batch_receive_policy(consumer_conf, &batch_receive_policy);
    ASSERT_EQ(batch_receive_policy.maxNumBytes, 100L * 1024 * 1024 * 1024);

    new_batch_receive_policy.timeoutMs = 365L * 24 * 3600 * 1000;
    ASSERT_EQ(
        0, pulsar_consumer_configuration_set_batch_receive_policy(consumer_conf, &new_batch_receive_policy));
    pulsar_consumer_configuration_get_batch_receive_policy(consumer_conf, &batch_receive_policy);
    ASSERT_EQ(batch_receive_policy.timeoutMs, 365L * 24 * 3600 * 1000);

    pulsar_consumer_configuration_free(consumer_conf);
}

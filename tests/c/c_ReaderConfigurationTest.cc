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
#include <pulsar/c/reader_configuration.h>

#include <climits>

/*
  ASSERT_EQ(consumerConf.getConsumerType(), ConsumerExclusive);
    ASSERT_EQ(consumerConf.getReceiverQueueSize(), 1000);
    ASSERT_EQ(consumerConf.isReadCompacted(), false);
    ASSERT_EQ(consumerConf.getSchema().getName(), "BYTES");
    ASSERT_EQ(consumerConf.getUnAckedMessagesTimeoutMs(), 0);
    ASSERT_EQ(consumerConf.getTickDurationInMs(), 1000);
    ASSERT_EQ(consumerConf.getAckGroupingTimeMs(), 100);
    ASSERT_EQ(consumerConf.getAckGroupingMaxSize(), 1000);
    ASSERT_EQ(consumerConf.getCryptoKeyReader().get(), nullptr);
    ASSERT_EQ(consumerConf.getCryptoFailureAction(), ConsumerCryptoFailureAction::FAIL);
    ASSERT_TRUE(consumerConf.getProperties().empty());
    ASSERT_TRUE(consumerConf.getConsumerName().empty());
    ASSERT_FALSE(consumerConf.hasMessageListener());
 */

TEST(C_ReaderConfigurationTest, testCApiConfig) {
    pulsar_reader_configuration_t *reader_conf = pulsar_reader_configuration_create();

    ASSERT_FALSE(pulsar_reader_configuration_has_reader_listener(reader_conf));

    ASSERT_EQ(pulsar_reader_configuration_get_receiver_queue_size(reader_conf), 1000);
    pulsar_reader_configuration_set_receiver_queue_size(reader_conf, 1729);
    ASSERT_EQ(pulsar_reader_configuration_get_receiver_queue_size(reader_conf), 1729);

    ASSERT_STREQ(pulsar_reader_configuration_get_subscription_role_prefix(reader_conf), "");
    pulsar_reader_configuration_set_subscription_role_prefix(reader_conf, "prefix");
    ASSERT_STREQ(pulsar_reader_configuration_get_subscription_role_prefix(reader_conf), "prefix");

    ASSERT_STREQ(pulsar_reader_configuration_get_reader_name(reader_conf), "");
    pulsar_reader_configuration_set_reader_name(reader_conf, "reader");
    ASSERT_STREQ(pulsar_reader_configuration_get_reader_name(reader_conf), "reader");

    ASSERT_FALSE(pulsar_reader_configuration_is_read_compacted(reader_conf));
    pulsar_reader_configuration_set_read_compacted(reader_conf, true);
    ASSERT_TRUE(pulsar_reader_configuration_is_read_compacted(reader_conf));

    ASSERT_EQ(pulsar_reader_configuration_get_crypto_failure_action(reader_conf), pulsar_ConsumerFail);
    pulsar_reader_configuration_set_crypto_failure_action(reader_conf, pulsar_ConsumerDiscard);
    ASSERT_EQ(pulsar_reader_configuration_get_crypto_failure_action(reader_conf), pulsar_ConsumerDiscard);

    pulsar_reader_configuration_free(reader_conf);
}

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

#include <pulsar/DeadLetterPolicyBuilder.h>
#include <pulsar/c/consumer.h>
#include <pulsar/c/consumer_configuration.h>

#include <climits>

#include "c_structs.h"

pulsar_consumer_configuration_t *pulsar_consumer_configuration_create() {
    return new pulsar_consumer_configuration_t;
}

void pulsar_consumer_configuration_free(pulsar_consumer_configuration_t *consumer_configuration) {
    delete consumer_configuration;
}

void pulsar_consumer_configuration_set_consumer_type(pulsar_consumer_configuration_t *consumer_configuration,
                                                     pulsar_consumer_type consumerType) {
    consumer_configuration->consumerConfiguration.setConsumerType((pulsar::ConsumerType)consumerType);
}

pulsar_consumer_type pulsar_consumer_configuration_get_consumer_type(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return (pulsar_consumer_type)consumer_configuration->consumerConfiguration.getConsumerType();
}

void pulsar_consumer_configuration_set_schema_info(pulsar_consumer_configuration_t *consumer_configuration,
                                                   pulsar_schema_type schemaType, const char *name,
                                                   const char *schema, pulsar_string_map_t *properties) {
    auto schemaInfo = pulsar::SchemaInfo((pulsar::SchemaType)schemaType, name, schema, properties->map);
    consumer_configuration->consumerConfiguration.setSchema(schemaInfo);
}

static void message_listener_callback(pulsar::Consumer consumer, const pulsar::Message &msg,
                                      pulsar_message_listener listener, void *ctx) {
    pulsar_consumer_t c_consumer;
    c_consumer.consumer = consumer;
    pulsar_message_t *message = new pulsar_message_t;
    message->message = msg;
    listener(&c_consumer, message, ctx);
}

void pulsar_consumer_configuration_set_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_message_listener messageListener,
    void *ctx) {
    consumer_configuration->consumerConfiguration.setMessageListener(std::bind(
        message_listener_callback, std::placeholders::_1, std::placeholders::_2, messageListener, ctx));
}

int pulsar_consumer_configuration_has_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.hasMessageListener();
}

void pulsar_consumer_configuration_set_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration, int size) {
    consumer_configuration->consumerConfiguration.setReceiverQueueSize(size);
}

int pulsar_consumer_configuration_get_receiver_queue_size(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getReceiverQueueSize();
}

void pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration, int maxTotalReceiverQueueSizeAcrossPartitions) {
    consumer_configuration->consumerConfiguration.setMaxTotalReceiverQueueSizeAcrossPartitions(
        maxTotalReceiverQueueSizeAcrossPartitions);
}

int pulsar_consumer_get_max_total_receiver_queue_size_across_partitions(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getMaxTotalReceiverQueueSizeAcrossPartitions();
}

void pulsar_consumer_set_consumer_name(pulsar_consumer_configuration_t *consumer_configuration,
                                       const char *consumerName) {
    consumer_configuration->consumerConfiguration.setConsumerName(consumerName);
}

const char *pulsar_consumer_get_consumer_name(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getConsumerName().c_str();
}

void pulsar_consumer_set_unacked_messages_timeout_ms(pulsar_consumer_configuration_t *consumer_configuration,
                                                     const uint64_t milliSeconds) {
    consumer_configuration->consumerConfiguration.setUnAckedMessagesTimeoutMs(milliSeconds);
}

long pulsar_consumer_get_unacked_messages_timeout_ms(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getUnAckedMessagesTimeoutMs();
}

void pulsar_configure_set_negative_ack_redelivery_delay_ms(
    pulsar_consumer_configuration_t *consumer_configuration, long redeliveryDelayMillis) {
    consumer_configuration->consumerConfiguration.setNegativeAckRedeliveryDelayMs(redeliveryDelayMillis);
}

long pulsar_configure_get_negative_ack_redelivery_delay_ms(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getNegativeAckRedeliveryDelayMs();
}

void pulsar_configure_set_ack_grouping_time_ms(pulsar_consumer_configuration_t *consumer_configuration,
                                               long ackGroupingMillis) {
    consumer_configuration->consumerConfiguration.setAckGroupingTimeMs(ackGroupingMillis);
}

long pulsar_configure_get_ack_grouping_time_ms(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getAckGroupingTimeMs();
}

void pulsar_configure_set_ack_grouping_max_size(pulsar_consumer_configuration_t *consumer_configuration,
                                                long maxGroupingSize) {
    consumer_configuration->consumerConfiguration.setAckGroupingMaxSize(maxGroupingSize);
}

long pulsar_configure_get_ack_grouping_max_size(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getAckGroupingMaxSize();
}

int pulsar_consumer_is_encryption_enabled(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isEncryptionEnabled();
}

void pulsar_consumer_configuration_set_default_crypto_key_reader(
    pulsar_consumer_configuration_t *consumer_configuration, const char *public_key_path,
    const char *private_key_path) {
    std::shared_ptr<pulsar::DefaultCryptoKeyReader> keyReader =
        std::make_shared<pulsar::DefaultCryptoKeyReader>(public_key_path, private_key_path);
    consumer_configuration->consumerConfiguration.setCryptoKeyReader(keyReader);
}

pulsar_consumer_crypto_failure_action pulsar_consumer_configuration_get_crypto_failure_action(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return (pulsar_consumer_crypto_failure_action)
        consumer_configuration->consumerConfiguration.getCryptoFailureAction();
}

void pulsar_consumer_configuration_set_crypto_failure_action(
    pulsar_consumer_configuration_t *consumer_configuration,
    pulsar_consumer_crypto_failure_action cryptoFailureAction) {
    consumer_configuration->consumerConfiguration.setCryptoFailureAction(
        (pulsar::ConsumerCryptoFailureAction)cryptoFailureAction);
}

int pulsar_consumer_is_read_compacted(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isReadCompacted();
}

void pulsar_consumer_set_read_compacted(pulsar_consumer_configuration_t *consumer_configuration,
                                        int compacted) {
    consumer_configuration->consumerConfiguration.setReadCompacted(compacted);
}

void pulsar_consumer_configuration_set_property(pulsar_consumer_configuration_t *conf, const char *name,
                                                const char *value) {
    conf->consumerConfiguration.setProperty(name, value);
}

void pulsar_consumer_set_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration, initial_position subscriptionInitialPosition) {
    consumer_configuration->consumerConfiguration.setSubscriptionInitialPosition(
        (pulsar::InitialPosition)subscriptionInitialPosition);
}

int pulsar_consumer_get_subscription_initial_position(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getSubscriptionInitialPosition();
}

void pulsar_consumer_configuration_set_priority_level(pulsar_consumer_configuration_t *consumer_configuration,
                                                      int priority_level) {
    consumer_configuration->consumerConfiguration.setPriorityLevel(priority_level);
}

int pulsar_consumer_configuration_get_priority_level(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getPriorityLevel();
}

void pulsar_consumer_configuration_set_max_pending_chunked_message(
    pulsar_consumer_configuration_t *consumer_configuration, int max_pending_chunked_message) {
    consumer_configuration->consumerConfiguration.setMaxPendingChunkedMessage(max_pending_chunked_message);
}

int pulsar_consumer_configuration_get_max_pending_chunked_message(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.getMaxPendingChunkedMessage();
}

void pulsar_consumer_configuration_set_auto_ack_oldest_chunked_message_on_queue_full(
    pulsar_consumer_configuration_t *consumer_configuration,
    int auto_ack_oldest_chunked_message_on_queue_full) {
    consumer_configuration->consumerConfiguration.setAutoAckOldestChunkedMessageOnQueueFull(
        auto_ack_oldest_chunked_message_on_queue_full);
}

int pulsar_consumer_configuration_is_auto_ack_oldest_chunked_message_on_queue_full(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isAutoAckOldestChunkedMessageOnQueueFull();
}

void pulsar_consumer_configuration_set_start_message_id_inclusive(
    pulsar_consumer_configuration_t *consumer_configuration, int start_message_id_inclusive) {
    consumer_configuration->consumerConfiguration.setStartMessageIdInclusive(start_message_id_inclusive);
}

int pulsar_consumer_configuration_is_start_message_id_inclusive(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isStartMessageIdInclusive();
}

void pulsar_consumer_configuration_set_batch_index_ack_enabled(
    pulsar_consumer_configuration_t *consumer_configuration, int enabled) {
    consumer_configuration->consumerConfiguration.setBatchIndexAckEnabled(enabled);
}

int pulsar_consumer_configuration_is_batch_index_ack_enabled(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isBatchIndexAckEnabled();
}

void pulsar_consumer_configuration_set_regex_subscription_mode(
    pulsar_consumer_configuration_t *consumer_configuration,
    pulsar_consumer_regex_subscription_mode regex_sub_mode) {
    consumer_configuration->consumerConfiguration.setRegexSubscriptionMode(
        (pulsar::RegexSubscriptionMode)regex_sub_mode);
}

pulsar_consumer_regex_subscription_mode pulsar_consumer_configuration_get_regex_subscription_mode(
    pulsar_consumer_configuration_t *consumer_configuration) {
    return (pulsar_consumer_regex_subscription_mode)
        consumer_configuration->consumerConfiguration.getRegexSubscriptionMode();
}

void pulsar_consumer_configuration_set_start_paused(pulsar_consumer_configuration_t *consumer_configuration,
                                                    int start_paused) {
    consumer_configuration->consumerConfiguration.setStartPaused(start_paused);
}

int pulsar_consumer_configuration_is_start_paused(pulsar_consumer_configuration_t *consumer_configuration) {
    return consumer_configuration->consumerConfiguration.isStartPaused();
}

int pulsar_consumer_configuration_set_batch_receive_policy(
    pulsar_consumer_configuration_t *consumer_configuration,
    const pulsar_consumer_batch_receive_policy_t *batch_receive_policy_t) {
    if (!batch_receive_policy_t) {
        return -1;
    }
    if (batch_receive_policy_t->maxNumMessages <= 0 && batch_receive_policy_t->maxNumBytes <= 0 &&
        batch_receive_policy_t->timeoutMs <= 0) {
        return -1;
    }
    pulsar::BatchReceivePolicy batchReceivePolicy(batch_receive_policy_t->maxNumMessages,
                                                  batch_receive_policy_t->maxNumBytes,
                                                  batch_receive_policy_t->timeoutMs);
    consumer_configuration->consumerConfiguration.setBatchReceivePolicy(batchReceivePolicy);
    return 0;
}

void pulsar_consumer_configuration_get_batch_receive_policy(
    pulsar_consumer_configuration_t *consumer_configuration, pulsar_consumer_batch_receive_policy_t *policy) {
    if (!policy) {
        return;
    }
    pulsar::BatchReceivePolicy batchReceivePolicy =
        consumer_configuration->consumerConfiguration.getBatchReceivePolicy();
    policy->maxNumMessages = batchReceivePolicy.getMaxNumMessages();
    policy->maxNumBytes = batchReceivePolicy.getMaxNumBytes();
    policy->timeoutMs = batchReceivePolicy.getTimeoutMs();
}

void pulsar_consumer_configuration_set_dlq_policy(
    pulsar_consumer_configuration_t *consumer_configuration,
    const pulsar_consumer_config_dead_letter_policy_t *dlq_policy) {
    auto dlqPolicyBuilder =
        pulsar::DeadLetterPolicyBuilder().maxRedeliverCount(dlq_policy->max_redeliver_count);
    if (dlq_policy->dead_letter_topic != nullptr) {
        dlqPolicyBuilder.deadLetterTopic(dlq_policy->dead_letter_topic);
    }
    if (dlq_policy->initial_subscription_name != nullptr) {
        dlqPolicyBuilder.initialSubscriptionName(dlq_policy->initial_subscription_name);
    }
    if (dlq_policy->max_redeliver_count <= 0) {
        dlqPolicyBuilder.maxRedeliverCount(INT_MAX);
    }
    consumer_configuration->consumerConfiguration.setDeadLetterPolicy(dlqPolicyBuilder.build());
}

void pulsar_consumer_configuration_get_dlq_policy(pulsar_consumer_configuration_t *consumer_configuration,
                                                  pulsar_consumer_config_dead_letter_policy_t *dlq_policy) {
    if (!dlq_policy) {
        return;
    }
    auto deadLetterPolicy = consumer_configuration->consumerConfiguration.getDeadLetterPolicy();
    dlq_policy->dead_letter_topic = deadLetterPolicy.getDeadLetterTopic().c_str();
    dlq_policy->max_redeliver_count = deadLetterPolicy.getMaxRedeliverCount();
    dlq_policy->initial_subscription_name = deadLetterPolicy.getInitialSubscriptionName().c_str();
}

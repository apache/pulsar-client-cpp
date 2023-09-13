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
#include <pulsar/c/table_view_configuration.h>

#include "c_structs.h"

pulsar_table_view_configuration_t *pulsar_table_view_configuration_create() {
    auto *table_view_configuration_t = new pulsar_table_view_configuration_t;
    table_view_configuration_t->tableViewConfiguration = pulsar::TableViewConfiguration();
    return table_view_configuration_t;
}

void pulsar_table_view_configuration_free(pulsar_table_view_configuration_t *conf) { delete conf; }

void pulsar_table_view_configuration_set_schema_info(
    pulsar_table_view_configuration_t *table_view_configuration_t, pulsar_schema_type schemaType,
    const char *name, const char *schema, pulsar_string_map_t *properties) {
    auto schemaInfo = pulsar::SchemaInfo((pulsar::SchemaType)schemaType, name, schema, properties->map);
    table_view_configuration_t->tableViewConfiguration.schemaInfo = schemaInfo;
}

void pulsar_table_view_configuration_set_subscription_name(
    pulsar_table_view_configuration_t *table_view_configuration_t, const char *subscription_name) {
    table_view_configuration_t->tableViewConfiguration.subscriptionName = subscription_name;
}

const char *pulsar_table_view_configuration_get_subscription_name(
    pulsar_table_view_configuration_t *table_view_configuration_t) {
    return table_view_configuration_t->tableViewConfiguration.subscriptionName.c_str();
}

void pulsar_table_view_configuration_set_default_crypto_key_reader(
    pulsar_table_view_configuration_t *table_view_configuration_t, const char *public_key_path, const char *private_key_path) {
    std::shared_ptr<pulsar::DefaultCryptoKeyReader> keyReader = std::make_shared<pulsar::DefaultCryptoKeyReader>(public_key_path, private_key_path);
    table_view_configuration_t->tableViewConfiguration.cryptoKeyReader = keyReader;
}

pulsar_consumer_crypto_failure_action pulsar_table_view_configuration_get_crypto_failure_action(
    pulsar_table_view_configuration_t *table_view_configuration_t) {
    return (pulsar_consumer_crypto_failure_action) table_view_configuration_t->tableViewConfiguration.cryptoFailureAction;
}

void pulsar_table_view_configuration_set_crypto_failure_action(
    pulsar_table_view_configuration_t *table_view_configuration_t, pulsar_consumer_crypto_failure_action crypto_failure_action) {
    table_view_configuration_t->tableViewConfiguration.cryptoFailureAction = (pulsar::ConsumerCryptoFailureAction)crypto_failure_action;
}

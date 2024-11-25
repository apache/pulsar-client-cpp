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

#include "pulsar/c/client_configuration.h"

TEST(C_ClientConfigurationTest, testCApiConfig) {
    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();

    pulsar_client_configuration_set_tls_private_key_file_path(conf, "private.key");
    pulsar_client_configuration_set_tls_certificate_file_path(conf, "certificate.pem");

    ASSERT_STREQ(pulsar_client_configuration_get_tls_private_key_file_path(conf), "private.key");
    ASSERT_STREQ(pulsar_client_configuration_get_tls_certificate_file_path(conf), "certificate.pem");

    pulsar_client_configuration_set_listener_name(conf, "listenerName");
    ASSERT_STREQ(pulsar_client_configuration_get_listener_name(conf), "listenerName");

    pulsar_client_configuration_set_partitions_update_interval(conf, 10);
    ASSERT_EQ(pulsar_client_configuration_get_partitions_update_interval(conf), 10);

    pulsar_client_configuration_set_keep_alive_interval_in_seconds(conf, 60);
    ASSERT_EQ(pulsar_client_configuration_get_keep_alive_interval_in_seconds(conf), 60);
}

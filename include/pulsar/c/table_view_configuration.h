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

#pragma once

#include <pulsar/defines.h>

#include "producer_configuration.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_table_view_configuration pulsar_table_view_configuration_t;

PULSAR_PUBLIC pulsar_table_view_configuration_t *pulsar_table_view_configuration_create();

PULSAR_PUBLIC void pulsar_table_view_configuration_free(pulsar_table_view_configuration_t *conf);

PULSAR_PUBLIC void pulsar_table_view_configuration_set_schema_info(
    pulsar_table_view_configuration_t *table_view_configuration_t, pulsar_schema_type schemaType,
    const char *name, const char *schema, pulsar_string_map_t *properties);

PULSAR_PUBLIC void pulsar_table_view_configuration_set_subscription_name(
    pulsar_table_view_configuration_t *table_view_configuration_t, const char *subscription_name);

PULSAR_PUBLIC const char *pulsar_table_view_configuration_get_subscription_name(
    pulsar_table_view_configuration_t *table_view_configuration_t);

#ifdef __cplusplus
}
#endif

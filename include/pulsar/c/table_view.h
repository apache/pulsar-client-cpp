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

#ifdef __cplusplus
extern "C" {
#endif

#include <pulsar/c/message.h>
#include <pulsar/c/messages.h>
#include <pulsar/c/result.h>
#include <stdint.h>

typedef struct _pulsar_table_view pulsar_table_view_t;

typedef void (*pulsar_table_view_action)(const char *key, const char *value, void *ctx);
typedef void (*pulsar_result_callback)(pulsar_result, void *);

/**
 * Move the latest value associated with the key.
 *
 * NOTE:
 * 1. Once the value has been retrieved successfully,
 * the associated value will be removed from the table view until next time the value is updated.
 * 2. Once the value has been retrieved successfully, `*value` will point to the memory that is allocated
 * internally. You have to call `delete value` to free it.
 *
 * Example:
 *
 * ```c
 * pulsar_table_view_t *table_view;
 * char *value;
 * while (true) {
 *     if (pulsar_table_view_retrieve_value(table_view, "key", &value)) {
 *         printf("value is update to: %s", value);
 *     } else {
 *         // sleep for a while or print the message that value is not updated
 *     }
 * }
 * delete value;
 * ```
 *
 * @param table_view
 * @param key
 * @param value the value associated with the key
 * @return true if there is an associated value of the key, otherwise false
 */
PULSAR_PUBLIC bool pulsar_table_view_retrieve_value(pulsar_table_view_t *table_view, const char *key,
                                                    char **value);

/**
 * It's similar with `pulsar_table_view_retrieve_value` except the associated value not will be removed from
 * the table view.
 *
 * NOTE:
 * Once the value has been get successfully, `*value` will point to the memory that is allocated internally.
 * You have to call `delete value` to free it.
 *
 * @param table_view
 * @param key
 * @param value the value associated with the key
 * @return true if there is an associated value of the key, otherwise false
 */
PULSAR_PUBLIC bool pulsar_table_view_get_value(pulsar_table_view_t *table_view, const char *key,
                                               char **value);

/**
 * Check if the key exists in the table view.
 * @param table_view
 * @param key
 * @return true if the key exists in the table view
 */
PULSAR_PUBLIC bool pulsar_table_view_contain_key(pulsar_table_view_t *table_view, const char *key);

/**
 * Get the size of the elements.
 * @param table_view
 * @return
 */
PULSAR_PUBLIC int pulsar_table_view_size(pulsar_table_view_t *table_view);

/**
 * Performs the given action for each entry in this map until all entries have been processed or the
 * action throws an exception.
 */
PULSAR_PUBLIC void pulsar_table_view_for_each(pulsar_table_view_t *table_view,
                                              pulsar_table_view_action action, void *ctx);

/**
 * Performs the given action for each entry in this map until all entries have been processed and
 * register the callback, which will be called each time a key-value pair is updated.
 */
PULSAR_PUBLIC void pulsar_table_view_for_each_add_listen(pulsar_table_view_t *table_view,
                                                         pulsar_table_view_action action, void *ctx);

/**
 * Move the table view data into the pulsar_string_map_t.
 *
 * @param table_view
 * @return *string_map `string_map` will point to the memory that is allocated internally.
 * You have to call `pulsar_string_map_free` to free it.
 */
PULSAR_PUBLIC pulsar_string_map_t *pulsar_table_view_snapshot(pulsar_table_view_t *table_view);

/**
 * Free the table view.
 * @param table_view
 */
PULSAR_PUBLIC void pulsar_table_view_free(pulsar_table_view_t *table_view);

/**
 * Close the table view and stop the broker to push more messages
 * @param table_view
 * @return
 */
PULSAR_PUBLIC pulsar_result pulsar_table_view_close(pulsar_table_view_t *table_view);

/**
 * Async close the table view and stop the broker to push more messages
 * @param table_view
 * @param callback
 * @param ctx
 */
PULSAR_PUBLIC void pulsar_table_view_close_async(pulsar_table_view_t *table_view,
                                                 pulsar_result_callback callback, void *ctx);

#ifdef __cplusplus
}
#endif

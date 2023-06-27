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

#include <pulsar/c/table_view.h>

#include "c_structs.h"
#include "cstring"

char *str_malloc_and_copy(const char *s) {
    size_t slen = strlen(s);
    char *result = (char *)malloc(slen + 1);
    if (result == NULL) {
        return NULL;
    }
    memcpy(result, s, slen + 1);
    return result;
}

bool pulsar_table_view_retrieve_value(pulsar_table_view_t *table_view, const char *key, char **value) {
    std::string v;
    bool result = table_view->tableView.retrieveValue(key, v);
    if (result) {
        *value = str_malloc_and_copy(v.c_str());
    }
    return result;
}

bool pulsar_table_view_get_value(pulsar_table_view_t *table_view, const char *key, char **value) {
    std::string v;
    bool result = table_view->tableView.getValue(key, v);
    if (result) {
        *value = str_malloc_and_copy(v.c_str());
    }
    return result;
}

bool pulsar_table_view_contain_key(pulsar_table_view_t *table_view, const char *key) {
    return table_view->tableView.containsKey(key);
}

int pulsar_table_view_size(pulsar_table_view_t *table_view) { return table_view->tableView.size(); }

void pulsar_table_view_for_each(pulsar_table_view_t *table_view, pulsar_table_view_action action, void *ctx) {
    table_view->tableView.forEach([action, ctx](const std::string &key, const std::string &value) {
        if (action) {
            action(key.c_str(), value.c_str(), ctx);
        }
    });
}

void pulsar_table_view_for_each_add_listen(pulsar_table_view_t *table_view, pulsar_table_view_action action,
                                           void *ctx) {
    table_view->tableView.forEachAndListen([action, ctx](const std::string &key, const std::string &value) {
        if (action) {
            action(key.c_str(), value.c_str(), ctx);
        }
    });
}

pulsar_string_map_t *pulsar_table_view_snapshot(pulsar_table_view_t *table_view) {
    auto map = pulsar_string_map_create();
    auto snapshot = table_view->tableView.snapshot();
    for (const auto &item : snapshot) {
        map->map.emplace(item);
    }
    return map;
}

void pulsar_table_view_free(pulsar_table_view_t *table_view) { delete table_view; }

pulsar_result pulsar_table_view_close(pulsar_table_view_t *table_view) {
    return (pulsar_result)table_view->tableView.close();
}

void pulsar_table_view_close_async(pulsar_table_view_t *table_view, pulsar_result_callback callback,
                                   void *ctx) {
    table_view->tableView.closeAsync(
        [callback, ctx](pulsar::Result result) { return handle_result_callback(result, callback, ctx); });
}

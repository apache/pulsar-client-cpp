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

#include <pulsar/c/message.h>
#include <pulsar/defines.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_messages pulsar_messages_t;

/**
 * Get the number of messages.
 *
 * NOTE: Undefined behavior will happen if `msgs` is NULL.
 */
PULSAR_PUBLIC size_t pulsar_messages_size(pulsar_messages_t* msgs);

/**
 * Get the Nth message according to the given index.
 *
 * NOTE:
 * 1. You should not free the returned pointer, which always points to a valid memory unless `msgs` is freed.
 * 2. Undefined behavior will happen if `msgs` is NULL or `index` is not smaller than the number of messages.
 */
PULSAR_PUBLIC pulsar_message_t* pulsar_messages_get(pulsar_messages_t* msgs, size_t index);

PULSAR_PUBLIC void pulsar_messages_free(pulsar_messages_t* msgs);

#ifdef __cplusplus
}
#endif

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

#ifdef __cplusplus
extern "C" {
#endif

#include <pulsar/defines.h>

typedef struct _pulsar_messages pulsar_messages_t;

/**
 * Create the messages.
 * @return messages
 */
PULSAR_PUBLIC pulsar_messages_t *pulsar_messages_create();

/**
 * Free the messages.
 */
PULSAR_PUBLIC void pulsar_messages_free(pulsar_messages_t *messages);

/**
 * Get the size of the messages.
 * @return the size of messages.
 */
PULSAR_PUBLIC int pulsar_messages_size(pulsar_messages_t *messages);

/**
 * Append message to messages.
 * @param message the pointer of message
 */
PULSAR_PUBLIC void pulsar_messages_append(pulsar_messages_t *messages, const pulsar_message_t *message);

/**
 * Get message by index.
 * @param index the index of messages
 * @return
 */
PULSAR_PUBLIC pulsar_message_t *pulsar_messages_get(pulsar_messages_t *messages, int index);

#ifdef __cplusplus
}
#endif

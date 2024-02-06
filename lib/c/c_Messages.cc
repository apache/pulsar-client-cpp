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
#include <pulsar/c/messages.h>

#include "lib/c/c_structs.h"

size_t pulsar_messages_size(pulsar_messages_t* msgs) { return msgs->messages.size(); }

pulsar_message_t* pulsar_messages_get(pulsar_messages_t* msgs, size_t index) {
    return &msgs->messages[index];
}

void pulsar_messages_free(pulsar_messages_t* msgs) { delete msgs; }

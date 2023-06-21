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
#include <lib/c/c_structs.h>
#include <pulsar/c/message.h>

TEST(c_MessageTest, MessageCopy) {
    pulsar_message_t *from = pulsar_message_create();
    pulsar_message_set_content(from, "hello", 5);
    from->message = from->builder.build();
    pulsar_message_t *to = pulsar_message_create();

    pulsar_message_copy(from, to);
    ASSERT_STREQ((const char *)pulsar_message_get_data(to), (const char *)pulsar_message_get_data(from));

    pulsar_message_free(from);
    pulsar_message_free(to);
}

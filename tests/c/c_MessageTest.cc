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
#include <pulsar/Message.h>
#include <pulsar/MessageBuilder.h>
#include <pulsar/c/message.h>

struct _pulsar_message {
    pulsar::MessageBuilder builder;
    pulsar::Message message;
};

TEST(c_MessageTest, MessageCopy) {
    pulsar_message_t *from = pulsar_message_create();
    pulsar_message_set_content(from, "hello", 5);
    from->message = from->builder.build();
    std::cout << "from: " << (const char *)pulsar_message_get_data(from) << std::endl;

    pulsar_message_t *to = pulsar_message_create();
    pulsar_message_copy(from, to);
    pulsar_message_free(from);

    std::cout << "to: " << (const char *)pulsar_message_get_data(to) << std::endl;
    ASSERT_STREQ((const char *)pulsar_message_get_data(to), "hello");

    pulsar_message_free(to);
}

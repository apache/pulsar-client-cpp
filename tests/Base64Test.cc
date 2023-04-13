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

#include <cstring>

#include "lib/Base64Utils.h"

using namespace pulsar;

TEST(Base64Test, testJsonEncodeDecode) {
    const std::string s1 = R"("{"key":"value"}")";
    const auto s2 = base64::decode(base64::encode(s1));
    ASSERT_EQ(s1, s2);
}

TEST(Base64Test, testPaddings) {
    auto encode = [](const char* s) { return base64::encode(s, strlen(s)); };
    ASSERT_EQ(encode("x"), "eA==");    // 2 paddings
    ASSERT_EQ(encode("xy"), "eHk=");   // 1 padding
    ASSERT_EQ(encode("xyz"), "eHl6");  // 0 padding
}

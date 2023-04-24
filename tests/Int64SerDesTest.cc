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

#include "lib/Int64SerDes.h"

using namespace pulsar;

TEST(Int64SerDes, testNormal) {
    int64_t x = 0x0102030405060708L;
    const auto bytes = toBigEndianBytes(x);
    ASSERT_EQ(bytes.size(), 8);
    for (int i = 0; i < 8; i++) {
        ASSERT_EQ(bytes[i], i + 1);
    }
    int64_t y = fromBigEndianBytes(bytes.data());
    ASSERT_EQ(x, y);
}

TEST(Int64SerDes, testOverflow) {
    int64_t x = 0x8000000000000000L;
    const auto bytes = toBigEndianBytes(x);
    ASSERT_EQ(bytes.size(), 8);
    ASSERT_EQ(static_cast<unsigned char>(bytes[0]), 0x80);
    for (int i = 1; i < 8; i++) {
        ASSERT_EQ(bytes[i], 0x00);
    }
    int64_t y = fromBigEndianBytes(bytes);
    ASSERT_EQ(x, y);
}

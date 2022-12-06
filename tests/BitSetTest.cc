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

#include <map>
#include <vector>

#include "lib/BatchMessageAcker.h"
#include "lib/BitSet.h"

using namespace pulsar;

static std::vector<uint64_t> toLongVector(const BitSet& bitSet) {
    std::vector<uint64_t> v;
    for (uint64_t x : bitSet) {
        v.emplace_back(x);
    }
    return v;
}

TEST(BitSetTest, testFill) {
    // An int64_t has 64 bits, so we test 64*N + {-1, 0, 1}
    std::map<int, std::vector<uint64_t>> expectedResults;
    expectedResults[7] = {0x7f};
    expectedResults[63] = {0x7fffffffffffffff};
    expectedResults[64] = {0xffffffffffffffff};
    expectedResults[65] = {0xffffffffffffffff, 1};
    expectedResults[127] = {0xffffffffffffffff, 0x7fffffffffffffff};
    expectedResults[128] = {0xffffffffffffffff, 0xffffffffffffffff};
    expectedResults[129] = {0xffffffffffffffff, 0xffffffffffffffff, 1};

    std::map<int, std::vector<uint64_t>> actualResults;
    for (const auto& kv : expectedResults) {
        BitSet bitSet(kv.first);
        ASSERT_TRUE(toLongVector(bitSet).empty());
        bitSet.set(0, kv.first);
        actualResults[kv.first] = toLongVector(bitSet);
    }
    ASSERT_EQ(actualResults, expectedResults);
}

TEST(BitSetTest, testSet) {
    BitSet bitSet(64 * 5 + 1);  // 6 words
    ASSERT_TRUE(toLongVector(bitSet).empty());

    // range contains one word
    bitSet.set(3, 29);
    ASSERT_EQ(toLongVector(bitSet), std::vector<uint64_t>{0x1ffffff8});

    // range contains multiple words
    bitSet.set(64 * 2 + 11, 64 * 4 + 19);
    ASSERT_EQ(toLongVector(bitSet),
              (std::vector<uint64_t>{0x1ffffff8, 0, 0xfffffffffffff800, 0xffffffffffffffff, 0x7ffff}));
}

TEST(BitSetTest, testRangeClear) {
    BitSet bitSet(64 * 5 + 1);  // 6 words
    bitSet.set(0, 64 * 5 + 1);
    ASSERT_EQ(toLongVector(bitSet),
              (std::vector<uint64_t>{0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff,
                                     0xffffffffffffffff, 0xffffffffffffffff, 1}));

    // range contains one word
    bitSet.clear(64 * 5, 64 * 5 + 1);
    ASSERT_EQ(toLongVector(bitSet),
              (std::vector<uint64_t>{0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff,
                                     0xffffffffffffffff, 0xffffffffffffffff}));

    // range contains multiple words
    bitSet.clear(64 * 2 + 13, 64 * 5);
    ASSERT_EQ(toLongVector(bitSet), (std::vector<uint64_t>{0xffffffffffffffff, 0xffffffffffffffff, 0x1fff}));
}

TEST(BitSetTest, testSingleClear) {
    BitSet bitSet(64 * 2 + 1);  // 3 words
    bitSet.set(0, 64 * 2 + 1);
    ASSERT_EQ(toLongVector(bitSet), (std::vector<uint64_t>{0xffffffffffffffff, 0xffffffffffffffff, 1}));

    // words in use shrinked
    bitSet.clear(64 * 2);
    ASSERT_EQ(toLongVector(bitSet), (std::vector<uint64_t>{0xffffffffffffffff, 0xffffffffffffffff}));

    // words in use doesn't change
    bitSet.clear(13);
    ASSERT_EQ(toLongVector(bitSet), (std::vector<uint64_t>{0xffffffffffffdfff, 0xffffffffffffffff}));
}

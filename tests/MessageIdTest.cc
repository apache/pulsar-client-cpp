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
#include <pulsar/MessageIdBuilder.h>

#include <string>

#include "PulsarFriend.h"
#include "lib/MessageIdUtil.h"

using namespace pulsar;

TEST(MessageIdTest, testSerialization) {
    auto msgId = MessageIdBuilder().ledgerId(1L).entryId(2L).batchIndex(3L).build();

    std::string serialized;
    msgId.serialize(serialized);

    MessageId deserialized = MessageId::deserialize(serialized);

    ASSERT_EQ(msgId, deserialized);
}

TEST(MessageIdTest, testCompareLedgerAndEntryId) {
    auto id1 = MessageIdBuilder().ledgerId(2L).entryId(1L).batchIndex(0).build();
    auto id2 = MessageIdBuilder::from(id1).batchIndex(1).build();
    auto id3 = MessageIdBuilder().ledgerId(2L).entryId(2L).batchIndex(0).build();
    auto id4 = MessageIdBuilder().ledgerId(3L).entryId(0L).batchIndex(0).build();
    ASSERT_EQ(compareLedgerAndEntryId(id1, id2), 0);
    ASSERT_EQ(compareLedgerAndEntryId(id1, id2), 0);

    ASSERT_EQ(compareLedgerAndEntryId(id1, id3), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id3, id1), 1);

    ASSERT_EQ(compareLedgerAndEntryId(id1, id4), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id4, id1), 1);

    ASSERT_EQ(compareLedgerAndEntryId(id2, id4), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id4, id2), 1);

    ASSERT_EQ(compareLedgerAndEntryId(id3, id4), -1);
    ASSERT_EQ(compareLedgerAndEntryId(id4, id3), 1);
}

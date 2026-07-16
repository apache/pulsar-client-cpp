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
#include <pulsar/st/MessageId.h>

#include <cstddef>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "lib/st/MessageIdImpl.h"

using namespace pulsar::st;

namespace {

MessageId makeId(std::int64_t segmentId, std::int64_t ledgerId, std::int64_t entryId,
                 std::int32_t batchIndex = -1, std::int32_t partition = -1) {
    return MessageIdFactory::create(pulsar::MessageId(partition, ledgerId, entryId, batchIndex), segmentId);
}

}  // namespace

TEST(StMessageIdTest, testSentinels) {
    ASSERT_TRUE(static_cast<bool>(MessageId::earliest()));
    ASSERT_TRUE(static_cast<bool>(MessageId::latest()));
    ASSERT_TRUE(MessageId::earliest() == MessageId::earliest());
    ASSERT_FALSE(MessageId::earliest() == MessageId::latest());
    ASSERT_TRUE(MessageId::earliest() < MessageId::latest());
}

TEST(StMessageIdTest, testOrderingWithinAndAcrossSegments) {
    MessageId a = makeId(1, 10, 1);
    MessageId b = makeId(1, 10, 2);
    MessageId c = makeId(2, 5, 0);

    // Same segment: classic id order decides.
    ASSERT_TRUE(a < b);
    ASSERT_TRUE(b > a);
    // Different segments: segment id decides, regardless of ledger/entry.
    ASSERT_TRUE(b < c);
    ASSERT_TRUE(a <= a);
    ASSERT_TRUE(a == makeId(1, 10, 1));
    ASSERT_FALSE(a == b);
    ASSERT_FALSE(a == c);
}

TEST(StMessageIdTest, testEmptyIdComparisons) {
    MessageId empty1;
    MessageId empty2;
    MessageId real = makeId(0, 1, 1);
    ASSERT_TRUE(empty1 == empty2);
    ASSERT_FALSE(empty1 == real);
    ASSERT_TRUE(empty1 < real);  // empty sorts first
}

TEST(StMessageIdTest, testHashSupportsUnorderedContainers) {
    std::unordered_set<MessageId> ids;
    ids.insert(makeId(1, 10, 1));
    ids.insert(makeId(1, 10, 2));
    ids.insert(makeId(2, 10, 1));
    ids.insert(makeId(1, 10, 1));  // duplicate
    ASSERT_EQ(ids.size(), 3u);
    ASSERT_EQ(std::hash<MessageId>()(makeId(1, 10, 1)), std::hash<MessageId>()(makeId(1, 10, 1)));
}

TEST(StMessageIdTest, testStreamOutput) {
    std::ostringstream out;
    out << makeId(7, 10, 3);
    ASSERT_NE(out.str().find("segment=7"), std::string::npos);

    std::ostringstream emptyOut;
    emptyOut << MessageId();
    ASSERT_EQ(emptyOut.str(), "{empty}");
}

TEST(StMessageIdTest, testByteArrayRoundtripProducerPath) {
    MessageId original = makeId(42, 1234, 5678, 3, 5);
    std::vector<std::byte> bytes = original.toByteArray();
    ASSERT_FALSE(bytes.empty());

    MessageId restored = MessageId::fromByteArray(std::span<const std::byte>(bytes));
    ASSERT_TRUE(static_cast<bool>(restored));
    ASSERT_TRUE(restored == original);

    const auto& impl = MessageIdFactory::impl(restored);
    ASSERT_EQ(impl->segmentId, 42);
    ASSERT_EQ(impl->v4MessageId.ledgerId(), 1234);
    ASSERT_EQ(impl->v4MessageId.entryId(), 5678);
    ASSERT_EQ(impl->v4MessageId.batchIndex(), 3);
    ASSERT_EQ(impl->v4MessageId.partition(), 5);
    ASSERT_TRUE(impl->positionVector.empty());
    ASSERT_FALSE(impl->parentTopic.has_value());
    ASSERT_FALSE(impl->multiTopicVector.has_value());
}

TEST(StMessageIdTest, testByteArrayRoundtripFullSections) {
    auto impl = std::make_shared<MessageIdImpl>();
    impl->v4MessageId = pulsar::MessageId(-1, 10, 20, -1);
    impl->segmentId = 3;
    impl->positionVector.emplace(1, pulsar::MessageId(-1, 100, 1, -1));
    impl->positionVector.emplace(2, pulsar::MessageId(-1, 200, 2, -1));
    impl->parentTopic = "topic://public/default/orders";
    std::map<std::string, std::map<std::int64_t, pulsar::MessageId>> multi;
    multi["topic://public/default/other"].emplace(9, pulsar::MessageId(-1, 900, 9, -1));
    impl->multiTopicVector = multi;

    MessageId original = MessageIdFactory::create(impl);
    auto bytes = original.toByteArray();
    MessageId restored = MessageId::fromByteArray(std::span<const std::byte>(bytes));
    ASSERT_TRUE(static_cast<bool>(restored));
    ASSERT_TRUE(restored == original);

    const auto& r = MessageIdFactory::impl(restored);
    ASSERT_EQ(r->positionVector.size(), 2u);
    ASSERT_EQ(r->positionVector.at(1).ledgerId(), 100);
    ASSERT_EQ(r->positionVector.at(2).ledgerId(), 200);
    ASSERT_TRUE(r->parentTopic.has_value());
    ASSERT_EQ(*r->parentTopic, "topic://public/default/orders");
    ASSERT_TRUE(r->multiTopicVector.has_value());
    ASSERT_EQ(r->multiTopicVector->size(), 1u);
    ASSERT_EQ(r->multiTopicVector->at("topic://public/default/other").at(9).ledgerId(), 900);
}

TEST(StMessageIdTest, testMalformedBytesYieldEmptyId) {
    ASSERT_FALSE(static_cast<bool>(MessageId::fromByteArray({})));

    std::vector<std::byte> tooShort(6, std::byte{1});
    ASSERT_FALSE(static_cast<bool>(MessageId::fromByteArray(std::span<const std::byte>(tooShort))));

    std::vector<std::byte> garbage(64, std::byte{0xFF});
    ASSERT_FALSE(static_cast<bool>(MessageId::fromByteArray(std::span<const std::byte>(garbage))));

    // A truncated valid id must not parse.
    auto bytes = makeId(1, 2, 3).toByteArray();
    bytes.resize(bytes.size() / 2);
    ASSERT_FALSE(static_cast<bool>(MessageId::fromByteArray(std::span<const std::byte>(bytes))));
}

TEST(StMessageIdTest, testEmptyIdSerializesToNothing) { ASSERT_TRUE(MessageId().toByteArray().empty()); }

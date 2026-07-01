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

#include <cstdint>

#include "PulsarApi.pb.h"
#include "lib/Commands.h"
#include "lib/SharedBuffer.h"

// Wire-level tests for the scalable-topics commands: the framed buffers the
// Commands factories produce must deserialize back to the intended protobuf.

using namespace pulsar;
namespace proto = pulsar::proto;

namespace {

// Unwrap [totalSize][cmdSize][BaseCommand] framing produced by the factories.
proto::BaseCommand parseCommand(SharedBuffer buffer) {
    const uint32_t totalSize = buffer.readUnsignedInt();
    const uint32_t cmdSize = buffer.readUnsignedInt();
    EXPECT_EQ(totalSize, cmdSize + sizeof(uint32_t));
    EXPECT_EQ(buffer.readableBytes(), cmdSize);
    proto::BaseCommand cmd;
    EXPECT_TRUE(cmd.ParseFromArray(buffer.data(), static_cast<int>(cmdSize)));
    return cmd;
}

}  // namespace

TEST(ScalableCommandsTest, testScalableTopicLookupRoundtrip) {
    auto cmd = parseCommand(Commands::newScalableTopicLookup(42, "persistent://public/default/orders", true));
    ASSERT_EQ(cmd.type(), proto::BaseCommand::SCALABLE_TOPIC_LOOKUP);
    ASSERT_TRUE(cmd.has_scalabletopiclookup());
    ASSERT_EQ(cmd.scalabletopiclookup().session_id(), 42u);
    ASSERT_EQ(cmd.scalabletopiclookup().topic(), "persistent://public/default/orders");
    ASSERT_TRUE(cmd.scalabletopiclookup().create_if_missing());
}

TEST(ScalableCommandsTest, testScalableTopicLookupNoCreate) {
    auto cmd = parseCommand(Commands::newScalableTopicLookup(7, "orders", false));
    ASSERT_EQ(cmd.scalabletopiclookup().session_id(), 7u);
    ASSERT_FALSE(cmd.scalabletopiclookup().create_if_missing());
}

TEST(ScalableCommandsTest, testScalableTopicCloseRoundtrip) {
    auto cmd = parseCommand(Commands::newScalableTopicClose(42));
    ASSERT_EQ(cmd.type(), proto::BaseCommand::SCALABLE_TOPIC_CLOSE);
    ASSERT_TRUE(cmd.has_scalabletopicclose());
    ASSERT_EQ(cmd.scalabletopicclose().session_id(), 42u);
}

TEST(ScalableCommandsTest, testMessageTypeNamesScalableCommands) {
    // messageType() runs on every incoming command (debug logging) and throws on
    // unknown enum values: every scalable type the broker can send or the client
    // can log must have a name.
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::SCALABLE_TOPIC_LOOKUP), "SCALABLE_TOPIC_LOOKUP");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::SCALABLE_TOPIC_UPDATE), "SCALABLE_TOPIC_UPDATE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::SCALABLE_TOPIC_CLOSE), "SCALABLE_TOPIC_CLOSE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::SCALABLE_TOPIC_SUBSCRIBE),
              "SCALABLE_TOPIC_SUBSCRIBE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE),
              "SCALABLE_TOPIC_SUBSCRIBE_RESPONSE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::SCALABLE_TOPIC_ASSIGNMENT_UPDATE),
              "SCALABLE_TOPIC_ASSIGNMENT_UPDATE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::WATCH_SCALABLE_TOPICS), "WATCH_SCALABLE_TOPICS");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::WATCH_SCALABLE_TOPICS_UPDATE),
              "WATCH_SCALABLE_TOPICS_UPDATE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::WATCH_SCALABLE_TOPICS_CLOSE),
              "WATCH_SCALABLE_TOPICS_CLOSE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::WATCH_TC_ASSIGNMENTS), "WATCH_TC_ASSIGNMENTS");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::WATCH_TC_ASSIGNMENTS_UPDATE),
              "WATCH_TC_ASSIGNMENTS_UPDATE");
    ASSERT_EQ(Commands::messageType(proto::BaseCommand::WATCH_TC_ASSIGNMENTS_CLOSE),
              "WATCH_TC_ASSIGNMENTS_CLOSE");
}

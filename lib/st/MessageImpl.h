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

#include <pulsar/Message.h>
#include <pulsar/st/MessageId.h>
#include <pulsar/st/detail/MessageCore.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace pulsar::st {

/**
 * INTERNAL — the received message behind `detail::MessageCore`.
 *
 * A thin view over a classic `pulsar::Message` (which owns the payload and metadata)
 * plus the segment-qualified `pulsar::st::MessageId` minted on the receive path. An
 * optional `topicOverride` carries the scalable topic identity in namespace mode
 * (a plain segment consumer reports the segment backing topic otherwise).
 */
class MessageImpl {
   public:
    MessageImpl(pulsar::Message message, MessageId id, std::optional<std::string> topicOverride = std::nullopt)
        : classic_(std::move(message)), id_(std::move(id)), topicOverride_(std::move(topicOverride)) {}

    std::span<const std::byte> data() const {
        return {static_cast<const std::byte*>(classic_.getData()), classic_.getLength()};
    }
    const MessageId& id() const { return id_; }
    std::optional<std::string_view> key() const {
        if (!classic_.hasPartitionKey()) return std::nullopt;
        return std::string_view(classic_.getPartitionKey());
    }
    const Properties& properties() const { return classic_.getProperties(); }
    Timestamp publishTime() const { return fromMillis(classic_.getPublishTimestamp()); }
    std::optional<Timestamp> eventTime() const {
        const uint64_t millis = classic_.getEventTimestamp();
        return millis != 0 ? std::optional<Timestamp>(fromMillis(millis)) : std::nullopt;
    }
    // The classic public Message API does not expose the message's sequence id; populating it
    // would require reaching into pulsar::MessageImpl's metadata, i.e. touching the classic API.
    // TODO: revisit when the Stream consumer needs it (a classic Message::getSequenceId() accessor).
    int64_t sequenceId() const { return -1; }
    std::optional<std::string_view> producerName() const {
        const std::string& name = classic_.getProducerName();
        return name.empty() ? std::nullopt : std::optional<std::string_view>(name);
    }
    std::string_view topic() const {
        return topicOverride_ ? std::string_view(*topicOverride_) : std::string_view(classic_.getTopicName());
    }
    int redeliveryCount() const { return classic_.getRedeliveryCount(); }
    std::optional<std::string_view> replicatedFrom() const {
        const std::optional<const std::string*> from = classic_.getReplicatedFrom();
        if (!from || *from == nullptr) return std::nullopt;
        return std::string_view(**from);
    }

   private:
    static Timestamp fromMillis(uint64_t millis) {
        return Timestamp(std::chrono::milliseconds(static_cast<std::int64_t>(millis)));
    }

    pulsar::Message classic_;
    MessageId id_;
    std::optional<std::string> topicOverride_;
};

}  // namespace pulsar::st

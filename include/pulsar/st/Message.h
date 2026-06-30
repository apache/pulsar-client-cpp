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

#include <pulsar/defines.h>
#include <pulsar/st/MessageId.h>
#include <pulsar/st/Schema.h>
#include <pulsar/st/detail/MessageCore.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace pulsar::st {

/**
 * A message received from a scalable topic, carrying a value of type `T`.
 *
 * The value is decoded lazily through `Schema<T>` on each `value()` call; the raw
 * bytes and all metadata are available without decoding.
 *
 * @tparam T the application type the payload decodes to, via `Schema<T>`.
 */
template <typename T>
class Message {
   public:
    /** Construct an empty message (`operator bool` is `false`). */
    Message() = default;

    /**
     * Wrap a received core payload together with the schema used to decode it.
     *
     * Constructed by the SDK on delivery; applications obtain `Message` objects from
     * a consumer rather than building them.
     *
     * @param core the raw received message (payload and metadata).
     * @param schema the schema used to decode the payload in `value()`.
     */
    Message(detail::MessageCore core, Schema<T> schema)
        : core_(std::move(core)), schema_(std::move(schema)) {}

    /**
     * Decode the payload through `Schema<T>` and return the typed value.
     *
     * Decoding happens on every call (the result is not cached). The SDK handles a
     * payload that cannot be decoded internally — such a message is not delivered to
     * the application — so this does not surface decode failures to the caller. The
     * raw bytes remain available via `data()` / `size()`.
     *
     * @return the decoded value of type `T`.
     */
    T value() const {
        const auto* bytes = reinterpret_cast<const std::byte*>(core_.data());
        return schema_.decode(std::span<const std::byte>(bytes, core_.size())).value();
    }

    /**
     * Pointer to the raw, undecoded payload bytes.
     *
     * @return a pointer to `size()` bytes of payload, valid for this message's lifetime.
     */
    const char* data() const { return core_.data(); }

    /**
     * Size of the raw payload in bytes.
     *
     * @return the number of bytes pointed to by `data()`.
     */
    std::size_t size() const { return core_.size(); }

    /**
     * The message's position within the topic.
     *
     * @return the `MessageId` identifying this message.
     */
    MessageId id() const { return core_.id(); }

    /**
     * The optional partition/routing key the message was published with.
     *
     * @return the key, or `std::nullopt` if the message has none.
     */
    std::optional<std::string> key() const {
        return core_.hasKey() ? std::optional<std::string>(core_.key()) : std::nullopt;
    }

    /**
     * The application-defined string properties attached to the message.
     *
     * @return a reference to the properties map.
     */
    const Properties& properties() const { return core_.properties(); }

    /**
     * The broker-assigned publish time.
     *
     * @return the timestamp at which the message was published.
     */
    Timestamp publishTime() const { return Timestamp(std::chrono::milliseconds(core_.publishTimeMs())); }

    /**
     * The optional application-supplied event time.
     *
     * @return the event time, or `std::nullopt` if the producer did not set one.
     */
    std::optional<Timestamp> eventTime() const {
        auto ms = core_.eventTimeMs();
        return ms != 0 ? std::optional<Timestamp>(Timestamp(std::chrono::milliseconds(ms))) : std::nullopt;
    }

    /**
     * The producer-assigned sequence id of the message.
     *
     * @return the sequence id.
     */
    int64_t sequenceId() const { return core_.sequenceId(); }

    /**
     * The name of the producer that published the message, if available.
     *
     * @return the producer name, or `std::nullopt` if not present.
     */
    std::optional<std::string> producerName() const {
        return core_.hasProducerName() ? std::optional<std::string>(core_.producerName()) : std::nullopt;
    }

    /**
     * The resolved canonical topic the message was received from.
     *
     * @return the fully-qualified topic name.
     */
    std::string topic() const { return core_.topic(); }

    /**
     * How many times this message has been redelivered.
     *
     * @return the redelivery count (0 on first delivery).
     */
    int redeliveryCount() const { return core_.redeliveryCount(); }

    /**
     * The source cluster this message was replicated from, if any.
     *
     * @return the originating cluster name, or `std::nullopt` if the message was not
     *         replicated from another cluster.
     */
    std::optional<std::string> replicatedFrom() const {
        return core_.hasReplicatedFrom() ? std::optional<std::string>(core_.replicatedFrom()) : std::nullopt;
    }

    /**
     * Whether this is a non-empty message.
     *
     * @return `true` for a real received message, `false` for a default-constructed one.
     */
    explicit operator bool() const { return static_cast<bool>(core_); }

   private:
    detail::MessageCore core_;
    Schema<T> schema_;
};

/**
 * An iterable batch of messages, as returned by `receiveMulti`. Carries up to the
 * requested count.
 *
 * @tparam T the application type of each contained `Message<T>`.
 */
template <typename T>
class Messages {
   public:
    /** The element type of the batch. */
    using value_type = Message<T>;
    /** Const iterator over the contained messages. */
    using const_iterator = typename std::vector<Message<T>>::const_iterator;

    /**
     * Construct a batch wrapping the given messages.
     *
     * @param messages the messages to hold (empty by default).
     */
    explicit Messages(std::vector<Message<T>> messages = {}) : messages_(std::move(messages)) {}

    /**
     * The number of messages in the batch.
     *
     * @return the element count.
     */
    std::size_t size() const { return messages_.size(); }

    /**
     * Whether the batch contains no messages.
     *
     * @return `true` if empty, `false` otherwise.
     */
    bool empty() const { return messages_.empty(); }

    /**
     * Access the message at index @p i.
     *
     * @param i a zero-based index, which must be less than `size()`.
     * @return a reference to the message at that index.
     */
    const Message<T>& operator[](std::size_t i) const { return messages_[i]; }

    /**
     * Iterator to the first message.
     *
     * @return a const iterator to the beginning of the batch.
     */
    const_iterator begin() const { return messages_.begin(); }

    /**
     * Iterator past the last message.
     *
     * @return a const iterator to the end of the batch.
     */
    const_iterator end() const { return messages_.end(); }

   private:
    std::vector<Message<T>> messages_;
};

}  // namespace pulsar::st

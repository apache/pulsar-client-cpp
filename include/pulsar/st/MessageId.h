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
#include <pulsar/st/detail/Cxx20.h>

#include <compare>
#include <cstddef>
#include <iosfwd>
#include <memory>
#include <span>
#include <vector>

namespace pulsar::st {

class MessageIdImpl;
class MessageIdFactory;

/**
 * The identifier of a single message within a scalable topic.
 *
 * It is **opaque** — no ledger/entry/segment structure is exposed. Internally it
 * encodes the segment the message belongs to (so a later ack can be routed to the
 * right segment), but that is not part of the contract. A `MessageId` is:
 *  - serializable, via `toByteArray()` / `fromByteArray()`, for external storage;
 *  - totally ordered within a single topic, via the comparison operators.
 *
 * For a consistent position across *all* segments of a topic, use `Checkpoint`,
 * not `MessageId` — a single id cannot express a multi-segment position.
 */
class PULSAR_PUBLIC MessageId {
   public:
    /** Construct an empty/invalid id (compares equal only to other empty ids). */
    MessageId();

    /** Sentinel: the earliest (oldest) message available in the topic. */
    [[nodiscard]] static const MessageId& earliest();

    /** Sentinel: the latest (most recently published) message in the topic. */
    [[nodiscard]] static const MessageId& latest();

    /** Serialize to a portable binary form for external storage. */
    [[nodiscard]] std::vector<std::byte> toByteArray() const;

    /** Restore a `MessageId` previously produced by `toByteArray()`. */
    [[nodiscard]] static MessageId fromByteArray(std::span<const std::byte> data);

    // Totally ordered within a topic; `<=>` and `==` synthesize <, <=, >, >=, !=.
    /**
     * Three-way comparison establishing a total order within a single topic.
     *
     * Synthesizes `<`, `<=`, `>`, and `>=`. Ordering of ids from different topics is
     * unspecified.
     *
     * @param other the id to compare against.
     * @return the relative ordering of `*this` and @p other.
     */
    std::strong_ordering operator<=>(const MessageId& other) const;

    /**
     * Equality comparison; also synthesizes `!=`.
     *
     * @param other the id to compare against.
     * @return `true` if the two ids denote the same position.
     */
    bool operator==(const MessageId& other) const;

    /**
     * Whether this is a valid (non-empty) id.
     *
     * @return `true` for a real id, `false` for a default-constructed empty one.
     */
    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    friend class MessageIdFactory;
    explicit MessageId(std::shared_ptr<MessageIdImpl> impl);

    /**
     * Write a human-readable representation of @p messageId to @p s.
     *
     * Intended for logging and debugging; the format is not a stable contract and
     * must not be parsed (use `toByteArray()` for serialization).
     *
     * @param s the output stream to write to.
     * @param messageId the id to format.
     * @return the stream @p s, to allow chaining.
     */
    friend PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const MessageId& messageId);

    std::shared_ptr<MessageIdImpl> impl_;
};

}  // namespace pulsar::st

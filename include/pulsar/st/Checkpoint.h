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

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace pulsar::st {

class CheckpointImpl;
class CheckpointFactory;

/**
 * @brief An opaque, serializable position vector marking a consistent point across
 * all segments of a scalable topic.
 *
 * A `Checkpoint` is the only position type accepted by a `CheckpointConsumer`:
 * unlike `MessageId`, which identifies a single message within one segment, a
 * `Checkpoint` captures the read position of *every* segment at once, so it can
 * express a position that spans the whole topic.
 *
 * The intended workflow is store-and-restore, owned entirely by the application:
 *  - `CheckpointConsumer::checkpoint()` captures an atomic snapshot of the current
 *    per-segment read positions;
 *  - the application serializes the snapshot with `toByteArray()` and persists the
 *    bytes in its own state backend;
 *  - on restart it rebuilds the `Checkpoint` with `fromByteArray()` and resumes
 *    from it via `CheckpointConsumerBuilder::startPosition(...)`.
 *
 * The value is opaque: it carries no observable ledger/entry/segment structure and
 * is not comparable or ordered. Timestamp-based positioning is performed
 * out-of-band through the `scalable-topics seek` admin operation, not through this
 * type.
 */
class PULSAR_PUBLIC Checkpoint {
   public:
    /**
     * @brief Construct an empty/invalid checkpoint.
     *
     * The result is falsy under `operator bool` and must not be passed as a start
     * position; use `earliest()` or `latest()`, or a value restored from
     * `fromByteArray()`, instead.
     */
    Checkpoint();

    /**
     * @brief Well-known sentinel positioned before the earliest available message
     * of every segment.
     *
     * Use as a start position to replay a scalable topic from the very beginning.
     *
     * @return Reference to the shared earliest-position sentinel.
     */
    [[nodiscard]] static const Checkpoint& earliest();

    /**
     * @brief Well-known sentinel positioned after the latest published message of
     * every segment.
     *
     * Use as a start position to consume only messages published after the
     * consumer is created. This is the default start position of
     * `CheckpointConsumerBuilder`.
     *
     * @return Reference to the shared latest-position sentinel.
     */
    [[nodiscard]] static const Checkpoint& latest();

    /**
     * @brief Serialize this checkpoint to a portable binary form for external
     * storage.
     *
     * The returned bytes are an opaque blob suitable for persisting in any state
     * backend; restore them later with `fromByteArray()`.
     *
     * @return a byte vector encoding the cross-segment position.
     */
    [[nodiscard]] std::vector<std::byte> toByteArray() const;

    /**
     * @brief Restore a `Checkpoint` previously produced by `toByteArray()`.
     *
     * @param data bytes returned by an earlier `toByteArray()` call.
     * @return The reconstructed `Checkpoint`.
     */
    [[nodiscard]] static Checkpoint fromByteArray(std::span<const std::byte> data);

    /**
     * @brief Test whether this checkpoint holds a valid position.
     *
     * @return `true` for a sentinel or a value restored from `fromByteArray()`;
     *         `false` for a default-constructed (empty) checkpoint.
     */
    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    friend class CheckpointFactory;
    explicit Checkpoint(std::shared_ptr<CheckpointImpl> impl);

    std::shared_ptr<CheckpointImpl> impl_;
};

}  // namespace pulsar::st

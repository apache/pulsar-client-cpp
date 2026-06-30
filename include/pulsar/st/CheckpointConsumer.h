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
#include <pulsar/st/Checkpoint.h>
#include <pulsar/st/Expected.h>
#include <pulsar/st/Future.h>
#include <pulsar/st/Message.h>
#include <pulsar/st/Schema.h>
#include <pulsar/st/detail/CheckpointConsumerCore.h>
#include <pulsar/st/detail/ClientCore.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace pulsar::st {

/**
 * @brief Configuration accumulated by `CheckpointConsumerBuilder<T>`.
 *
 * Populated through the builder's fluent setters and consumed by
 * `CheckpointConsumerBuilder::create()` / `createAsync()`; applications normally do
 * not construct this directly.
 */
struct CheckpointConsumerConfig {
    std::string topic;  ///< Scalable topic to read. REQUIRED; no default.
    Checkpoint startPosition =
        Checkpoint::latest();  ///< Position to start from. Default `Checkpoint::latest()`.
    std::optional<std::string>
        consumerGroup;  ///< Consumer group to join. Unset (default) => ungrouped, reads every segment.
    std::optional<std::string>
        consumerName;       ///< Human-readable consumer name. Unset (default) => auto-generated.
    Properties properties;  ///< Free-form key/value metadata attached to the consumer. Default empty.
    SchemaInfo schema;      ///< Schema descriptor; filled in from `Schema<T>` by the builder.
};

template <typename T>
class CheckpointConsumerBuilder;

/**
 * @brief Unmanaged consumer with an externally held position — reader semantics,
 * no broker-managed cursor and no acknowledgment (spec §7.3).
 *
 * A `CheckpointConsumer` reads from every segment of a scalable topic like a
 * reader: the broker keeps no durable cursor for it and there is no acknowledgment.
 * The application owns the position. It captures a `Checkpoint` with
 * `checkpoint()`, persists it (see `Checkpoint::toByteArray()`), and on restart
 * resumes from it through `CheckpointConsumerBuilder::startPosition(...)`.
 *
 * Instances are created with `CheckpointConsumerBuilder::create()` /
 * `createAsync()` (note: not `subscribe()`, since there is no subscription). A
 * default-constructed `CheckpointConsumer` is empty and falsy under `operator
 * bool`.
 *
 * @tparam T Message payload type; decoded according to the configured `Schema<T>`.
 */
template <typename T>
class CheckpointConsumer {
   public:
    /** @brief Construct an empty, unusable consumer (falsy under `operator bool`). */
    CheckpointConsumer() = default;

    /**
     * @brief Block until the next message is available and return it.
     *
     * Waits indefinitely for the next message at the current read position.
     *
     * @return `Expected<Message<T>>` holding the decoded message, or an `Error` if
     *         the receive fails (e.g. the consumer is closed or disconnected, or the
     *         payload cannot be decoded).
     */
    Expected<Message<T>> receive() { return toTyped(core_.receiveAsync().get()); }

    /**
     * @brief Block for at most `timeout` waiting for the next message.
     *
     * @param timeout Maximum time to wait (`std::chrono::milliseconds`).
     * @return `Expected<Message<T>>` holding the decoded message, or an `Error`; a
     *         timeout surfaces as `Error{ResultTimeout}`. May also fail on
     *         close/disconnect or a decode error.
     */
    Expected<Message<T>> receive(std::chrono::milliseconds timeout) {
        return toTyped(core_.receiveAsync(timeout.count()).get());
    }

    /**
     * @brief Asynchronously receive the next message.
     *
     * Non-blocking counterpart of `receive()`; the returned future completes when a
     * message is available or the receive fails.
     *
     * @return `Future<Message<T>>` resolving to the decoded message, or an `Error`.
     */
    Future<Message<T>> receiveAsync() {
        Schema<T> schema = schema_;
        return core_.receiveAsync().thenApply(
            [schema](const detail::MessageCore& core) { return Message<T>(core, schema); });
    }

    /**
     * @brief Block to receive up to `maxMessages`, returning early on `timeout`.
     *
     * Accumulates whatever messages are available, returning as soon as
     * `maxMessages` is reached or `timeout` elapses (whichever comes first). The
     * returned batch may contain fewer than `maxMessages` entries.
     *
     * @param maxMessages Maximum number of messages to return in one call.
     * @param timeout Maximum time to wait for the batch to fill
     *        (`std::chrono::milliseconds`).
     * @return `Expected<Messages<T>>` holding the decoded batch, or an `Error` on
     *         failure.
     */
    Expected<Messages<T>> receiveMulti(int maxMessages, std::chrono::milliseconds timeout) {
        return toTypedBatch(core_.receiveMultiAsync(maxMessages, timeout.count()).get());
    }

    /**
     * @brief Capture an atomic snapshot of the current read positions across all
     * segments (spec §8).
     *
     * Records, as a single consistent `Checkpoint`, the per-segment positions read
     * so far. The snapshot is taken locally with no broker round-trip. Persist the
     * returned value (see `Checkpoint::toByteArray()`) to be able to resume from
     * exactly this point later.
     *
     * @return A `Checkpoint` representing the current cross-segment position.
     */
    Checkpoint checkpoint() const { return core_.checkpoint(); }

    /**
     * @brief Close the consumer and release its resources.
     *
     * Blocks until the close completes. After closing, no further receives succeed.
     *
     * @return `Expected<void>` holding success, or an `Error` if the close failed.
     */
    Expected<void> close() { return core_.closeAsync().get(); }

    /**
     * @brief Asynchronously close the consumer.
     *
     * Non-blocking counterpart of `close()`.
     *
     * @return `Future<void>` resolving to success, or an `Error` on failure.
     */
    Future<void> closeAsync() { return core_.closeAsync(); }

    /**
     * @brief Return the topic this consumer reads from.
     *
     * @return a view of the topic name, valid while this consumer is alive.
     */
    std::string_view topic() const { return core_.topic(); }

    /**
     * @brief Test whether this consumer is usable.
     *
     * @return `true` for a consumer produced by the builder; `false` for a
     *         default-constructed (empty) one.
     */
    explicit operator bool() const { return static_cast<bool>(core_); }

   private:
    template <typename U>
    friend class CheckpointConsumerBuilder;
    CheckpointConsumer(detail::CheckpointConsumerCore core, Schema<T> schema)
        : core_(std::move(core)), schema_(std::move(schema)) {}

    Expected<Message<T>> toTyped(Expected<detail::MessageCore> r) const {
        if (r) return Message<T>(*r, schema_);
        return Expected<Message<T>>(r.error());
    }
    Expected<Messages<T>> toTypedBatch(Expected<std::vector<detail::MessageCore>> r) const {
        if (!r) return Expected<Messages<T>>(r.error());
        std::vector<Message<T>> out;
        out.reserve(r->size());
        for (auto& core : *r) out.emplace_back(core, schema_);
        return Messages<T>(std::move(out));
    }

    detail::CheckpointConsumerCore core_;
    Schema<T> schema_;
};

/**
 * @brief Fluent builder for a `CheckpointConsumer<T>`.
 *
 * Obtained from `PulsarClient`. Configure it through the chainable setters, then
 * call the terminal `create()` / `createAsync()` to build the consumer. Note the
 * terminal is `create()`, not `subscribe()`, because a checkpoint consumer has no
 * broker-managed subscription.
 *
 * @tparam T Message payload type of the consumer being built.
 */
template <typename T>
class CheckpointConsumerBuilder {
   public:
    /**
     * @brief Set the scalable topic to read from. REQUIRED; no default.
     *
     * @param t Topic name.
     * @return `*this`, for call chaining.
     */
    CheckpointConsumerBuilder& topic(std::string t) {
        config_.topic = std::move(t);
        return *this;
    }
    /**
     * @brief Set the position to start reading from. Default `Checkpoint::latest()`.
     *
     * Pass `Checkpoint::earliest()` to replay from the beginning, or a value
     * restored via `Checkpoint::fromByteArray()` to resume from a persisted
     * position.
     *
     * @param c Start position.
     * @return `*this`, for call chaining.
     */
    CheckpointConsumerBuilder& startPosition(Checkpoint c) {
        config_.startPosition = std::move(c);
        return *this;
    }
    /**
     * @brief Join a consumer group so segments are shared across its members.
     *
     * Members of the same group divide the topic's segments among themselves;
     * leaving this unset (the default) makes the consumer ungrouped, so it reads
     * every segment on its own.
     *
     * @param g Consumer group name.
     * @return `*this`, for call chaining.
     */
    CheckpointConsumerBuilder& consumerGroup(std::string g) {
        config_.consumerGroup = std::move(g);
        return *this;
    }
    /**
     * @brief Set a human-readable consumer name. Default: auto-generated when unset.
     *
     * @param n Consumer name.
     * @return `*this`, for call chaining.
     */
    CheckpointConsumerBuilder& consumerName(std::string n) {
        config_.consumerName = std::move(n);
        return *this;
    }
    /**
     * @brief Attach a free-form key/value property to the consumer.
     *
     * May be called repeatedly to add several properties; defaults to none.
     *
     * @param k Property key.
     * @param v Property value.
     * @return `*this`, for call chaining.
     */
    CheckpointConsumerBuilder& property(std::string k, std::string v) {
        config_.properties.insert_or_assign(std::move(k), std::move(v));
        return *this;
    }

    /**
     * @brief Build the consumer, blocking until it is ready.
     *
     * @return `Expected<CheckpointConsumer<T>>` holding the consumer, or an `Error`
     *         if creation failed.
     */
    Expected<CheckpointConsumer<T>> create() { return createAsync().get(); }

    /**
     * @brief Asynchronously build the consumer.
     *
     * Non-blocking counterpart of `create()`.
     *
     * @return `Future<CheckpointConsumer<T>>` resolving to the consumer, or an
     *         `Error` on failure.
     */
    Future<CheckpointConsumer<T>> createAsync() {
        Schema<T> schema = schema_;
        CheckpointConsumerConfig config = config_;
        config.schema = schema.info();
        return client_.createCheckpointAsync(std::move(config))
            .thenApply([schema](const detail::CheckpointConsumerCore& core) {
                return CheckpointConsumer<T>(core, schema);
            });
    }

   private:
    friend class PulsarClient;
    CheckpointConsumerBuilder(detail::ClientCore client, Schema<T> schema)
        : client_(std::move(client)), schema_(std::move(schema)) {}

    detail::ClientCore client_;
    Schema<T> schema_;
    CheckpointConsumerConfig config_;
};

}  // namespace pulsar::st

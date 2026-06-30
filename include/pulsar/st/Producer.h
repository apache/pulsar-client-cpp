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
#include <pulsar/st/Error.h>
#include <pulsar/st/Expected.h>
#include <pulsar/st/Future.h>
#include <pulsar/st/Message.h>
#include <pulsar/st/MessageId.h>
#include <pulsar/st/Schema.h>
#include <pulsar/st/Transaction.h>
#include <pulsar/st/detail/ClientCore.h>
#include <pulsar/st/detail/ProducerCore.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace pulsar::st {

/**
 * How a producer claims write access to a topic (spec Appendix A / §4.1).
 *
 * The access mode is fixed at creation time via `ProducerBuilder<T>::accessMode`
 * and controls how the broker arbitrates between multiple producers on the same
 * topic.
 */
enum class ProducerAccessMode
{
    /** Multiple producers may publish to the topic concurrently. The default. */
    Shared,
    /** Only one producer may be active at a time; another producer requesting
     *  `Exclusive` access is rejected while one is already attached. */
    Exclusive,
    /** Like `Exclusive`, but a new producer fences out (evicts) any currently
     *  attached producer rather than being rejected. */
    ExclusiveWithFencing,
    /** Like `Exclusive`, but instead of failing when another producer holds the
     *  topic, this producer waits in line and becomes active once that one
     *  detaches. */
    WaitForExclusive
};

/**
 * Producer configuration accumulated by `ProducerBuilder<T>`.
 *
 * Applications do not populate this directly; the builder fills it in from its
 * fluent setters and hands the completed config to the client when creating a
 * producer. Field semantics, units and defaults mirror the corresponding builder
 * setters.
 */
struct ProducerConfig {
    /** Fully-qualified topic to produce to. REQUIRED; no default. */
    std::string topic;
    /** Optional producer name. Unset (the default) lets the broker assign one. */
    std::optional<std::string> producerName;
    /** Write-access arbitration mode. Defaults to `ProducerAccessMode::Shared`. */
    ProducerAccessMode accessMode = ProducerAccessMode::Shared;
    /** Per-message send timeout in milliseconds. Unset uses the SDK default. */
    std::optional<int64_t> sendTimeoutMs;
    /** When true (the default), block the caller while the send queue is full
     *  instead of failing fast with `ResultProducerQueueIsFull`. */
    bool blockIfQueueFull = true;
    /** Sequence id to assign to the first published message. Unset starts from
     *  the broker-tracked value (0 for a fresh producer). */
    std::optional<int64_t> initialSequenceId;
    /** Arbitrary user metadata attached to the producer. Empty by default. */
    Properties properties;
    /** Schema descriptor sent to the broker for compatibility checking. Filled in
     *  by the builder from `Schema<T>::info()`. */
    SchemaInfo schema;
};

/**
 * A byte-oriented outgoing message assembled by `MessageBuilder<T>`.
 *
 * This is the encoded, schema-agnostic form of a message: the typed value has
 * already been serialized to `payload` bytes. The builder fills these fields from
 * its fluent setters and hands the result to the producer core for publishing.
 */
struct OutgoingMessage {
    /** Encoded message payload — the value serialized to bytes through `Schema<T>`.
     *  Published unless `usesView` is set. */
    std::vector<std::byte> payload;
    /** Non-owning view of already-encoded bytes for zero-copy publishing
     *  (`Schema<BytesView>`); the caller keeps them valid until the send completes. */
    std::span<const std::byte> payloadView;
    /** When true, publish `payloadView` directly without copying; otherwise `payload`. */
    bool usesView = false;
    /** Whether a routing/ordering key is set. `false` (the default) means no key. */
    bool hasKey = false;
    /** Partition/ordering key; meaningful only when `hasKey` is true. */
    std::string key;
    /** Per-message user metadata. Empty by default. */
    Properties properties;
    int64_t eventTimeMs = 0;  ///< Application event time, epoch ms; 0 = unset.
    int64_t sequenceId = -1;  ///< Explicit sequence id; -1 = auto-assign.
    int64_t deliverAtMs = 0;  ///< Absolute delivery time, epoch ms; 0 = deliver immediately.
    /** Target clusters for geo-replication; empty applies the topic's default. */
    std::vector<std::string> replicationClusters;
    std::optional<Transaction> transaction;  ///< Enlisting transaction; unset = non-transactional.
};

template <typename T>
class Producer;

/**
 * Fluent builder for a single message, obtained from `Producer<T>::newMessage()`.
 *
 * Each setter mutates the in-progress message and returns `*this`, so calls can
 * be chained. The typed value is encoded through `Schema<T>` on `value()`; the
 * terminal `send()` / `sendAsync()` hand the encoded message to the producer. A
 * builder describes a single message and is consumed by its terminal call (the
 * message is moved out), so it should not be reused afterwards.
 */
template <typename T>
class MessageBuilder {
   public:
    /**
     * Set the message key, used for per-key ordering and key-affinity routing.
     *
     * @param k the key; taken by value and moved into the message.
     * @return `*this`, for chaining.
     */
    MessageBuilder& key(std::string k) {
        message_.hasKey = true;
        message_.key = std::move(k);
        return *this;
    }
    /**
     * Set the message value, encoding it to bytes through this producer's
     * `Schema<T>`.
     *
     * For a zero-copy `Schema<BytesView>` producer the bytes are not copied — the
     * view is published directly, so the caller must keep them valid until the send
     * completes. A rare encoding failure (e.g. an unset schema) is not reported here,
     * so the fluent chain stays unbroken; it surfaces from the terminal `send()` /
     * `sendAsync()` instead.
     *
     * @param v the typed value to publish.
     * @return `*this`, for chaining.
     */
    MessageBuilder& value(const T& v) {
        if constexpr (std::is_same_v<T, BytesView>) {
            message_.payloadView = v;
            message_.usesView = true;
        } else {
            auto r = schema_.encode(v, message_.payload);
            encodeError_ = r ? std::nullopt : std::optional<Error>(r.error());
        }
        return *this;
    }
    /**
     * Add or overwrite a single user property on the message.
     *
     * @param k property key.
     * @param v property value.
     * @return `*this`, for chaining.
     */
    MessageBuilder& property(std::string k, std::string v) {
        message_.properties.insert_or_assign(std::move(k), std::move(v));
        return *this;
    }
    /**
     * Replace all user properties on the message.
     *
     * @param p the full property map; taken by value and moved in.
     * @return `*this`, for chaining.
     */
    MessageBuilder& properties(Properties p) {
        message_.properties = std::move(p);
        return *this;
    }
    /**
     * Set the application-defined event time of the message.
     *
     * @param t the event time as a wall-clock `Timestamp`; stored as epoch
     *          milliseconds. Unset by default (event time absent).
     * @return `*this`, for chaining.
     */
    MessageBuilder& eventTime(Timestamp t) {
        message_.eventTimeMs = toEpochMs(t);
        return *this;
    }
    /**
     * Set an explicit sequence id for this message, overriding auto-assignment.
     *
     * @param s the sequence id. By default (-1) the producer assigns one
     *          automatically.
     * @return `*this`, for chaining.
     */
    MessageBuilder& sequenceId(int64_t s) {
        message_.sequenceId = s;
        return *this;
    }
    /**
     * Request delayed delivery: deliver the message after `delay` has elapsed from
     * now (spec §4 delayed delivery).
     *
     * @param delay delay relative to the current time, in milliseconds. Computed
     *          into an absolute delivery time. Mutually exclusive with
     *          `deliverAt`; the last of the two called wins.
     * @return `*this`, for chaining.
     */
    MessageBuilder& deliverAfter(std::chrono::milliseconds delay) {
        message_.deliverAtMs = toEpochMs(std::chrono::system_clock::now()) + delay.count();
        return *this;
    }
    /**
     * Request delayed delivery at a specific wall-clock time (spec §4 delayed
     * delivery).
     *
     * @param t absolute delivery time; stored as epoch milliseconds. A time in the
     *          past delivers immediately. Mutually exclusive with `deliverAfter`;
     *          the last of the two called wins.
     * @return `*this`, for chaining.
     */
    MessageBuilder& deliverAt(Timestamp t) {
        message_.deliverAtMs = toEpochMs(t);
        return *this;
    }
    /**
     * Enlist this publish in a transaction so it becomes visible only on commit
     * (spec §9).
     *
     * @param txn the open transaction to enlist the publish in.
     * @return `*this`, for chaining.
     */
    MessageBuilder& transaction(const Transaction& txn) {
        message_.transaction = txn;
        return *this;
    }

    /**
     * Publish the message and block until the broker acknowledges it.
     *
     * @return the assigned `MessageId` on success, or the `Error` on failure. Call
     *         `.value()` on the result to throw `ClientException` instead.
     */
    Expected<MessageId> send() { return sendAsync().get(); }
    /**
     * Publish the message asynchronously without blocking.
     *
     * @return a `Future<MessageId>` that completes with the assigned id on success
     *         or the failure. The future may be ignored for fire-and-forget sends.
     */
    Future<MessageId> sendAsync() {
        if (encodeError_) {
            detail::Promise<MessageId> promise;
            promise.setError(*encodeError_);
            return promise.getFuture();
        }
        return core_.sendAsync(std::move(message_));
    }

   private:
    friend class Producer<T>;
    MessageBuilder(detail::ProducerCore core, Schema<T> schema)
        : core_(std::move(core)), schema_(std::move(schema)) {}

    static int64_t toEpochMs(Timestamp t) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
    }

    detail::ProducerCore core_;
    Schema<T> schema_;
    OutgoingMessage message_;
    std::optional<Error> encodeError_;
};

/**
 * A typed producer for a single scalable topic.
 *
 * Publishes values of type `T`, encoding each through its `Schema<T>`. A producer
 * is a lightweight, copyable handle over shared state; a default-constructed
 * producer is empty (see `operator bool`) and only a producer obtained from
 * `ProducerBuilder<T>::create()` / `createAsync()` is live. All publish methods
 * are thread-safe.
 */
template <typename T>
class Producer {
   public:
    /** Construct an empty producer; `operator bool` is false until assigned a
     *  live producer from `ProducerBuilder<T>`. */
    Producer() = default;

    /**
     * Begin building a single message with per-message options (key, properties,
     * event time, delayed delivery, transaction, ...).
     *
     * @return a fresh `MessageBuilder<T>` bound to this producer.
     */
    MessageBuilder<T> newMessage() { return MessageBuilder<T>(core_, schema_); }

    /**
     * Publish a value and block until the broker acknowledges it. Convenience for
     * `newMessage().value(value).send()`.
     *
     * @param value the value to publish.
     * @return the assigned `MessageId` on success, or the `Error` on failure. Call
     *         `.value()` on the result to throw `ClientException` instead.
     */
    Expected<MessageId> send(const T& value) { return newMessage().value(value).send(); }
    /**
     * Publish a value asynchronously without blocking. Convenience for
     * `newMessage().value(value).sendAsync()`.
     *
     * @param value the value to publish.
     * @return a `Future<MessageId>` that completes with the assigned id or the
     *         failure. May be ignored for fire-and-forget sends.
     */
    Future<MessageId> sendAsync(const T& value) { return newMessage().value(value).sendAsync(); }

    /** @return a view of the topic this producer publishes to, valid while the producer is alive. */
    std::string_view topic() const { return core_.topic(); }
    /** @return a view of the producer's name (broker-assigned when none was configured), valid
     *  while the producer is alive. */
    std::string_view name() const { return core_.name(); }
    /** @return the sequence id of the most recently published message, or -1 if
     *  none has been published yet. */
    int64_t lastSequenceId() const { return core_.lastSequenceId(); }

    /**
     * Block until all sends issued before this call have completed. Takes a
     * snapshot of in-flight sends at the time of the call; sends issued afterwards
     * are not awaited.
     *
     * @return success, or the first `Error` among the awaited sends. Call
     *         `.value()` to throw `ClientException` instead.
     */
    Expected<void> flush() { return core_.flushAsync().get(); }
    /**
     * Asynchronously await all sends issued before this call (a snapshot of
     * in-flight sends).
     *
     * @return a `Future<void>` that completes once those sends finish.
     */
    Future<void> flushAsync() { return core_.flushAsync(); }
    /**
     * Block until pending sends complete, then release the producer. Idempotent:
     * closing an already-closed or empty producer succeeds.
     *
     * @return success, or the `Error` if closing failed. Call `.value()` to throw
     *         `ClientException` instead.
     */
    Expected<void> close() { return core_.closeAsync().get(); }
    /**
     * Asynchronously complete pending sends and release the producer. Idempotent.
     *
     * @return a `Future<void>` that completes once the producer is closed.
     */
    Future<void> closeAsync() { return core_.closeAsync(); }

    /** @return true if this is a live producer handle; false if empty (default
     *  constructed or moved-from). */
    explicit operator bool() const { return static_cast<bool>(core_); }

   private:
    template <typename U>
    friend class ProducerBuilder;
    Producer(detail::ProducerCore core, Schema<T> schema)
        : core_(std::move(core)), schema_(std::move(schema)) {}

    detail::ProducerCore core_;
    Schema<T> schema_;
};

/**
 * Builder for a `Producer<T>`, obtained from `PulsarClient::newProducer`.
 *
 * Each setter returns `*this` for chaining. `topic` is the only required setting;
 * the terminal `create()` / `createAsync()` produce the `Producer<T>`.
 */
template <typename T>
class ProducerBuilder {
   public:
    /**
     * Set the topic to produce to. REQUIRED; there is no default.
     *
     * @param t the fully-qualified topic name; taken by value and moved in.
     * @return `*this`, for chaining.
     */
    ProducerBuilder& topic(std::string t) {
        config_.topic = std::move(t);
        return *this;
    }
    /**
     * Set an explicit producer name.
     *
     * @param n the producer name; taken by value and moved in. Optional; when
     *          unset (the default) the broker assigns a name.
     * @return `*this`, for chaining.
     */
    ProducerBuilder& producerName(std::string n) {
        config_.producerName = std::move(n);
        return *this;
    }
    /**
     * Set the write-access arbitration mode (spec Appendix A / §4.1).
     *
     * @param m the access mode. Defaults to `ProducerAccessMode::Shared`.
     * @return `*this`, for chaining.
     */
    ProducerBuilder& accessMode(ProducerAccessMode m) {
        config_.accessMode = m;
        return *this;
    }
    /**
     * Set the per-message send timeout: how long a send may stay unacknowledged
     * before failing.
     *
     * @param d the timeout, in milliseconds. Optional; when unset the SDK default
     *          applies.
     * @return `*this`, for chaining.
     */
    ProducerBuilder& sendTimeout(std::chrono::milliseconds d) {
        config_.sendTimeoutMs = d.count();
        return *this;
    }

    /**
     * Control behavior when the producer's send queue is full.
     *
     * @param b when true (the DEFAULT), block the caller until the queue drains;
     *          when false, fail fast with `ResultProducerQueueIsFull`.
     * @return `*this`, for chaining.
     */
    ProducerBuilder& blockIfQueueFull(bool b) {
        config_.blockIfQueueFull = b;
        return *this;
    }
    /**
     * Set the sequence id assigned to the first published message.
     *
     * @param s the initial sequence id. Optional; when unset the producer starts
     *          from the broker-tracked value (0 for a fresh producer).
     * @return `*this`, for chaining.
     */
    ProducerBuilder& initialSequenceId(int64_t s) {
        config_.initialSequenceId = s;
        return *this;
    }
    /**
     * Add or overwrite a single user property on the producer.
     *
     * @param k property key.
     * @param v property value.
     * @return `*this`, for chaining.
     */
    ProducerBuilder& property(std::string k, std::string v) {
        config_.properties.insert_or_assign(std::move(k), std::move(v));
        return *this;
    }

    /**
     * Create the producer, blocking until it is ready.
     *
     * @return the live `Producer<T>` on success, or the `Error` on failure. Call
     *         `.value()` on the result to throw `ClientException` instead.
     */
    Expected<Producer<T>> create() { return createAsync().get(); }
    /**
     * Create the producer asynchronously without blocking.
     *
     * @return a `Future<Producer<T>>` that completes with the live producer on
     *         success or the failure.
     */
    Future<Producer<T>> createAsync() {
        Schema<T> schema = schema_;
        ProducerConfig config = config_;
        config.schema = schema.info();
        return client_.createProducerAsync(std::move(config))
            .thenApply([schema](const detail::ProducerCore& core) { return Producer<T>(core, schema); });
    }

   private:
    friend class PulsarClient;
    ProducerBuilder(detail::ClientCore client, Schema<T> schema)
        : client_(std::move(client)), schema_(std::move(schema)) {}

    detail::ClientCore client_;
    Schema<T> schema_;
    ProducerConfig config_;
};

}  // namespace pulsar::st

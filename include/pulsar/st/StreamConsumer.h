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
#include <pulsar/st/Consumer.h>
#include <pulsar/st/Expected.h>
#include <pulsar/st/Future.h>
#include <pulsar/st/Message.h>
#include <pulsar/st/MessageId.h>
#include <pulsar/st/Schema.h>
#include <pulsar/st/Transaction.h>
#include <pulsar/st/detail/ClientCore.h>
#include <pulsar/st/detail/StreamConsumerCore.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace pulsar::st {

/**
 * Plain-old-data configuration accumulated by `StreamConsumerBuilder<T>`.
 *
 * Each field mirrors a builder setter. Prefer building through
 * `StreamConsumerBuilder<T>` rather than populating this struct directly; the
 * builder enforces the invariants (notably that exactly one of topic vs.
 * namespace mode is selected and that `subscriptionName` is set).
 */
struct StreamConsumerConfig {
    /// Selects namespace mode over single-topic mode. When `false` (the default),
    /// `topic` is used; when `true`, `namespaceName` (and `propertyFilters`) apply.
    bool useNamespace = false;
    /// Fully-qualified topic name. Used only when `useNamespace == false`. Mutually
    /// exclusive with `namespaceName`.
    std::string topic;  // when !useNamespace
    /// Namespace name (`tenant/namespace`). Used only when `useNamespace == true`.
    /// Subscribes to all scalable topics in the namespace with live membership.
    std::string namespaceName;  // when useNamespace
    /// Namespace mode only: AND filters matched against topic properties to select
    /// which topics in the namespace are included. Empty means no filtering (all
    /// topics). Ignored in single-topic mode.
    Properties propertyFilters;  // namespace mode: AND filters over topic properties
    /// REQUIRED. Subscription name shared by all consumers of this subscription.
    std::string subscriptionName;  // REQUIRED
    /// Where the subscription starts when it is first created. Default
    /// `SubscriptionInitialPosition::Latest` (skip the backlog). Has no effect once
    /// the subscription already exists.
    SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition::Latest;
    /// Optional key/value properties attached to the subscription itself (persisted
    /// broker-side). Default empty.
    Properties subscriptionProperties;
    /// Optional consumer name (useful for diagnostics and metrics). Default unset, in
    /// which case the broker assigns one.
    std::optional<std::string> consumerName;
    /// Acknowledgment tuning (e.g. the ack-grouping/batching window). Default-constructed
    /// `AckPolicy` when unset.
    AckPolicy ackPolicy;
    /// When set to `true`, read from the topic's compacted view (latest value per key)
    /// instead of the full log. Default unset (broker default, i.e. uncompacted).
    std::optional<bool> readCompacted;
    /// When set to `true`, replicate the subscription's acknowledged position across
    /// geo-replication clusters. Default unset (broker default, i.e. disabled).
    std::optional<bool> replicateSubscriptionState;
    /// Arbitrary client-side consumer properties (reported in topic stats). Default empty.
    Properties properties;
    /// Schema descriptor for the value type `T`. Populated automatically by the builder
    /// from the `Schema<T>` it was constructed with.
    SchemaInfo schema;
};

template <typename T>
class StreamConsumerBuilder;

/**
 * Ordered (per-key), broker-managed consumer with **cumulative ack only**.
 *
 * This is the closest analog to a classic Failover subscription, but spanning all
 * segments of a scalable topic (spec §7.1): messages are delivered in order
 * (per-key) and the broker manages segment assignment for you. A single
 * `acknowledgeCumulative` advances every segment up to the delivered message's
 * position; there is no individual ack, no negative ack, and no dead-letter
 * support. For parallel, unordered consumption with per-message ack use
 * `QueueConsumer<T>` instead.
 *
 * Obtain an instance from `StreamConsumerBuilder<T>`. The default-constructed
 * consumer is an empty handle (`operator bool` is `false`) until assigned one.
 *
 * @tparam T the decoded message value type, determined by the `Schema<T>` used to
 *           build this consumer.
 */
template <typename T>
class StreamConsumer {
   public:
    /** Construct an empty, non-live handle. `operator bool` returns `false` until a
     *  subscribed consumer is move-assigned into it. */
    StreamConsumer() = default;

    /**
     * Block until the next message arrives and return it.
     *
     * Returns `Expected` because a receive can fail *without* yielding a message —
     * the consumer was closed, the connection dropped, or the payload failed to
     * decode. On such failures the result holds an `Error` instead of a message;
     * call `.value()` on the result if you would rather throw a `ClientException`.
     *
     * @return the next `Message<T>`, or an `Error` describing why no message could
     *         be delivered.
     */
    Expected<Message<T>> receive() { return toTyped(core_.receiveAsync().get()); }
    /**
     * Block for the next message, but for no longer than `timeout`.
     *
     * @param timeout maximum time to wait for a message.
     * @return the next `Message<T>`; if no message arrives within `timeout`, an
     *         `Error{ResultTimeout}`; or another `Error` on close/disconnect/decode
     *         failure.
     */
    Expected<Message<T>> receive(std::chrono::milliseconds timeout) {
        return toTyped(core_.receiveAsync(timeout.count()).get());
    }
    /**
     * Request the next message without blocking.
     *
     * @return a `Future<Message<T>>` completed with the message when one is
     *         available, or completed with an `Error` (via the future's
     *         `Expected` result) on close/disconnect/decode failure.
     */
    Future<Message<T>> receiveAsync() {
        Schema<T> schema = schema_;
        return core_.receiveAsync().thenApply(
            [schema](const detail::MessageCore& core) { return Message<T>(core, schema); });
    }

    /**
     * Block for a batch of up to `maxMessages`, bounded by `timeout`.
     *
     * Returns as soon as `maxMessages` have been collected or `timeout` elapses,
     * whichever comes first, so the returned batch may contain fewer than
     * `maxMessages` (including zero on timeout).
     *
     * @param maxMessages the maximum number of messages to return in the batch.
     * @param timeout maximum time to wait while accumulating the batch.
     * @return the collected `Messages<T>`, or an `Error` on close/disconnect/decode
     *         failure.
     */
    Expected<Messages<T>> receiveMulti(int maxMessages, std::chrono::milliseconds timeout) {
        return toTypedBatch(core_.receiveMultiAsync(maxMessages, timeout.count()).get());
    }

    /**
     * Cumulatively acknowledge every message up to and including `id`, advancing all
     * segments up to that position (spec §7.1).
     *
     * Fire-and-forget: it does not block and does not report an error. Acks are
     * buffered and delivered best-effort; a lost ack simply causes redelivery.
     *
     * @param id the message position up to which (inclusive) to acknowledge.
     */
    void acknowledgeCumulative(const MessageId& id) { core_.acknowledgeCumulative(id); }
    /**
     * Transactional cumulative acknowledge: enlist the cumulative ack up to `id` in
     * `txn`.
     *
     * The acknowledgment becomes effective only when `txn` commits; its outcome (and
     * any error) surfaces at `Transaction::commit()`, not here.
     *
     * @param id the message position up to which (inclusive) to acknowledge.
     * @param txn the transaction the acknowledgment is enlisted in.
     */
    void acknowledgeCumulative(const MessageId& id, const Transaction& txn) {
        core_.acknowledgeCumulative(id, txn);
    }

    /**
     * Close the consumer, releasing its broker-side resources. Blocking.
     *
     * @return an empty `Expected<void>` on success, or an `Error` if the close
     *         failed. Call `.value()` to throw instead.
     */
    Expected<void> close() { return core_.closeAsync().get(); }
    /**
     * Close the consumer without blocking.
     *
     * @return a `Future<void>` completed when the close finishes (or with an `Error`
     *         on failure).
     */
    Future<void> closeAsync() { return core_.closeAsync(); }

    /** @return a view of the topic this consumer is subscribed to (in namespace mode, the
     *  namespace-derived subscription target), valid while the consumer is alive. */
    std::string_view topic() const { return core_.topic(); }
    /** @return a view of the subscription name, valid while the consumer is alive. */
    std::string_view subscription() const { return core_.subscription(); }
    /** @return a view of the consumer name (broker-assigned if none was set on the builder),
     *  valid while the consumer is alive. */
    std::string_view consumerName() const { return core_.consumerName(); }

    /** @return `true` if this is a live, subscribed consumer; `false` if it is an empty
     *  (default-constructed or closed/moved-from) handle. */
    explicit operator bool() const { return static_cast<bool>(core_); }

   private:
    template <typename U>
    friend class StreamConsumerBuilder;
    StreamConsumer(detail::StreamConsumerCore core, Schema<T> schema)
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

    detail::StreamConsumerCore core_;
    Schema<T> schema_;
};

/**
 * Fluent builder for a `StreamConsumer<T>`.
 *
 * Obtain one from `PulsarClient`. Set **exactly one** of `topic()` or
 * `inNamespace()` to choose the subscription target, set the REQUIRED
 * `subscriptionName()`, then call `subscribe()` / `subscribeAsync()` to create the
 * consumer. All setters return `*this` for chaining.
 *
 * @tparam T the decoded message value type, fixed by the `Schema<T>` the builder
 *           was created with.
 */
template <typename T>
class StreamConsumerBuilder {
   public:
    /**
     * Subscribe to a single scalable topic. Mutually exclusive with `inNamespace()`;
     * set exactly one. Calling this clears any namespace selection.
     *
     * @param t the fully-qualified topic name.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& topic(std::string t) {
        config_.useNamespace = false;
        config_.topic = std::move(t);
        return *this;
    }

    /**
     * Subscribe to all scalable topics in a namespace with live membership — topics
     * created or removed later are joined/dropped automatically (spec §7.1).
     * Mutually exclusive with `topic()`; set exactly one.
     *
     * Named `inNamespace` because `namespace` is a C++ keyword.
     *
     * @param ns the namespace (`tenant/namespace`) to subscribe across.
     * @param propertyFilters optional AND filters matched against topic properties;
     *        only topics matching all entries are included. Default empty (no
     *        filtering — every topic in the namespace).
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& inNamespace(std::string ns, Properties propertyFilters = {}) {
        config_.useNamespace = true;
        config_.namespaceName = std::move(ns);
        config_.propertyFilters = std::move(propertyFilters);
        return *this;
    }
    /**
     * REQUIRED. Set the subscription name shared by all consumers of this
     * subscription.
     *
     * @param s the subscription name.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& subscriptionName(std::string s) {
        config_.subscriptionName = std::move(s);
        return *this;
    }
    /**
     * Set where the subscription starts when first created.
     *
     * @param p the initial position. Default `SubscriptionInitialPosition::Latest`.
     *        Has no effect if the subscription already exists.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& subscriptionInitialPosition(SubscriptionInitialPosition p) {
        config_.initialPosition = p;
        return *this;
    }
    /**
     * Attach key/value properties to the subscription itself (persisted broker-side).
     * StreamConsumer only.
     *
     * @param p the subscription properties. Default empty.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& subscriptionProperties(Properties p) {
        config_.subscriptionProperties = std::move(p);
        return *this;
    }
    /**
     * Set an explicit consumer name (useful for diagnostics and metrics).
     *
     * @param n the consumer name. Default unset, in which case the broker assigns one.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& consumerName(std::string n) {
        config_.consumerName = std::move(n);
        return *this;
    }

    /**
     * Tune acknowledgment behavior (e.g. the ack-grouping/batching window).
     *
     * @param policy the ack policy. Default-constructed `AckPolicy` when unset.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& ackPolicy(AckPolicy policy) {
        config_.ackPolicy = std::move(policy);
        return *this;
    }
    /**
     * Read the topic's compacted view (latest value per key) instead of the full log.
     * StreamConsumer only.
     *
     * @param b `true` to read compacted. Default unset (broker default: uncompacted).
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& readCompacted(bool b) {
        config_.readCompacted = b;
        return *this;
    }
    /**
     * Replicate the subscription's acknowledged position across geo-replication
     * clusters. StreamConsumer only.
     *
     * @param b `true` to enable. Default unset (broker default: disabled).
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& replicateSubscriptionState(bool b) {
        config_.replicateSubscriptionState = b;
        return *this;
    }
    /**
     * Add a single client-side consumer property (reported in topic stats). Call
     * repeatedly to add multiple; a repeated key overwrites the previous value.
     *
     * @param k property key.
     * @param v property value.
     * @return `*this` for chaining.
     */
    StreamConsumerBuilder& property(const std::string& k, const std::string& v) {
        config_.properties[k] = v;
        return *this;
    }

    /**
     * Create the consumer and subscribe. Blocking.
     *
     * @return the live `StreamConsumer<T>` on success, or an `Error` if the
     *         subscription failed (e.g. missing `subscriptionName`, both/neither of
     *         topic and namespace set, or a broker error). Call `.value()` to throw
     *         instead.
     */
    Expected<StreamConsumer<T>> subscribe() { return subscribeAsync().get(); }
    /**
     * Create the consumer and subscribe without blocking.
     *
     * @return a `Future<StreamConsumer<T>>` completed with the live consumer, or with
     *         an `Error` on failure.
     */
    Future<StreamConsumer<T>> subscribeAsync() {
        Schema<T> schema = schema_;
        StreamConsumerConfig config = config_;
        config.schema = schema.info();
        return client_.subscribeStreamAsync(std::move(config))
            .thenApply(
                [schema](const detail::StreamConsumerCore& core) { return StreamConsumer<T>(core, schema); });
    }

   private:
    friend class PulsarClient;
    StreamConsumerBuilder(detail::ClientCore client, Schema<T> schema)
        : client_(std::move(client)), schema_(std::move(schema)) {}

    detail::ClientCore client_;
    Schema<T> schema_;
    StreamConsumerConfig config_;
};

}  // namespace pulsar::st

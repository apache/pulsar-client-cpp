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
#include <pulsar/st/detail/QueueConsumerCore.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace pulsar::st {

/**
 * Plain-old-data configuration accumulated by `QueueConsumerBuilder<T>`.
 *
 * Each field mirrors a builder setter. Prefer building through
 * `QueueConsumerBuilder<T>` rather than populating this struct directly; the
 * builder enforces the invariants (notably that exactly one of topic vs. namespace
 * mode is selected and that `subscriptionName` is set).
 *
 * This POD can also express states the type system does not rule out — including
 * its **default value**, which selects single-topic mode with an empty `topic`. A
 * configuration with no target (empty `topic` while `useNamespace == false`, or
 * empty `namespaceName` while `useNamespace == true`) or no `subscriptionName` is
 * invalid: `create()` / `createAsync()` reject it with an `Error` instead of
 * connecting. Fields not selected by `useNamespace` are ignored.
 */
struct QueueConsumerConfig {
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
    /// Optional consumer name (useful for diagnostics and metrics). Default unset, in
    /// which case the broker assigns one.
    std::optional<std::string> consumerName;
    /// Acknowledgment tuning (e.g. the ack-grouping/batching window and negative-ack
    /// redelivery delay). Default-constructed `AckPolicy` when unset.
    AckPolicy ackPolicy;
    /// Optional dead-letter policy: route messages to a dead-letter topic after
    /// repeated redelivery. Default unset (no dead-lettering).
    std::optional<DeadLetterPolicy> deadLetterPolicy;
    /// Arbitrary client-side consumer properties (reported in topic stats). Default empty.
    Properties properties;
    /// Schema descriptor for the value type `T`. Populated automatically by the builder
    /// from the `Schema<T>` it was constructed with.
    SchemaInfo schema;
};

template <typename T>
class QueueConsumerBuilder;

/**
 * Parallel, broker-managed consumer with **individual ack + nack + dead-letter**.
 *
 * This is the analog of a classic Shared subscription, attached to all segments of
 * a scalable topic (spec §7.2): work is distributed across all consumers on the
 * subscription with no ordering or key affinity. Each message is acknowledged
 * individually with `acknowledge`, can be negatively acknowledged with
 * `negativeAcknowledge` to schedule redelivery, and can be routed to a dead-letter
 * topic after repeated redelivery (see `QueueConsumerBuilder::deadLetterPolicy`).
 * For ordered, cumulative-ack consumption use `StreamConsumer<T>` instead.
 *
 * Obtain an instance from `QueueConsumerBuilder<T>`. The default-constructed
 * consumer is an empty handle (`operator bool` is `false`) until assigned one.
 *
 * @tparam T the decoded message value type, determined by the `Schema<T>` used to
 *           build this consumer.
 */
template <typename T>
class QueueConsumer {
   public:
    /** Construct an empty, non-live handle. `operator bool` returns `false` until a
     *  subscribed consumer is move-assigned into it. */
    QueueConsumer() = default;

    /** Copyable, movable handle; copies share the underlying consumer. */
    QueueConsumer(const QueueConsumer&) = default;
    QueueConsumer& operator=(const QueueConsumer&) = default;
    QueueConsumer(QueueConsumer&&) = default;
    QueueConsumer& operator=(QueueConsumer&&) = default;

    /**
     * Block until the next message arrives and return it.
     *
     * Returns `Expected` because a receive can fail *without* yielding a message —
     * the consumer was closed or the connection dropped. On such failures the result
     * holds an `Error` instead of a message; call `.value()` on the result if you
     * would rather throw a `ClientException`. (A message whose payload cannot be
     * decoded is handled by the SDK and never delivered, so it is not a receive
     * failure.)
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
     *         `Error{ResultTimeout}`; or another `Error` on close/disconnect
     *         failure.
     */
    Expected<Message<T>> receive(std::chrono::milliseconds timeout) {
        return toTyped(core_.receiveAsync(timeout).get());
    }
    /**
     * Request the next message without blocking.
     *
     * @return a `Future<Message<T>>` completed with the message when one is
     *         available, or completed with an `Error` (via the future's `Expected`
     *         result) on close/disconnect failure.
     */
    Future<Message<T>> receiveAsync() {
        Schema<T> schema = schema_;
        return core_.receiveAsync().thenApply(
            [schema](const detail::MessageCore& core) { return Message<T>(core, schema); });
    }

    /**
     * Individually acknowledge one message.
     *
     * Fire-and-forget: it does not block and does not report an error. Acks are
     * buffered and delivered best-effort; a lost ack simply causes redelivery.
     *
     * @param id the id of the message to acknowledge.
     */
    void acknowledge(const MessageId& id) { core_.acknowledge(id); }
    /**
     * Transactional individual acknowledge: enlist the ack of `id` in `txn`.
     *
     * The acknowledgment becomes effective only when `txn` commits; its outcome (and
     * any error) surfaces at `Transaction::commit()`, not here.
     *
     * @param id the id of the message to acknowledge.
     * @param txn the transaction the acknowledgment is enlisted in.
     */
    void acknowledge(const MessageId& id, const Transaction& txn) { core_.acknowledge(id, txn); }

    /**
     * Negatively acknowledge a message, scheduling it for redelivery.
     *
     * Fire-and-forget `void`: it does not block and does not report an error. The
     * redelivery delay is governed by the configured `AckPolicy`. After enough
     * redeliveries the message may be sent to the dead-letter topic if a
     * `DeadLetterPolicy` was configured.
     *
     * @param id the id of the message to redeliver.
     */
    void negativeAcknowledge(const MessageId& id) { core_.negativeAcknowledge(id); }

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
    friend class QueueConsumerBuilder;
    QueueConsumer(detail::QueueConsumerCore core, Schema<T> schema)
        : core_(std::move(core)), schema_(std::move(schema)) {}

    Expected<Message<T>> toTyped(Expected<detail::MessageCore> r) const {
        if (r) return Message<T>(*r, schema_);
        return Expected<Message<T>>(r.error());
    }

    detail::QueueConsumerCore core_;
    Schema<T> schema_;
};

/**
 * Fluent builder for a `QueueConsumer<T>`.
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
class QueueConsumerBuilder {
   public:
    /**
     * Subscribe to a single scalable topic. Mutually exclusive with `inNamespace()`;
     * set exactly one. Calling this clears any namespace selection.
     *
     * @param t the fully-qualified topic name.
     * @return `*this` for chaining.
     */
    QueueConsumerBuilder& topic(std::string t) {
        config_.useNamespace = false;
        config_.topic = std::move(t);
        return *this;
    }
    /**
     * Subscribe to all scalable topics in a namespace with live membership — topics
     * created or removed later are joined/dropped automatically (spec §7.2).
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
    QueueConsumerBuilder& inNamespace(std::string ns, Properties propertyFilters = {}) {
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
    QueueConsumerBuilder& subscriptionName(std::string s) {
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
    QueueConsumerBuilder& subscriptionInitialPosition(SubscriptionInitialPosition p) {
        config_.initialPosition = p;
        return *this;
    }
    /**
     * Set an explicit consumer name (useful for diagnostics and metrics).
     *
     * @param n the consumer name. Default unset, in which case the broker assigns one.
     * @return `*this` for chaining.
     */
    QueueConsumerBuilder& consumerName(std::string n) {
        config_.consumerName = std::move(n);
        return *this;
    }
    /**
     * Tune acknowledgment behavior (e.g. the ack-grouping/batching window and the
     * negative-ack redelivery delay).
     *
     * @param policy the ack policy. Default-constructed `AckPolicy` when unset.
     * @return `*this` for chaining.
     */
    QueueConsumerBuilder& ackPolicy(AckPolicy policy) {
        config_.ackPolicy = std::move(policy);
        return *this;
    }
    /**
     * Route messages to a dead-letter topic after repeated redelivery (spec §7.2).
     * QueueConsumer only.
     *
     * @param policy the dead-letter policy (max redeliveries, DLQ topic name, etc.).
     *        Default unset (no dead-lettering).
     * @return `*this` for chaining.
     */
    QueueConsumerBuilder& deadLetterPolicy(DeadLetterPolicy policy) {
        config_.deadLetterPolicy = std::move(policy);
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
    QueueConsumerBuilder& property(std::string k, std::string v) {
        config_.properties.insert_or_assign(std::move(k), std::move(v));
        return *this;
    }

    /**
     * Create the consumer and subscribe. Blocking.
     *
     * @return the live `QueueConsumer<T>` on success, or an `Error` if the
     *         subscription failed (e.g. missing `subscriptionName`, both/neither of
     *         topic and namespace set, or a broker error). Call `.value()` to throw
     *         instead.
     */
    Expected<QueueConsumer<T>> subscribe() { return subscribeAsync().get(); }
    /**
     * Create the consumer and subscribe without blocking.
     *
     * @return a `Future<QueueConsumer<T>>` completed with the live consumer, or with
     *         an `Error` on failure.
     */
    Future<QueueConsumer<T>> subscribeAsync() {
        Schema<T> schema = schema_;
        QueueConsumerConfig config = config_;
        config.schema = schema.info();
        return client_.subscribeQueueAsync(std::move(config))
            .thenApply(
                [schema](const detail::QueueConsumerCore& core) { return QueueConsumer<T>(core, schema); });
    }

   private:
    friend class PulsarClient;
    QueueConsumerBuilder(detail::ClientCore client, Schema<T> schema)
        : client_(std::move(client)), schema_(std::move(schema)) {}

    detail::ClientCore client_;
    Schema<T> schema_;
    QueueConsumerConfig config_;
};

}  // namespace pulsar::st

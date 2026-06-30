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

#include <pulsar/Authentication.h>
#include <pulsar/defines.h>
#include <pulsar/st/CheckpointConsumer.h>
#include <pulsar/st/Expected.h>
#include <pulsar/st/Future.h>
#include <pulsar/st/Policies.h>
#include <pulsar/st/Producer.h>
#include <pulsar/st/QueueConsumer.h>
#include <pulsar/st/Schema.h>
#include <pulsar/st/StreamConsumer.h>
#include <pulsar/st/Transaction.h>
#include <pulsar/st/detail/ClientCore.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace pulsar::st {

class PulsarClientBuilder;

/**
 * The heavyweight, thread-safe entry point of the scalable-topics SDK. It owns the
 * connection pool, IO threads, and memory buffers. An application SHOULD create a
 * single instance and keep it for the whole application lifetime, sharing it
 * across all producers and consumers, and close it exactly once at shutdown
 * (spec §3).
 *
 * A `PulsarClient` is a lightweight, copyable handle to that shared state, built
 * only through `PulsarClient::builder()`.
 */
class PULSAR_PUBLIC PulsarClient {
   public:
    /** Copyable, movable handle; copies share the underlying client. */
    PulsarClient(const PulsarClient&) = default;
    PulsarClient& operator=(const PulsarClient&) = default;
    PulsarClient(PulsarClient&&) = default;
    PulsarClient& operator=(PulsarClient&&) = default;

    /**
     * Begin configuring a client.
     *
     * This is the only way to obtain a PulsarClient: the default constructor is
     * private. Set at least PulsarClientBuilder::serviceUrl, then call
     * PulsarClientBuilder::build.
     *
     * @return a fresh, unconfigured client builder
     */
    static PulsarClientBuilder builder();

    /**
     * Start building a producer for values of type `T`.
     *
     * The schema governs serialization and broker-side compatibility: built-ins
     * cover `Bytes` (the default), `std::string` and numeric primitives;
     * structured types use `jsonSchema<T>()`, `avroSchema<T>()`,
     * `protobufNativeSchema<T>()`, or a custom SerDe (see Schema.h and the
     * dedicated schema headers).
     *
     * @tparam T the value type produced; defaults to `Bytes` (raw payload).
     * @param schema the schema describing how `T` is encoded; defaults to the
     *               built-in schema for `T`.
     * @return a ProducerBuilder to further configure and create the producer
     */
    template <typename T = Bytes>
    ProducerBuilder<T> newProducer(Schema<T> schema = {}) {
        return ProducerBuilder<T>(core_, std::move(schema));
    }

    /**
     * Start building a stream consumer: an ordered, broker-managed,
     * cumulative-ack consumer (spec §7.1).
     *
     * @tparam T the value type consumed; defaults to `Bytes` (raw payload).
     * @param schema the schema describing how `T` is decoded; defaults to the
     *               built-in schema for `T`.
     * @return a StreamConsumerBuilder to further configure and create the consumer
     */
    template <typename T = Bytes>
    StreamConsumerBuilder<T> newStreamConsumer(Schema<T> schema = {}) {
        return StreamConsumerBuilder<T>(core_, std::move(schema));
    }

    /**
     * Start building a queue consumer: a parallel, broker-managed,
     * individual-ack consumer (spec §7.2).
     *
     * @tparam T the value type consumed; defaults to `Bytes` (raw payload).
     * @param schema the schema describing how `T` is decoded; defaults to the
     *               built-in schema for `T`.
     * @return a QueueConsumerBuilder to further configure and create the consumer
     */
    template <typename T = Bytes>
    QueueConsumerBuilder<T> newQueueConsumer(Schema<T> schema = {}) {
        return QueueConsumerBuilder<T>(core_, std::move(schema));
    }

    /**
     * Start building a checkpoint consumer: an unmanaged consumer whose read
     * position is held by the client rather than the broker (spec §7.3).
     *
     * @tparam T the value type consumed; defaults to `Bytes` (raw payload).
     * @param schema the schema describing how `T` is decoded; defaults to the
     *               built-in schema for `T`.
     * @return a CheckpointConsumerBuilder to further configure and create the consumer
     */
    template <typename T = Bytes>
    CheckpointConsumerBuilder<T> newCheckpointConsumer(Schema<T> schema = {}) {
        return CheckpointConsumerBuilder<T>(core_, std::move(schema));
    }

    /**
     * Open a new transaction synchronously (spec §9).
     *
     * Blocks until the transaction has been started by the broker. The
     * transaction uses the default timeout configured via
     * PulsarClientBuilder::transactionPolicy.
     *
     * @return the new Transaction, or an Error if it could not be started
     */
    Expected<Transaction> newTransaction() { return core_.newTransactionAsync().get(); }

    /**
     * Open a new transaction asynchronously (spec §9).
     *
     * @return a Future that completes with the new Transaction, or an Error if it
     *         could not be started
     */
    Future<Transaction> newTransactionAsync() { return core_.newTransactionAsync(); }

    /**
     * Close the client gracefully.
     *
     * Awaits all pending operations to complete, then releases every resource the
     * client owns (connections, IO threads, buffers). Blocks until the shutdown
     * finishes. Call this exactly once at application shutdown.
     *
     * @return an empty Expected on success, or an Error if the close failed
     */
    Expected<void> close() { return core_.closeAsync().get(); }

    /**
     * Close the client gracefully, asynchronously.
     *
     * Like close() but returns immediately with a Future that completes once all
     * pending operations have drained and resources have been released.
     *
     * @return a Future that completes empty on success, or with an Error if the
     *         close failed
     */
    Future<void> closeAsync() { return core_.closeAsync(); }

    /**
     * Shut the client down immediately.
     *
     * Drops any pending operations without waiting for them and releases
     * resources at once. Prefer close() for an orderly shutdown; use this only
     * when a graceful close is not possible or not desired.
     */
    void shutdown() { core_.shutdown(); }

    /**
     * Test whether this handle refers to a live client.
     *
     * @return true if the handle is backed by shared client state; false if it
     *         has been moved from
     */
    explicit operator bool() const { return static_cast<bool>(core_); }

   private:
    friend class PulsarClientBuilder;
    PulsarClient() = default;
    explicit PulsarClient(detail::ClientCore core) : core_(std::move(core)) {}

    detail::ClientCore core_;
};

/**
 * Configures and builds a `PulsarClient`. `serviceUrl` is the only required
 * setting; everything else has sensible defaults and is grouped into policy
 * objects (spec Appendix A). With C++20 designated initializers:
 *
 *   auto client = PulsarClient::builder()
 *       .serviceUrl("pulsar://localhost:6650")
 *       .connectionPolicy({.connectionsPerBroker = 4, .connectionTimeout = 10s})
 *       .build();
 */
class PULSAR_PUBLIC PulsarClientBuilder {
   public:
    /**
     * REQUIRED — the Pulsar endpoint, e.g. `pulsar://localhost:6650`.
     *
     * This is the only required setting; build() fails if it is not set.
     *
     * @param url the broker or proxy service URL to connect to
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& serviceUrl(std::string url) {
        serviceUrl_ = std::move(url);
        return *this;
    }

    /**
     * Set the authentication provider used when connecting to the broker.
     *
     * Optional. When unset, the client connects without authentication.
     *
     * @param auth the authentication provider to use
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& authentication(AuthenticationPtr auth) {
        authentication_ = std::move(auth);
        return *this;
    }

    /**
     * Set connection-pool, lookup, and request-timeout tuning.
     *
     * Optional. Any field left unset within the policy falls back to the client
     * default for that setting.
     *
     * @param policy the connection policy to apply
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& connectionPolicy(ConnectionPolicy policy) {
        connectionPolicy_ = std::move(policy);
        return *this;
    }
    /**
     * Set the reconnection backoff policy.
     *
     * Optional. Any field left unset within the policy falls back to the client
     * default for that bound.
     *
     * @param policy the backoff policy to apply
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& backoffPolicy(BackoffPolicy policy) {
        backoffPolicy_ = std::move(policy);
        return *this;
    }
    /**
     * Set the transport security (TLS) policy.
     *
     * Optional. When unset, TLS is disabled and connections are plaintext.
     *
     * @param policy the TLS policy to apply
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& tlsPolicy(TlsPolicy policy) {
        tlsPolicy_ = std::move(policy);
        return *this;
    }
    /**
     * Set client-wide transaction settings (spec §9).
     *
     * Optional. Transactions are always available; this only tunes the default
     * transaction timeout. When unset, the client applies its built-in default.
     *
     * @param policy the transaction policy to apply
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& transactionPolicy(TransactionPolicy policy) {
        transactionPolicy_ = std::move(policy);
        return *this;
    }

    /**
     * Set IO- and listener-thread pool sizing.
     *
     * Optional. Any field left unset within the policy falls back to the client
     * default of a single thread. (The advertised `listenerName` for broker
     * discovery now lives on `ConnectionPolicy`.)
     *
     * @param policy the thread policy to apply
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& threadPolicy(ThreadPolicy policy) {
        threadPolicy_ = std::move(policy);
        return *this;
    }
    /**
     * Set the client-wide memory budget for pending (in-flight) messages.
     *
     * Optional. When unset, the client applies its built-in default limit.
     *
     * @param policy the memory policy to apply
     * @return `*this`, for call chaining
     */
    PulsarClientBuilder& memoryPolicy(MemoryPolicy policy) {
        memoryPolicy_ = std::move(policy);
        return *this;
    }

    /**
     * Build the client from the configured settings.
     *
     * @return the new PulsarClient on success, or an Error describing the
     *         configuration problem (for example, a missing or invalid
     *         serviceUrl())
     */
    Expected<PulsarClient> build();

   private:
    std::string serviceUrl_;
    AuthenticationPtr authentication_;
    ConnectionPolicy connectionPolicy_;
    ThreadPolicy threadPolicy_;
    MemoryPolicy memoryPolicy_;
    BackoffPolicy backoffPolicy_;
    TlsPolicy tlsPolicy_;
    TransactionPolicy transactionPolicy_;
};

inline PulsarClientBuilder PulsarClient::builder() { return PulsarClientBuilder{}; }

}  // namespace pulsar::st

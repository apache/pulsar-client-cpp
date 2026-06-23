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
#include <pulsar/st/Expected.h>
#include <pulsar/st/Future.h>

#include <memory>

namespace pulsar::st {

class TransactionImpl;
using TransactionImplPtr = std::shared_ptr<TransactionImpl>;
namespace detail {
class ClientCore;
class ProducerCore;
class StreamConsumerCore;
class QueueConsumerCore;
}  // namespace detail

/**
 * @brief Lifecycle states of a transaction (spec §9).
 *
 * A transaction starts `Open`, transitions through a transient `Committing` or
 * `Aborting` phase while the outcome is being made durable, and ends in one of the
 * terminal states `Committed`, `Aborted`, `Error`, or `TimedOut`. Query the
 * current state with `Transaction::state()`.
 */
enum class TransactionState {
    Open,        ///< Active: messages and acks may still be enlisted; not yet committed or aborted.
    Committing,  ///< Transient: a `commit()`/`commitAsync()` is in progress but not yet durable.
    Aborting,    ///< Transient: an `abort()`/`abortAsync()` is in progress but not yet finalized.
    Committed,   ///< Terminal: committed successfully; produced messages are visible and acks durable.
    Aborted,     ///< Terminal: aborted; produced messages are discarded and acks rolled back.
    Error,       ///< Terminal: a failure left the transaction in an unrecoverable state.
    TimedOut,    ///< Terminal: the transaction timeout elapsed before commit, so it was aborted.
};

/**
 * @brief A transaction providing exactly-once semantics across multiple scalable
 * topics and subscriptions (spec §9).
 *
 * Messages produced and acknowledgments made within a single transaction are
 * applied atomically: on commit they all take effect, and on abort none of them
 * do. This lets an application consume, transform, and produce across topics with
 * no duplicates and no lost work.
 *
 * Typical usage:
 *  -# obtain a transaction from `PulsarClient::newTransaction()`;
 *  -# enlist publishes with `MessageBuilder::transaction(txn)` and acknowledgments
 *     with `consumer.acknowledge*(id, txn)`;
 *  -# call `commit()` to make the produced messages visible and the acks durable,
 *     or `abort()` to discard everything done within the transaction.
 *
 * A default-constructed `Transaction` is empty (falsy under `operator bool`) and
 * must not be used to enlist or commit work.
 *
 * Holds the hidden `TransactionImpl`; its operations are defined in lib/st.
 */
class PULSAR_PUBLIC Transaction {
   public:
    /** @brief Construct an empty, unusable transaction (falsy under `operator bool`). */
    Transaction() = default;

    /**
     * @brief Return the current lifecycle state of this transaction.
     *
     * @return The current `TransactionState`.
     */
    TransactionState state() const;

    /**
     * @brief Commit the transaction: make produced messages visible and acks durable.
     *
     * Atomically applies every publish and acknowledgment enlisted in this
     * transaction. Blocks until the outcome is durable.
     *
     * @return `Expected<void>` holding success, or an `Error` if the commit failed.
     */
    Expected<void> commit() { return commitAsync().get(); }

    /**
     * @brief Asynchronously commit the transaction.
     *
     * Non-blocking counterpart of `commit()`; the returned future completes when the
     * commit is durable or fails.
     *
     * @return `Future<void>` resolving to success, or an `Error` on failure.
     */
    Future<void> commitAsync() const;

    /**
     * @brief Abort the transaction: discard produced messages and roll back acks.
     *
     * Atomically discards every publish and acknowledgment enlisted in this
     * transaction, as if none had happened. Blocks until the abort is finalized.
     *
     * @return `Expected<void>` holding success, or an `Error` if the abort failed.
     */
    Expected<void> abort() { return abortAsync().get(); }

    /**
     * @brief Asynchronously abort the transaction.
     *
     * Non-blocking counterpart of `abort()`; the returned future completes when the
     * abort is finalized or fails.
     *
     * @return `Future<void>` resolving to success, or an `Error` on failure.
     */
    Future<void> abortAsync() const;

    /**
     * @brief Test whether this object wraps a live transaction.
     *
     * @return `true` for a transaction obtained from `PulsarClient::newTransaction()`;
     *         `false` for a default-constructed (empty) one.
     */
    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    // The client constructs a Transaction; the producer/consumer cores read its
    // impl to enlist sends and acks. All of these live in lib/st.
    friend class detail::ClientCore;
    friend class detail::ProducerCore;
    friend class detail::StreamConsumerCore;
    friend class detail::QueueConsumerCore;
    explicit Transaction(TransactionImplPtr impl) : impl_(std::move(impl)) {}

    TransactionImplPtr impl_;
};

}  // namespace pulsar::st

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

class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
class Transaction;
class PulsarClientBuilder;
struct ProducerConfig;
struct StreamConsumerConfig;
struct QueueConsumerConfig;
struct CheckpointConsumerConfig;

namespace detail {

class ProducerCore;
class StreamConsumerCore;
class QueueConsumerCore;
class CheckpointConsumerCore;

/**
 * INTERNAL — not part of the public API. The non-templated client operations that
 * the templated builders call across to reach the core in lib/st (the typed-API
 * equivalent of the methods today's non-templated `Client` defines out-of-line).
 * Holds the hidden `ClientImpl`; applications use `PulsarClient`, not this.
 */
class PULSAR_PUBLIC ClientCore {
   public:
    ClientCore() = default;

    Future<ProducerCore> createProducerAsync(ProducerConfig config) const;
    Future<StreamConsumerCore> subscribeStreamAsync(StreamConsumerConfig config) const;
    Future<QueueConsumerCore> subscribeQueueAsync(QueueConsumerConfig config) const;
    Future<CheckpointConsumerCore> createCheckpointConsumerAsync(CheckpointConsumerConfig config) const;
    Future<Transaction> newTransactionAsync() const;
    Future<void> closeAsync() const;
    void shutdown() const;

    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    friend class pulsar::st::PulsarClientBuilder;
    explicit ClientCore(ClientImplPtr impl) : impl_(std::move(impl)) {}

    ClientImplPtr impl_;
};

}  // namespace detail
}  // namespace pulsar::st

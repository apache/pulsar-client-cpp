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

#include <pulsar/st/CheckpointConsumer.h>
#include <pulsar/st/Future.h>
#include <pulsar/st/Policies.h>
#include <pulsar/st/Producer.h>
#include <pulsar/st/QueueConsumer.h>
#include <pulsar/st/StreamConsumer.h>
#include <pulsar/st/Transaction.h>

#include <memory>

#include "lib/ClientImpl.h"

namespace pulsar::st {

/**
 * The hidden client behind `detail::ClientCore`.
 *
 * It owns a classic `pulsar::ClientImpl`, reusing its connection pool, executor
 * pools, lookup service and memory limiting wholesale — a scalable topic's
 * segments are ordinary persistent topics broker-side, so the underlying
 * transport stack is unchanged. The scalable-topics specific machinery (DAG
 * watch, segment routing, controller sessions) is layered on top in lib/st,
 * phase by phase.
 */
class ClientImpl {
   public:
    ClientImpl(pulsar::ClientImplPtr classicClient, const TransactionPolicy& transactionPolicy);

    Future<detail::ProducerCore> createProducerAsync(ProducerConfig config);
    Future<detail::StreamConsumerCore> subscribeStreamAsync(StreamConsumerConfig config);
    Future<detail::QueueConsumerCore> subscribeQueueAsync(QueueConsumerConfig config);
    Future<detail::CheckpointConsumerCore> createCheckpointConsumerAsync(CheckpointConsumerConfig config);
    Future<Transaction> newTransactionAsync();
    Future<void> closeAsync();
    void shutdown();

    /** The wrapped classic client (connection pool, executors, lookup). */
    pulsar::ClientImpl& classicClient() { return *classic_; }

   private:
    pulsar::ClientImplPtr classic_;
    TransactionPolicy transactionPolicy_;  // applied when the transaction-coordinator session lands
};

}  // namespace pulsar::st

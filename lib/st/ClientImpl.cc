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
#include "ClientImpl.h"

#include <string>
#include <utility>

namespace pulsar::st {

namespace {

// Placeholder for the operations whose lib/st implementation has not landed
// yet: fail the returned future instead of hanging or crashing. Each of these
// turns into the real implementation in its own phase (producer first).
template <typename T>
Future<T> notImplementedYet(const char* what) {
    detail::Promise<T> promise;
    promise.setError(Error{ResultOperationNotSupported,
                           std::string(what) + " is not implemented yet in the scalable-topics client"});
    return promise.getFuture();
}

}  // namespace

ClientImpl::ClientImpl(pulsar::ClientImplPtr classicClient, const TransactionPolicy& transactionPolicy)
    : classic_(std::move(classicClient)), transactionPolicy_(transactionPolicy) {}

// The producer/consumer/transaction paths land in follow-up phases. Each takes
// its config by value (the sink the real implementation will move from), but as
// a stub it does not consume the config yet — hence the value-param suppressions.
// NOLINTNEXTLINE(performance-unnecessary-value-param)
Future<detail::ProducerCore> ClientImpl::createProducerAsync(ProducerConfig) {
    return notImplementedYet<detail::ProducerCore>("createProducer");
}

// NOLINTNEXTLINE(performance-unnecessary-value-param)
Future<detail::StreamConsumerCore> ClientImpl::subscribeStreamAsync(StreamConsumerConfig) {
    return notImplementedYet<detail::StreamConsumerCore>("subscribeStream");
}

// NOLINTNEXTLINE(performance-unnecessary-value-param)
Future<detail::QueueConsumerCore> ClientImpl::subscribeQueueAsync(QueueConsumerConfig) {
    return notImplementedYet<detail::QueueConsumerCore>("subscribeQueue");
}

// NOLINTNEXTLINE(performance-unnecessary-value-param)
Future<detail::CheckpointConsumerCore> ClientImpl::createCheckpointConsumerAsync(CheckpointConsumerConfig) {
    return notImplementedYet<detail::CheckpointConsumerCore>("createCheckpointConsumer");
}

Future<Transaction> ClientImpl::newTransactionAsync() {
    return notImplementedYet<Transaction>("newTransaction");
}

Future<void> ClientImpl::closeAsync() {
    detail::Promise<void> promise;
    classic_->closeAsync([promise](pulsar::Result result) {
        if (result == pulsar::ResultOk) {
            promise.setSuccess();
        } else {
            promise.setError(Error{result, "failed to close the client"});
        }
    });
    return promise.getFuture();
}

void ClientImpl::shutdown() { classic_->shutdown(); }

}  // namespace pulsar::st

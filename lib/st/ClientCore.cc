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
#include <pulsar/st/detail/ClientCore.h>

#include <utility>

#include "ClientImpl.h"

namespace pulsar::st::detail {

Future<ProducerCore> ClientCore::createProducerAsync(ProducerConfig config) const {
    return impl_->createProducerAsync(std::move(config));
}

Future<StreamConsumerCore> ClientCore::subscribeStreamAsync(StreamConsumerConfig config) const {
    return impl_->subscribeStreamAsync(std::move(config));
}

Future<QueueConsumerCore> ClientCore::subscribeQueueAsync(QueueConsumerConfig config) const {
    return impl_->subscribeQueueAsync(std::move(config));
}

Future<CheckpointConsumerCore> ClientCore::createCheckpointConsumerAsync(
    CheckpointConsumerConfig config) const {
    return impl_->createCheckpointConsumerAsync(std::move(config));
}

Future<Transaction> ClientCore::newTransactionAsync() const { return impl_->newTransactionAsync(); }

Future<void> ClientCore::closeAsync() const { return impl_->closeAsync(); }

void ClientCore::shutdown() const { impl_->shutdown(); }

}  // namespace pulsar::st::detail

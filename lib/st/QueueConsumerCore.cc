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
#include <pulsar/st/detail/MessageCore.h>
#include <pulsar/st/detail/QueueConsumerCore.h>

#include "QueueConsumerImpl.h"

namespace pulsar::st::detail {

// Thin forwarders to the hidden QueueConsumerImpl. The receive path maps the impl's MessageImplPtr
// to a MessageCore — the mapping lambda runs in this member context, which is a friend of
// MessageCore, so it can reach MessageCore's private constructor.
Future<MessageCore> QueueConsumerCore::receiveAsync() const {
    return impl_->receiveAsync().thenApply(
        [](const MessageImplPtr& message) { return MessageCore{message}; });
}
Future<MessageCore> QueueConsumerCore::receiveAsync(std::chrono::milliseconds timeout) const {
    return impl_->receiveAsync(timeout).thenApply(
        [](const MessageImplPtr& message) { return MessageCore{message}; });
}
void QueueConsumerCore::acknowledge(const MessageId& id) const { impl_->acknowledge(id); }
void QueueConsumerCore::acknowledge(const MessageId& id, const Transaction& txn) const {
    impl_->acknowledge(id, txn);
}
void QueueConsumerCore::negativeAcknowledge(const MessageId& id) const { impl_->negativeAcknowledge(id); }
Future<void> QueueConsumerCore::closeAsync() const { return impl_->closeAsync(); }
std::string_view QueueConsumerCore::topic() const { return impl_->topic(); }
std::string_view QueueConsumerCore::subscription() const { return impl_->subscription(); }
std::string_view QueueConsumerCore::consumerName() const { return impl_->consumerName(); }

}  // namespace pulsar::st::detail

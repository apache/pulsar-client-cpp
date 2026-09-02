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
#include <pulsar/st/detail/StreamConsumerCore.h>

#include "StreamConsumerImpl.h"

namespace pulsar::st::detail {

// Thin forwarders to the hidden StreamConsumerImpl. The receive paths map the impl's
// MessageImplPtr to a MessageCore — the mapping lambdas run in this member context,
// which is a friend of MessageCore, so they can reach MessageCore's private constructor.
Future<MessageCore> StreamConsumerCore::receiveAsync() const {
    return impl_->receiveAsync().thenApply(
        [](const MessageImplPtr& message) { return MessageCore{message}; });
}
Future<MessageCore> StreamConsumerCore::receiveAsync(std::chrono::milliseconds timeout) const {
    return impl_->receiveAsync(timeout).thenApply(
        [](const MessageImplPtr& message) { return MessageCore{message}; });
}
Future<std::vector<MessageCore>> StreamConsumerCore::receiveMultiAsync(
    int maxMessages, std::chrono::milliseconds timeout) const {
    return impl_->receiveMultiAsync(maxMessages, timeout)
        .thenApply([](const std::vector<MessageImplPtr>& messages) {
            std::vector<MessageCore> cores;
            cores.reserve(messages.size());
            for (const auto& message : messages) cores.push_back(MessageCore{message});
            return cores;
        });
}
void StreamConsumerCore::acknowledgeCumulative(const MessageId& id) const {
    impl_->acknowledgeCumulative(id);
}
void StreamConsumerCore::acknowledgeCumulative(const MessageId& id, const Transaction& txn) const {
    impl_->acknowledgeCumulative(id, txn);
}
Future<void> StreamConsumerCore::closeAsync() const { return impl_->closeAsync(); }
std::string_view StreamConsumerCore::topic() const { return impl_->topic(); }
std::string_view StreamConsumerCore::subscription() const { return impl_->subscription(); }
std::string_view StreamConsumerCore::consumerName() const { return impl_->consumerName(); }

}  // namespace pulsar::st::detail

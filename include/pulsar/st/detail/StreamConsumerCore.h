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
#include <pulsar/st/Future.h>
#include <pulsar/st/MessageId.h>
#include <pulsar/st/detail/MessageCore.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace pulsar::st {

class StreamConsumerImpl;
using StreamConsumerImplPtr = std::shared_ptr<StreamConsumerImpl>;
class Transaction;

namespace detail {

class ClientCore;

/**
 * INTERNAL — not part of the public API. Non-templated stream-consumer operations
 * over the hidden impl (lib/st). `StreamConsumer<T>` is a thin wrapper over it.
 */
class PULSAR_PUBLIC StreamConsumerCore {
   public:
    StreamConsumerCore() = default;

    Future<MessageCore> receiveAsync() const;
    Future<MessageCore> receiveAsync(int64_t timeoutMs) const;
    Future<std::vector<MessageCore>> receiveMultiAsync(int maxMessages, int64_t timeoutMs) const;
    void acknowledgeCumulative(const MessageId& id) const;
    void acknowledgeCumulative(const MessageId& id, const Transaction& txn) const;
    Future<void> closeAsync() const;
    const std::string& topic() const;
    const std::string& subscription() const;
    const std::string& consumerName() const;

    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    friend class ClientCore;
    explicit StreamConsumerCore(StreamConsumerImplPtr impl) : impl_(std::move(impl)) {}

    StreamConsumerImplPtr impl_;
};

}  // namespace detail
}  // namespace pulsar::st

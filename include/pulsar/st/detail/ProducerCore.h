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

#include <cstdint>
#include <memory>
#include <string>

namespace pulsar::st {

class ProducerImplBase;
using ProducerImplPtr = std::shared_ptr<ProducerImplBase>;
struct OutgoingMessage;

namespace detail {

class ClientCore;

/**
 * INTERNAL — not part of the public API. Non-templated producer operations over
 * the hidden `ProducerImpl` (defined in lib/st). `Producer<T>` / `MessageBuilder<T>`
 * are thin wrappers over it.
 */
class PULSAR_PUBLIC ProducerCore {
   public:
    ProducerCore() = default;

    Future<MessageId> sendAsync(OutgoingMessage message) const;
    const std::string& topic() const;
    const std::string& name() const;
    int64_t lastSequenceId() const;
    Future<void> flushAsync() const;
    Future<void> closeAsync() const;

    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    friend class ClientCore;
    explicit ProducerCore(ProducerImplPtr impl) : impl_(std::move(impl)) {}

    ProducerImplPtr impl_;
};

}  // namespace detail
}  // namespace pulsar::st

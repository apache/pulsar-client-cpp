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

#include <pulsar/st/Future.h>
#include <pulsar/st/MessageId.h>

#include <cstdint>
#include <optional>
#include <string_view>

namespace pulsar::st {

struct OutgoingMessage;

/**
 * INTERNAL — the abstract producer the public `detail::ProducerCore` forwards to.
 *
 * `detail::ProducerCore` holds a `std::shared_ptr<ProducerImplBase>` and each of its
 * methods is a one-line forward to the matching virtual here; `StProducerImpl` (the
 * scalable-topics producer) is the concrete implementation. This lives in `pulsar::st`
 * (not `detail`) to match the forward declaration in `detail/ProducerCore.h`. It is a
 * different type from the classic `pulsar::ProducerImplBase` — different namespace, no
 * collision.
 *
 * The observer methods are `const`; the mutating ones are not. `ProducerCore`'s methods
 * are all `const`, but a `const` handle still reaches a non-const pointee through the
 * shared_ptr, so the mutating virtuals need not be const.
 */
class ProducerImplBase {
   public:
    virtual ~ProducerImplBase() = default;

    virtual Future<MessageId> sendAsync(OutgoingMessage message) = 0;
    virtual std::string_view topic() const = 0;
    virtual std::string_view producerName() const = 0;
    virtual std::optional<std::int64_t> lastSequenceId() const = 0;
    virtual Future<void> flushAsync() = 0;
    virtual Future<void> closeAsync() = 0;
};

}  // namespace pulsar::st

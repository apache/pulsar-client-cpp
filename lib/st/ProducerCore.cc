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
#include <pulsar/st/detail/ProducerCore.h>

#include <pulsar/st/Producer.h>  // complete OutgoingMessage (moved into the impl)

#include <utility>

#include "ProducerImplBase.h"

namespace pulsar::st::detail {

// Thin forwarders to the hidden ProducerImplBase. A const ProducerCore still reaches a
// non-const pointee through the shared_ptr, so the mutating impl methods need no const.
Future<MessageId> ProducerCore::sendAsync(OutgoingMessage message) const {
    return impl_->sendAsync(std::move(message));
}
std::string_view ProducerCore::topic() const { return impl_->topic(); }
std::string_view ProducerCore::producerName() const { return impl_->producerName(); }
std::optional<int64_t> ProducerCore::lastSequenceId() const { return impl_->lastSequenceId(); }
Future<void> ProducerCore::flushAsync() const { return impl_->flushAsync(); }
Future<void> ProducerCore::closeAsync() const { return impl_->closeAsync(); }

}  // namespace pulsar::st::detail

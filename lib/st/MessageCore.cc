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

#include "MessageImpl.h"

namespace pulsar::st::detail {

// Thin forwarders to the hidden MessageImpl (see ProducerCore.cc for the same pattern).
std::span<const std::byte> MessageCore::data() const { return impl_->data(); }
MessageId MessageCore::id() const { return impl_->id(); }
std::optional<std::string_view> MessageCore::key() const { return impl_->key(); }
const Properties& MessageCore::properties() const { return impl_->properties(); }
Timestamp MessageCore::publishTime() const { return impl_->publishTime(); }
std::optional<Timestamp> MessageCore::eventTime() const { return impl_->eventTime(); }
int64_t MessageCore::sequenceId() const { return impl_->sequenceId(); }
std::optional<std::string_view> MessageCore::producerName() const { return impl_->producerName(); }
std::string_view MessageCore::topic() const { return impl_->topic(); }
int MessageCore::redeliveryCount() const { return impl_->redeliveryCount(); }
std::optional<std::string_view> MessageCore::replicatedFrom() const { return impl_->replicatedFrom(); }

}  // namespace pulsar::st::detail

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
#include <pulsar/st/MessageId.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>

namespace pulsar::st {

class MessageImpl;
using MessageImplPtr = std::shared_ptr<MessageImpl>;

using Timestamp = std::chrono::system_clock::time_point;
using Properties = std::map<std::string, std::string>;

namespace detail {

class StreamConsumerCore;
class QueueConsumerCore;
class CheckpointConsumerCore;

/**
 * INTERNAL — not part of the public API. Non-templated, byte-oriented view of a
 * received message; its accessors are defined in lib/st. `Message<T>` wraps this
 * and decodes the payload through `Schema<T>`.
 */
class PULSAR_PUBLIC MessageCore {
   public:
    MessageCore() = default;

    const char* data() const;
    std::size_t size() const;
    MessageId id() const;
    bool hasKey() const;
    const std::string& key() const;
    const Properties& properties() const;
    int64_t publishTimeMs() const;
    int64_t eventTimeMs() const;  // 0 if unset
    int64_t sequenceId() const;
    bool hasProducerName() const;
    const std::string& producerName() const;
    const std::string& topic() const;
    int redeliveryCount() const;
    bool hasReplicatedFrom() const;
    const std::string& replicatedFrom() const;

    explicit operator bool() const { return static_cast<bool>(impl_); }

   private:
    friend class StreamConsumerCore;
    friend class QueueConsumerCore;
    friend class CheckpointConsumerCore;
    explicit MessageCore(MessageImplPtr impl) : impl_(std::move(impl)) {}

    MessageImplPtr impl_;
};

}  // namespace detail
}  // namespace pulsar::st

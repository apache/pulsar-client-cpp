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

#include <google/protobuf/message.h>
#include <pulsar/ProtobufNativeSchema.h>
#include <pulsar/defines.h>
#include <pulsar/st/Schema.h>

#include <cstddef>
#include <span>
#include <string>
#include <type_traits>

namespace pulsar::st {

/// @cond INTERNAL
/// Internal: the protobuf-backed SerDe used by protobufNativeSchema<T>(). Not part
/// of the public API.
namespace detail {
template <typename T>
struct ProtobufNativeSerDe {
    static_assert(std::is_base_of_v<google::protobuf::Message, T>,
                  "protobufNativeSchema<T> requires T to be a generated protobuf Message");
    SchemaInfo info() const { return pulsar::createProtobufNativeSchema(T::descriptor()); }
    std::string encode(const T& value) const { return value.SerializeAsString(); }
    Expected<T> decode(std::span<const char> data) const {
        T message;
        if (message.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
            return message;
        }
        return unexpected(pulsar::ResultInvalidMessage, "failed to parse protobuf message");
    }
};
}  // namespace detail
/// @endcond

/**
 * @brief Creates a schema for a generated protobuf message type `T`.
 *
 * Unlike JSON/Avro, the SerDe is **fully automatic**: protobuf itself provides the
 * serialization, and the broker schema is derived from the message descriptor, so
 * no per-type mapping or reflection library is needed.
 *
 * @code
 * auto producer = client.newProducer(protobufNativeSchema<MyProto>()).topic(t).create();
 * @endcode
 *
 * @tparam T the message type; must derive from `google::protobuf::Message` (a
 *         generated protobuf class). This is enforced at compile time.
 * @return a `Schema<T>` whose `encode`/`decode` use protobuf's native wire format.
 */
template <typename T>
Schema<T> protobufNativeSchema() {
    return Schema<T>(detail::ProtobufNativeSerDe<T>{});
}

}  // namespace pulsar::st

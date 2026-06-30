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

#include <pulsar/st/Schema.h>

#include <cstddef>
#include <exception>
#include <rfl.hpp>
#include <rfl/avro.hpp>
#include <span>
#include <string>
#include <vector>

// avroSchema<T>() is the Avro counterpart of jsonSchema<T>(): reflect-cpp derives
// the SerDe and the Avro schema from T's fields — no per-type serializer. The
// reflect-cpp Avro backend is assumed always present (a required dependency).
//
// NOTE: the `rfl::` calls live here (not in a lib/st .cc) because the SerDe is a
// template instantiated on the user's `T` — that instantiation must happen in the
// including TU. reflect-cpp is therefore confined to this opt-in schema header,
// not the core API headers.

namespace pulsar::st {

/// @cond INTERNAL
/// Internal: the reflect-cpp-backed Avro SerDe used by avroSchema<T>(). Not part
/// of the public API.
namespace detail {
template <typename T>
struct AvroSerDe {
    SchemaInfo info() const { return SchemaInfo(SchemaType::AVRO, "AVRO", rfl::avro::to_schema<T>()); }
    Expected<void> encode(const T& value, std::vector<std::byte>& out) const {
        const std::string s = rfl::avro::write(value);
        const auto* p = reinterpret_cast<const std::byte*>(s.data());
        out.assign(p, p + s.size());
        return {};
    }
    Expected<T> decode(std::span<const std::byte> data) const {
        try {
            return rfl::avro::read<T>(std::string(reinterpret_cast<const char*>(data.data()), data.size()))
                .value();
        } catch (const std::exception& e) {
            return unexpected(pulsar::ResultInvalidMessage, e.what());
        }
    }
};
}  // namespace detail
/// @endcond

/**
 * @brief Creates an Avro schema for `T`, with no boilerplate.
 *
 * The Avro counterpart of jsonSchema(): reflect-cpp derives both the SerDe and the
 * Avro schema directly from the struct's fields, with no per-type serializer.
 *
 * @code
 * auto producer = client.newProducer(avroSchema<Order>()).topic(t).create();
 * @endcode
 *
 * @tparam T the struct type to serialize as Avro; its fields must be reflectable
 *         by reflect-cpp.
 * @return a `Schema<T>` whose `encode`/`decode` use Avro. `decode` reports input
 *         that is not a valid Avro encoding for `T` as an `Error` rather than throwing.
 */
template <typename T>
Schema<T> avroSchema() {
    return Schema<T>(detail::AvroSerDe<T>{});
}

}  // namespace pulsar::st

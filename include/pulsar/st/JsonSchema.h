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
#include <rfl/json.hpp>
#include <span>
#include <string>
#include <vector>

// jsonSchema<T>() derives BOTH the JSON SerDe and the declared schema from T's
// fields via reflect-cpp (https://github.com/getml/reflect-cpp) — no per-type
// serializer, no schema string. This is the Jackson-equivalent for the Java
// client: you pass a plain struct and it just works. reflect-cpp is a required
// dependency of the scalable-topics API (C++20).

namespace pulsar::st {

/// @cond INTERNAL
/// Internal: the reflect-cpp-backed JSON SerDe used by jsonSchema<T>(). Not part
/// of the public API.
namespace detail {
template <typename T>
struct JsonSerDe {
    SchemaInfo info() const { return SchemaInfo(SchemaType::JSON, "JSON", rfl::json::to_schema<T>()); }
    Expected<void> encode(const T& value, std::vector<std::byte>& out) const {
        try {
            const std::string s = rfl::json::write(value);
            const auto* p = reinterpret_cast<const std::byte*>(s.data());
            out.assign(p, p + s.size());
            return {};
        } catch (const std::exception& e) {
            return unexpected(pulsar::ResultInvalidMessage, e.what());
        }
    }
    Expected<T> decode(std::span<const std::byte> data) const {
        try {
            return rfl::json::read<T>(std::string(reinterpret_cast<const char*>(data.data()), data.size()))
                .value();
        } catch (const std::exception& e) {
            return unexpected(pulsar::ResultInvalidMessage, e.what());
        }
    }
};
}  // namespace detail
/// @endcond

/**
 * @brief Creates a JSON schema for `T`, with no boilerplate.
 *
 * reflect-cpp derives both the JSON SerDe and the declared schema directly from
 * the struct's fields (nested structs and containers included) — there is no
 * per-type serializer or hand-written schema string. This is the equivalent of the
 * Java client's Jackson-based JSON schema: pass a plain struct and it just works.
 *
 * @code
 * struct Order { std::string id; int qty; };   // the whole "declaration"
 * auto producer = client.newProducer(jsonSchema<Order>()).topic(t).create();
 * @endcode
 *
 * @tparam T the struct type to serialize as JSON; its fields must be reflectable
 *         by reflect-cpp.
 * @return a `Schema<T>` whose `encode`/`decode` use JSON. Both report failures as
 *         an `Error` (invalid JSON for `T` on `decode`, a serialization failure on
 *         `encode`) rather than throwing. Note: `info()` (schema derivation) is not
 *         on this non-throwing path and may propagate a reflect-cpp exception.
 */
template <typename T>
Schema<T> jsonSchema() {
    return Schema<T>(detail::JsonSerDe<T>{});
}

}  // namespace pulsar::st

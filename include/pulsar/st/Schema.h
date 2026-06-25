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

#include <pulsar/Result.h>
#include <pulsar/Schema.h>
#include <pulsar/defines.h>
#include <pulsar/st/Error.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace pulsar::st {

using pulsar::SchemaInfo;
using pulsar::SchemaType;

/**
 * @brief The default value type: a raw, uninterpreted byte payload.
 *
 * Alias for `std::vector<char>`. A `Schema<Bytes>` (the default schema) passes the
 * payload through verbatim in both directions, applying no encoding or schema
 * declaration to the broker beyond `SchemaType::BYTES`.
 */
using Bytes = std::vector<char>;

/**
 * @brief Constraint identifying a *SerDe* for `T`: a type that can describe `T` to
 * the broker and convert it to and from bytes.
 *
 * A SerDe is any copyable type providing the three const members required below.
 * `Schema<T>` is constructible from any value satisfying `SerDeFor<SerDe, T>`,
 * which is how the same `T` can be carried by different encodings (JSON, Avro,
 * protobuf, or a fully custom codec).
 *
 * The required members are:
 * - `SchemaInfo info() const` — the schema description sent to the broker for
 *   compatibility checking.
 * - `std::string encode(const T&) const` — serializes a value of `T` to bytes.
 * - `T decode(const char*, std::size_t) const` — deserializes bytes back to `T`.
 *
 * @tparam S the candidate SerDe type.
 * @tparam T the value type the SerDe handles.
 */
template <typename S, typename T>
concept SerDeFor = requires(const S& serde, const T& value, const char* data, std::size_t size) {
    { serde.info() }
    ->std::convertible_to<SchemaInfo>;
    { serde.encode(value) }
    ->std::convertible_to<std::string>;
    { serde.decode(data, size) }
    ->std::convertible_to<T>;
};

/**
 * `Schema<T>` is the typed seam of the API: `Producer<T>`, `Consumer<T>` and
 * `Message<T>` are thin facades that only ever call `Schema<T>::encode` / `decode`
 * (and `info`, sent to the broker for compatibility). It is a lightweight,
 * copyable **value** that holds a type-erased *SerDe* — so the same `T` can be
 * carried by different encodings, and a producer can be handed any schema.
 *
 * A SerDe is any copyable type providing three const members:
 *   SchemaInfo   info()                          const;  // describes T to the broker
 *   std::string  encode(const T& value)          const;  // T   -> bytes
 *   T            decode(const char* data, size_t) const;  // bytes -> T
 *
 * Construct a `Schema<T>` from a SerDe directly, or use a factory:
 *   - primitives: `Schema<std::string>{}`, `Schema<int64_t>{}`, `Schema<Bytes>{}` (default)
 *   - `jsonSchema<T>()`            — <pulsar/st/JsonSchema.h>  (reflect-cpp; no trait)
 *   - `avroSchema<T>()`            — <pulsar/st/AvroSchema.h>  (reflect-cpp; no trait)
 *   - `protobufNativeSchema<T>()`  — <pulsar/st/ProtobufNativeSchema.h> (automatic)
 *   - or a custom SerDe: `Schema<T>(mySerDe)` for full control.
 *
 * JSON/Avro for a user struct are derived automatically from the struct's fields
 * by reflect-cpp; protobuf uses the generated message's own reflection.
 */
template <typename T>
class Schema {
   public:
    /** @brief The value type carried by this schema. */
    using value_type = T;

    /**
     * @brief Constructs the default schema for `T`.
     *
     * For a primitive `T` this installs the built-in codec: `Bytes` (the default),
     * `std::string`, `std::int32_t`, `std::int64_t`, or `double`. Integers and
     * `double` are encoded as fixed-width big-endian, matching the Pulsar wire
     * format for primitive schemas.
     *
     * For any other (non-primitive) `T` this installs an "unset" schema: it reports
     * `SchemaType::BYTES` to the broker, but its `encode` and `decode` throw
     * `ClientException` on use. Supply a real schema (`jsonSchema<T>()`,
     * `avroSchema<T>()`, `protobufNativeSchema<T>()`, or a custom SerDe) before
     * producing or consuming such a `T`.
     */
    Schema();

    /**
     * @brief Constructs a schema from any SerDe value.
     *
     * Wraps and type-erases @p serde so that this `Schema<T>` forwards `info`,
     * `encode` and `decode` to it. Use this to plug in a custom encoding for `T`.
     *
     * @tparam SerDe a copyable type satisfying `SerDeFor<SerDe, T>`.
     * @param serde the SerDe to adopt; taken by value and stored.
     */
    template <typename SerDe>
    requires(!std::is_same_v<std::decay_t<SerDe>, Schema> && SerDeFor<std::decay_t<SerDe>, T>)
        Schema(SerDe serde)
        : self_(std::make_shared<Model<std::decay_t<SerDe>>>(std::move(serde))) {}

    /**
     * @brief Returns the schema description sent to the broker for compatibility.
     * @return the `SchemaInfo` (type, name and schema definition) describing `T`.
     */
    SchemaInfo info() const { return self_->info(); }

    /**
     * @brief Serializes a value to its wire bytes.
     * @param value the value to encode.
     * @return the encoded payload as a byte string.
     * @throws ClientException if this is an unset schema (non-primitive `T` with no
     *         SerDe supplied). A custom SerDe may throw on its own encoding errors.
     */
    std::string encode(const T& value) const { return self_->encode(value); }

    /**
     * @brief Deserializes wire bytes back into a value of `T`.
     * @param data pointer to the payload bytes.
     * @param size number of bytes available at @p data.
     * @return the decoded value.
     * @throws ClientException if this is an unset schema (non-primitive `T` with no
     *         SerDe supplied). A SerDe may also throw on malformed or incompatible
     *         bytes.
     */
    T decode(const char* data, std::size_t size) const { return self_->decode(data, size); }

   private:
    struct Concept {
        virtual ~Concept() = default;
        virtual SchemaInfo info() const = 0;
        virtual std::string encode(const T&) const = 0;
        virtual T decode(const char*, std::size_t) const = 0;
    };
    template <typename SerDe>
    struct Model final : Concept {
        SerDe serde;
        explicit Model(SerDe s) : serde(std::move(s)) {}
        SchemaInfo info() const override { return serde.info(); }
        std::string encode(const T& v) const override { return serde.encode(v); }
        T decode(const char* d, std::size_t n) const override { return serde.decode(d, n); }
    };

    std::shared_ptr<const Concept> self_;
};

/// @cond INTERNAL
/// Internal implementation details: built-in primitive codecs and helpers. Not
/// part of the public API.
namespace detail {

// Pulsar encodes numeric schemas as fixed-width big-endian.
template <typename U>
inline std::string encodeBigEndian(U value) {
    static_assert(std::is_integral_v<U>, "integral only");
    std::string out(sizeof(U), '\0');
    auto u = static_cast<std::make_unsigned_t<U>>(value);
    for (std::size_t i = 0; i < sizeof(U); ++i) {
        out[i] = static_cast<char>((u >> (8 * (sizeof(U) - 1 - i))) & 0xFF);
    }
    return out;
}
template <typename U>
inline U decodeBigEndian(const char* data, std::size_t size) {
    static_assert(std::is_integral_v<U>, "integral only");
    std::make_unsigned_t<U> u = 0;
    for (std::size_t i = 0; i < sizeof(U) && i < size; ++i) {
        u = (u << 8) | static_cast<unsigned char>(data[i]);
    }
    return static_cast<U>(u);
}

[[noreturn]] inline void throwNoSchema() {
#if defined(__cpp_exceptions) || defined(_CPPUNWIND)
    throw ClientException(pulsar::ResultInvalidConfiguration,
                          "no schema configured for this value type — pass an explicit Schema "
                          "(jsonSchema/avroSchema/protobufNativeSchema, or a custom SerDe)");
#else
    std::abort();
#endif
}

// Built-in SerDe codecs.
struct BytesCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::BYTES, "BYTES", ""); }
    std::string encode(const Bytes& v) const { return std::string(v.begin(), v.end()); }
    Bytes decode(const char* d, std::size_t n) const { return Bytes(d, d + n); }
};
struct StringCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::STRING, "String", ""); }
    std::string encode(const std::string& v) const { return v; }
    std::string decode(const char* d, std::size_t n) const { return std::string(d, n); }
};
struct Int32Codec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::INT32, "INT32", ""); }
    std::string encode(std::int32_t v) const { return encodeBigEndian(v); }
    std::int32_t decode(const char* d, std::size_t n) const { return decodeBigEndian<std::int32_t>(d, n); }
};
struct Int64Codec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::INT64, "INT64", ""); }
    std::string encode(std::int64_t v) const { return encodeBigEndian(v); }
    std::int64_t decode(const char* d, std::size_t n) const { return decodeBigEndian<std::int64_t>(d, n); }
};
struct DoubleCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::DOUBLE, "Double", ""); }
    std::string encode(double v) const {
        std::uint64_t bits;
        std::memcpy(&bits, &v, sizeof(bits));
        return encodeBigEndian(static_cast<std::int64_t>(bits));
    }
    double decode(const char* d, std::size_t n) const {
        auto bits = static_cast<std::uint64_t>(decodeBigEndian<std::int64_t>(d, n));
        double v;
        std::memcpy(&v, &bits, sizeof(v));
        return v;
    }
};
template <typename T>
struct UnsetCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::BYTES, "BYTES", ""); }
    std::string encode(const T&) const { throwNoSchema(); }
    T decode(const char*, std::size_t) const { throwNoSchema(); }
};

}  // namespace detail
/// @endcond

template <typename T>
Schema<T>::Schema() {
    if constexpr (std::is_same_v<T, Bytes>) {
        self_ = std::make_shared<Model<detail::BytesCodec>>(detail::BytesCodec{});
    } else if constexpr (std::is_same_v<T, std::string>) {
        self_ = std::make_shared<Model<detail::StringCodec>>(detail::StringCodec{});
    } else if constexpr (std::is_same_v<T, std::int32_t>) {
        self_ = std::make_shared<Model<detail::Int32Codec>>(detail::Int32Codec{});
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
        self_ = std::make_shared<Model<detail::Int64Codec>>(detail::Int64Codec{});
    } else if constexpr (std::is_same_v<T, double>) {
        self_ = std::make_shared<Model<detail::DoubleCodec>>(detail::DoubleCodec{});
    } else {
        self_ = std::make_shared<Model<detail::UnsetCodec<T>>>(detail::UnsetCodec<T>{});
    }
}

}  // namespace pulsar::st

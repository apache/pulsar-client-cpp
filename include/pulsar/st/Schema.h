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
#include <pulsar/st/Expected.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

namespace pulsar::st {

using pulsar::SchemaInfo;
using pulsar::SchemaType;

/**
 * @brief The default value type: a raw, uninterpreted byte payload.
 *
 * Alias for `std::vector<std::byte>`. A `Schema<Bytes>` (the default schema) passes
 * the payload through verbatim in both directions, applying no encoding or schema
 * declaration to the broker beyond `SchemaType::BYTES`.
 */
using Bytes = std::vector<std::byte>;

/**
 * @brief A non-owning, zero-copy view over raw payload bytes.
 *
 * Alias for `std::span<const std::byte>` — the zero-copy counterpart of `Bytes`. A
 * `Schema<BytesView>` publishes the viewed bytes without copying them (the caller
 * must keep them valid until the send completes), and on receive
 * `Message<BytesView>::value()` returns a view into the message's own buffer (valid
 * for that message's lifetime). Use `Bytes` when you want the SDK to own a copy.
 */
using BytesView = std::span<const std::byte>;

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
 * - `Expected<void> encode(const T&, std::vector<std::byte>& out) const` —
 *   serializes a value of `T` into @p out (replacing its contents), reporting any
 *   failure as an error value. The buffer is supplied by the caller so it can be
 *   reused across messages, avoiding a per-message allocation.
 * - `Expected<T> decode(std::span<const std::byte>) const` — deserializes bytes
 *   back to `T`, reporting malformed input as an error value.
 *
 * @tparam S the candidate SerDe type.
 * @tparam T the value type the SerDe handles.
 */
template <typename S, typename T>
concept SerDeFor = requires(const S& serde, const T& value, std::span<const std::byte> data,
                            std::vector<std::byte>& out) {
    { serde.info() }
    ->std::convertible_to<SchemaInfo>;
    { serde.encode(value, out) }
    ->std::convertible_to<Expected<void>>;
    { serde.decode(data) }
    ->std::convertible_to<Expected<T>>;
};

/**
 * `Schema<T>` is the typed seam of the API: `Producer<T>`, `Consumer<T>` and
 * `Message<T>` are thin facades that only ever call `Schema<T>::encode` / `decode`
 * (and `info`, sent to the broker for compatibility). It is a lightweight,
 * copyable **value** that holds a type-erased *SerDe* — so the same `T` can be
 * carried by different encodings, and a producer can be handed any schema.
 *
 * A SerDe is any copyable type providing three const members:
 *   SchemaInfo      info()                                              const;
 *   Expected<void>  encode(const T& value, std::vector<std::byte>& out) const;  // T -> bytes
 *   Expected<T>     decode(std::span<const std::byte> data)             const;  // bytes -> T
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
     * `SchemaType::BYTES` to the broker, but its `encode` and `decode` return an
     * `Error` on use. Supply a real schema (`jsonSchema<T>()`, `avroSchema<T>()`,
     * `protobufNativeSchema<T>()`, or a custom SerDe) before producing or consuming
     * such a `T`.
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
     * @brief Serializes a value into a caller-provided byte buffer.
     *
     * The destination buffer is supplied by the caller and reused across calls, so
     * the producer hot path does not allocate a fresh buffer per message.
     *
     * @param value the value to encode.
     * @param out destination buffer; its previous contents are replaced.
     * @return success, or an `Error` — including an unset schema (non-primitive `T`
     *         with no SerDe supplied) or a SerDe-specific encoding failure.
     */
    Expected<void> encode(const T& value, std::vector<std::byte>& out) const {
        return self_->encode(value, out);
    }

    /**
     * @brief Deserializes wire bytes back into a value of `T`.
     * @param data a view over the payload bytes.
     * @return the decoded value, or an `Error` if decoding fails — including an unset
     *         schema (non-primitive `T` with no SerDe supplied) or bytes that are
     *         malformed or incompatible with the schema.
     */
    Expected<T> decode(std::span<const std::byte> data) const { return self_->decode(data); }

   private:
    struct Concept {
        virtual ~Concept() = default;
        virtual SchemaInfo info() const = 0;
        virtual Expected<void> encode(const T&, std::vector<std::byte>&) const = 0;
        virtual Expected<T> decode(std::span<const std::byte>) const = 0;
    };
    template <typename SerDe>
    struct Model final : Concept {
        SerDe serde;
        explicit Model(SerDe s) : serde(std::move(s)) {}
        SchemaInfo info() const override { return serde.info(); }
        Expected<void> encode(const T& v, std::vector<std::byte>& out) const override {
            return serde.encode(v, out);
        }
        Expected<T> decode(std::span<const std::byte> d) const override { return serde.decode(d); }
    };

    std::shared_ptr<const Concept> self_;
};

/// @cond INTERNAL
/// Internal implementation details: built-in primitive codecs and helpers. Not
/// part of the public API.
namespace detail {

inline constexpr const char* kNoSchemaMsg =
    "no schema configured for this value type — pass an explicit Schema "
    "(jsonSchema/avroSchema/protobufNativeSchema, or a custom SerDe)";

// Pulsar encodes numeric schemas as fixed-width big-endian.
template <typename U>
inline void encodeBigEndian(U value, std::vector<std::byte>& out) {
    static_assert(std::is_integral_v<U>, "integral only");
    auto u = static_cast<std::make_unsigned_t<U>>(value);
    for (std::size_t i = 0; i < sizeof(U); ++i) {
        out.push_back(static_cast<std::byte>((u >> (8 * (sizeof(U) - 1 - i))) & 0xFF));
    }
}
template <typename U>
inline U decodeBigEndian(std::span<const std::byte> data) {
    static_assert(std::is_integral_v<U>, "integral only");
    std::make_unsigned_t<U> u = 0;
    for (std::size_t i = 0; i < sizeof(U) && i < data.size(); ++i) {
        u = (u << 8) | std::to_integer<unsigned char>(data[i]);
    }
    return static_cast<U>(u);
}

// Built-in SerDe codecs.
struct BytesCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::BYTES, "BYTES", ""); }
    Expected<void> encode(const Bytes& v, std::vector<std::byte>& out) const {
        out = v;
        return {};
    }
    Expected<Bytes> decode(std::span<const std::byte> d) const { return Bytes(d.begin(), d.end()); }
};
// Zero-copy raw bytes: decode hands back a view into the message buffer; the
// producer publishes the caller's span without copying (it bypasses encode, which
// is provided here only as an owning fallback).
struct SpanBytesCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::BYTES, "BYTES", ""); }
    Expected<void> encode(BytesView v, std::vector<std::byte>& out) const {
        out.assign(v.begin(), v.end());
        return {};
    }
    Expected<BytesView> decode(std::span<const std::byte> d) const { return d; }
};
struct StringCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::STRING, "String", ""); }
    Expected<void> encode(const std::string& v, std::vector<std::byte>& out) const {
        const auto* p = reinterpret_cast<const std::byte*>(v.data());
        out.assign(p, p + v.size());
        return {};
    }
    Expected<std::string> decode(std::span<const std::byte> d) const {
        return std::string(reinterpret_cast<const char*>(d.data()), d.size());
    }
};
struct Int32Codec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::INT32, "INT32", ""); }
    Expected<void> encode(std::int32_t v, std::vector<std::byte>& out) const {
        out.clear();
        encodeBigEndian(v, out);
        return {};
    }
    Expected<std::int32_t> decode(std::span<const std::byte> d) const {
        if (d.size() < sizeof(std::int32_t))
            return unexpected(pulsar::ResultInvalidMessage, "INT32 payload too short");
        return decodeBigEndian<std::int32_t>(d);
    }
};
struct Int64Codec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::INT64, "INT64", ""); }
    Expected<void> encode(std::int64_t v, std::vector<std::byte>& out) const {
        out.clear();
        encodeBigEndian(v, out);
        return {};
    }
    Expected<std::int64_t> decode(std::span<const std::byte> d) const {
        if (d.size() < sizeof(std::int64_t))
            return unexpected(pulsar::ResultInvalidMessage, "INT64 payload too short");
        return decodeBigEndian<std::int64_t>(d);
    }
};
struct DoubleCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::DOUBLE, "Double", ""); }
    Expected<void> encode(double v, std::vector<std::byte>& out) const {
        std::uint64_t bits;
        std::memcpy(&bits, &v, sizeof(bits));
        out.clear();
        encodeBigEndian(static_cast<std::int64_t>(bits), out);
        return {};
    }
    Expected<double> decode(std::span<const std::byte> d) const {
        if (d.size() < sizeof(double))
            return unexpected(pulsar::ResultInvalidMessage, "DOUBLE payload too short");
        auto bits = static_cast<std::uint64_t>(decodeBigEndian<std::int64_t>(d));
        double v;
        std::memcpy(&v, &bits, sizeof(v));
        return v;
    }
};
template <typename T>
struct UnsetCodec {
    SchemaInfo info() const { return SchemaInfo(SchemaType::BYTES, "BYTES", ""); }
    Expected<void> encode(const T&, std::vector<std::byte>&) const {
        return unexpected(pulsar::ResultInvalidConfiguration, kNoSchemaMsg);
    }
    Expected<T> decode(std::span<const std::byte>) const {
        return unexpected(pulsar::ResultInvalidConfiguration, kNoSchemaMsg);
    }
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
    } else if constexpr (std::is_same_v<T, BytesView>) {
        self_ = std::make_shared<Model<detail::SpanBytesCodec>>(detail::SpanBytesCodec{});
    } else {
        self_ = std::make_shared<Model<detail::UnsetCodec<T>>>(detail::UnsetCodec<T>{});
    }
}

}  // namespace pulsar::st

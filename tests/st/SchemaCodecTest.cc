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
#include <gtest/gtest.h>
#include <pulsar/st/Schema.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <vector>

using namespace pulsar::st;

namespace {

// Encode + decode through the public Schema<T> seam and require the value to
// survive the roundtrip.
template <typename T>
void expectRoundtrip(const T& value) {
    Schema<T> schema;
    std::vector<std::byte> encoded;
    auto ok = schema.encode(value, encoded);
    ASSERT_TRUE(ok);
    auto decoded = schema.decode(std::span<const std::byte>(encoded));
    ASSERT_TRUE(decoded);
    ASSERT_EQ(*decoded, value);
}

}  // namespace

TEST(SchemaCodecTest, testIntegerRoundtrips) {
    expectRoundtrip<std::int8_t>(42);
    expectRoundtrip<std::int8_t>(-1);
    expectRoundtrip<std::int16_t>(0x1234);
    expectRoundtrip<std::int16_t>(std::numeric_limits<std::int16_t>::min());
    expectRoundtrip<std::int32_t>(0x01020304);
    expectRoundtrip<std::int32_t>(-1);
    expectRoundtrip<std::int32_t>(std::numeric_limits<std::int32_t>::max());
    expectRoundtrip<std::int64_t>(0x0102030405060708LL);
    expectRoundtrip<std::int64_t>(std::numeric_limits<std::int64_t>::min());
}

TEST(SchemaCodecTest, testFloatingPointRoundtrips) {
    expectRoundtrip<float>(3.5f);
    expectRoundtrip<float>(-0.0f);
    expectRoundtrip<double>(2.718281828459045);
    expectRoundtrip<double>(std::numeric_limits<double>::max());
}

TEST(SchemaCodecTest, testStringRoundtrip) {
    expectRoundtrip<std::string>("hello scalable topics");
    expectRoundtrip<std::string>("");
}

TEST(SchemaCodecTest, testBytesRoundtrip) {
    Bytes payload = {std::byte{0x00}, std::byte{0xFF}, std::byte{0x7F}};
    expectRoundtrip<Bytes>(payload);
}

TEST(SchemaCodecTest, testInt32IsBigEndianOnTheWire) {
    Schema<std::int32_t> schema;
    std::vector<std::byte> encoded;
    ASSERT_TRUE(schema.encode(0x01020304, encoded));
    ASSERT_EQ(encoded.size(), 4u);
    ASSERT_EQ(encoded[0], std::byte{0x01});
    ASSERT_EQ(encoded[1], std::byte{0x02});
    ASSERT_EQ(encoded[2], std::byte{0x03});
    ASSERT_EQ(encoded[3], std::byte{0x04});
}

TEST(SchemaCodecTest, testShortPayloadIsAnErrorNotUb) {
    std::vector<std::byte> tooShort(2);
    ASSERT_FALSE(Schema<std::int32_t>{}.decode(std::span<const std::byte>(tooShort)));
    ASSERT_FALSE(Schema<std::int64_t>{}.decode(std::span<const std::byte>(tooShort)));
    ASSERT_FALSE(Schema<float>{}.decode(std::span<const std::byte>(tooShort)));
    ASSERT_FALSE(Schema<double>{}.decode(std::span<const std::byte>(tooShort)));
    std::vector<std::byte> empty;
    ASSERT_FALSE(Schema<std::int8_t>{}.decode(std::span<const std::byte>(empty)));
}

TEST(SchemaCodecTest, testBytesViewDecodeIsZeroCopy) {
    std::vector<std::byte> buffer = {std::byte{1}, std::byte{2}, std::byte{3}};
    Schema<BytesView> schema;
    auto view = schema.decode(std::span<const std::byte>(buffer));
    ASSERT_TRUE(view);
    ASSERT_EQ(view->data(), buffer.data());  // a view into the same buffer, no copy
    ASSERT_EQ(view->size(), buffer.size());
}

TEST(SchemaCodecTest, testPrimitiveSchemaInfoNames) {
    ASSERT_EQ(Schema<std::string>{}.info().getName(), "STRING");
    ASSERT_EQ(Schema<std::int32_t>{}.info().getName(), "INT32");
    ASSERT_EQ(Schema<std::int64_t>{}.info().getName(), "INT64");
    ASSERT_EQ(Schema<float>{}.info().getName(), "FLOAT");
    ASSERT_EQ(Schema<double>{}.info().getName(), "DOUBLE");
    ASSERT_EQ(Schema<Bytes>{}.info().getName(), "BYTES");
}

TEST(SchemaCodecTest, testUnsetSchemaReportsErrors) {
    struct NotAPrimitive {
        int x = 0;
    };
    Schema<NotAPrimitive> schema;  // no SerDe supplied -> unset codec
    std::vector<std::byte> out;
    ASSERT_FALSE(schema.encode(NotAPrimitive{}, out));
    ASSERT_FALSE(schema.decode(std::span<const std::byte>(out)));
}

TEST(SchemaCodecTest, testCustomSerDeThroughTypeErasure) {
    // A toy SerDe proving the pluggable seam: encodes an int as a decimal string.
    struct DecimalSerDe {
        SchemaInfo info() const { return SchemaInfo(SchemaType::STRING, "DECIMAL", ""); }
        Expected<void> encode(const int& v, std::vector<std::byte>& out) const {
            const std::string s = std::to_string(v);
            const auto* p = reinterpret_cast<const std::byte*>(s.data());
            out.assign(p, p + s.size());
            return {};
        }
        Expected<int> decode(std::span<const std::byte> d) const {
            return std::stoi(std::string(reinterpret_cast<const char*>(d.data()), d.size()));
        }
    };
    Schema<int> schema{DecimalSerDe{}};
    std::vector<std::byte> encoded;
    ASSERT_TRUE(schema.encode(12345, encoded));
    ASSERT_EQ(encoded.size(), 5u);  // "12345"
    auto decoded = schema.decode(std::span<const std::byte>(encoded));
    ASSERT_TRUE(decoded);
    ASSERT_EQ(*decoded, 12345);
}

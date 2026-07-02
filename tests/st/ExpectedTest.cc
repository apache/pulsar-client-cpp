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
#include <pulsar/st/Expected.h>

#include <memory>
#include <string>
#include <utility>

using namespace pulsar::st;
using UniqueInt = std::unique_ptr<int>;

TEST(ExpectedTest, testValueState) {
    Expected<int> e(42);
    ASSERT_TRUE(e.has_value());
    ASSERT_TRUE(static_cast<bool>(e));
    ASSERT_EQ(*e, 42);
    ASSERT_EQ(e.value(), 42);
}

TEST(ExpectedTest, testErrorState) {
    Expected<int> e(Error{ResultTimeout, "timed out"});
    ASSERT_FALSE(e.has_value());
    ASSERT_EQ(e.error().result, ResultTimeout);
    ASSERT_EQ(e.error().message, "timed out");
}

TEST(ExpectedTest, testUnexpectedFactory) {
    Expected<int> e = unexpected(ResultInvalidMessage, "bad payload");
    ASSERT_FALSE(e);
    ASSERT_EQ(e.error().result, ResultInvalidMessage);
}

TEST(ExpectedTest, testValueThrowsClientExceptionOnError) {
    Expected<int> e(Error{ResultUnknownError, "boom"});
    ASSERT_THROW(e.value(), ClientException);
}

TEST(ExpectedTest, testVoidSpecialization) {
    Expected<void> ok;
    ASSERT_TRUE(ok);
    ASSERT_NO_THROW(ok.value());

    Expected<void> failed(Error{ResultTimeout, "t"});
    ASSERT_FALSE(failed);
    ASSERT_EQ(failed.error().result, ResultTimeout);
    ASSERT_THROW(failed.value(), ClientException);
}

TEST(ExpectedTest, testValueOr) {
    Expected<int> ok(5);
    ASSERT_EQ(ok.value_or(0), 5);

    Expected<int> failed(Error{ResultUnknownError, ""});
    ASSERT_EQ(failed.value_or(7), 7);
}

TEST(ExpectedTest, testMonadicOpsOnLvalue) {
    Expected<int> e(5);
    auto doubled = e.transform([](int x) { return x * 2; });
    ASSERT_TRUE(doubled);
    ASSERT_EQ(*doubled, 10);

    auto chained = e.and_then([](int x) { return Expected<long>(x + 1L); });
    ASSERT_TRUE(chained);
    ASSERT_EQ(*chained, 6L);

    auto recovered = e.or_else([](const Error&) { return Expected<int>(-1); });
    ASSERT_TRUE(recovered);
    ASSERT_EQ(*recovered, 5);

    Expected<int> failed(Error{ResultTimeout, "t"});
    auto mapped = failed.transform([](int x) { return x * 2; });
    ASSERT_FALSE(mapped);
    ASSERT_EQ(mapped.error().result, ResultTimeout);
    auto rescued = failed.or_else([](const Error&) { return Expected<int>(42); });
    ASSERT_TRUE(rescued);
    ASSERT_EQ(*rescued, 42);
}

// Rvalue overloads: a move-only T flows through the chain without a copy.

TEST(ExpectedTest, testRvalueValueOrMovesOut) {
    Expected<UniqueInt> ok(std::make_unique<int>(7));
    UniqueInt got = std::move(ok).value_or(nullptr);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, 7);

    Expected<UniqueInt> failed(Error{ResultUnknownError, ""});
    UniqueInt fallback = std::move(failed).value_or(std::make_unique<int>(99));
    ASSERT_TRUE(fallback);
    ASSERT_EQ(*fallback, 99);
}

TEST(ExpectedTest, testRvalueAndThenConsumesValue) {
    Expected<UniqueInt> ok(std::make_unique<int>(5));
    auto r = std::move(ok).and_then([](UniqueInt p) { return Expected<int>(*p + 1); });
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 6);

    Expected<UniqueInt> failed(Error{ResultTimeout, "t"});
    auto propagated = std::move(failed).and_then([](UniqueInt) { return Expected<int>(0); });
    ASSERT_FALSE(propagated);
    ASSERT_EQ(propagated.error().result, ResultTimeout);
}

TEST(ExpectedTest, testRvalueTransformMapsMoveOnly) {
    Expected<UniqueInt> ok(std::make_unique<int>(3));
    auto r = std::move(ok).transform([](UniqueInt p) { return std::make_unique<long>(*p * 10L); });
    ASSERT_TRUE(r);
    ASSERT_EQ(**r, 30L);
}

TEST(ExpectedTest, testRvalueOrElse) {
    Expected<int> failed(Error{ResultUnknownError, ""});
    auto recovered = std::move(failed).or_else([](const Error&) { return Expected<int>(42); });
    ASSERT_TRUE(recovered);
    ASSERT_EQ(*recovered, 42);

    Expected<int> ok(10);
    auto passthrough = std::move(ok).or_else([](const Error&) { return Expected<int>(-1); });
    ASSERT_TRUE(passthrough);
    ASSERT_EQ(*passthrough, 10);
}

TEST(ExpectedTest, testRvalueValueMovesOut) {
    Expected<std::string> e(std::string("hello"));
    std::string s = std::move(e).value();
    ASSERT_EQ(s, "hello");
}

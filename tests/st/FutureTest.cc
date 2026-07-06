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
#include <pulsar/st/Future.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>

using namespace pulsar::st;
using pulsar::st::detail::Promise;

TEST(FutureTest, testGetReturnsCompletedValue) {
    Promise<int> promise;
    Future<int> future = promise.getFuture();
    ASSERT_FALSE(future.isReady());
    promise.setValue(42);
    ASSERT_TRUE(future.isReady());
    auto r = future.get();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 42);
}

TEST(FutureTest, testGetBlocksUntilCompletedFromAnotherThread) {
    Promise<int> promise;
    Future<int> future = promise.getFuture();
    std::thread completer([promise]() { promise.setValue(7); });
    auto r = future.get();
    completer.join();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 7);
}

TEST(FutureTest, testTimedGetTimesOutWhilePending) {
    Promise<int> promise;
    Future<int> future = promise.getFuture();
    auto r = future.get(std::chrono::milliseconds(10));
    ASSERT_FALSE(r.has_value());  // timed out, still pending
    promise.setValue(1);
    auto r2 = future.get(std::chrono::milliseconds(10));
    ASSERT_TRUE(r2.has_value());
    ASSERT_TRUE(*r2);
    ASSERT_EQ(**r2, 1);
}

TEST(FutureTest, testListenerRunsOnCompletion) {
    Promise<int> promise;
    Future<int> future = promise.getFuture();
    int seen = -1;
    future.addListener([&seen](const Expected<int>& r) { seen = r ? *r : -2; });
    ASSERT_EQ(seen, -1);
    promise.setValue(5);
    ASSERT_EQ(seen, 5);
}

TEST(FutureTest, testListenerAfterCompletionRunsSynchronously) {
    Promise<int> promise;
    promise.setValue(9);
    int seen = -1;
    promise.getFuture().addListener([&seen](const Expected<int>& r) { seen = r ? *r : -2; });
    ASSERT_EQ(seen, 9);
}

TEST(FutureTest, testCompleteIsFirstWriterWins) {
    Promise<int> promise;
    ASSERT_TRUE(promise.setValue(1));
    ASSERT_FALSE(promise.setValue(2));
    ASSERT_FALSE(promise.setError(Error{ResultUnknownError, ""}));
    auto r = promise.getFuture().get();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 1);
}

// --- thenApply ---------------------------------------------------------------

TEST(FutureTest, testThenApplyMapsValue) {
    Promise<int> promise;
    Future<std::string> mapped =
        promise.getFuture().thenApply([](const int& x) { return std::to_string(x + 1); });
    promise.setValue(41);
    auto r = mapped.get();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, "42");
}

TEST(FutureTest, testThenApplyPropagatesError) {
    Promise<int> promise;
    bool called = false;
    Future<int> mapped = promise.getFuture().thenApply([&called](const int& x) {
        called = true;
        return x;
    });
    promise.setError(Error{ResultTimeout, "t"});
    auto r = mapped.get();
    ASSERT_FALSE(r);
    ASSERT_EQ(r.error().result, ResultTimeout);
    ASSERT_FALSE(called);
}

TEST(FutureTest, testThenApplyVoidMapper) {
    Promise<int> promise;
    int seen = -1;
    Future<void> done = promise.getFuture().thenApply([&seen](const int& x) { seen = x; });
    promise.setValue(7);
    auto r = done.get();
    ASSERT_TRUE(r);
    ASSERT_EQ(seen, 7);
}

TEST(FutureTest, testThenApplyMoveOnlyMapper) {
    Promise<int> promise;
    auto bonus = std::make_unique<int>(100);
    Future<int> mapped =
        promise.getFuture().thenApply([b = std::move(bonus)](const int& x) { return x + *b; });
    promise.setValue(5);
    auto r = mapped.get();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 105);
}

// --- broken promise ----------------------------------------------------------

TEST(FutureTest, testAbandonedPromiseFailsTheFuture) {
    Future<int> future = [] {
        Promise<int> abandoned;
        return abandoned.getFuture();
    }();
    auto r = future.get();  // must not hang
    ASSERT_FALSE(r);
    ASSERT_EQ(r.error().result, ResultUnknownError);
}

TEST(FutureTest, testAbandonedPromiseCopiesFailOnlyAfterLastCopyDies) {
    Promise<int> outer;
    Future<int> future = outer.getFuture();
    {
        // Intentionally copy just to let it die: destroying one copy of a shared
        // promise must not fail the future. The copy's whole purpose is its scope.
        // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
        Promise<int> copy = outer;
    }
    ASSERT_FALSE(future.isReady());
    outer.setValue(9);
    auto r = future.get();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 9);
}

TEST(FutureTest, testAbandonedVoidPromise) {
    Future<void> future = [] {
        Promise<void> abandoned;
        return abandoned.getFuture();
    }();
    auto r = future.get();
    ASSERT_FALSE(r);
}

TEST(FutureTest, testCompletedPromiseGuardIsNoOp) {
    Future<int> future = [] {
        Promise<int> promise;
        Future<int> f = promise.getFuture();
        promise.setValue(123);
        return f;  // promise dies after completing: value must be preserved
    }();
    auto r = future.get();
    ASSERT_TRUE(r);
    ASSERT_EQ(*r, 123);
}

// --- coroutine awaiter --------------------------------------------------------

namespace {

struct TestTask {
    struct promise_type {
        TestTask get_return_object() { return {}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
};

TestTask awaitInto(Future<int> future, Expected<int>& out, std::atomic<bool>& done) {
    Expected<int> r = co_await future;
    out = r;
    done = true;
}

}  // namespace

TEST(FutureTest, testCoAwaitReadyFuture) {
    Promise<int> promise;
    promise.setValue(11);
    Expected<int> out(0);
    std::atomic<bool> done{false};
    awaitInto(promise.getFuture(), out, done);
    ASSERT_TRUE(done.load());
    ASSERT_TRUE(out);
    ASSERT_EQ(*out, 11);
}

TEST(FutureTest, testCoAwaitSuspendsUntilCompleted) {
    Promise<int> promise;
    Expected<int> out(0);
    std::atomic<bool> done{false};
    awaitInto(promise.getFuture(), out, done);
    ASSERT_FALSE(done.load());  // suspended, not resumed inside await_suspend
    promise.setValue(21);       // completes -> resumes the coroutine
    ASSERT_TRUE(done.load());
    ASSERT_TRUE(out);
    ASSERT_EQ(*out, 21);
}

TEST(FutureTest, testCoAwaitPropagatesError) {
    Promise<int> promise;
    Expected<int> out(0);
    std::atomic<bool> done{false};
    awaitInto(promise.getFuture(), out, done);
    promise.setError(Error{ResultTimeout, "t"});
    ASSERT_TRUE(done.load());
    ASSERT_FALSE(out);
    ASSERT_EQ(out.error().result, ResultTimeout);
}

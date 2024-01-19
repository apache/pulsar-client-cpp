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

#include <atomic>
#include <chrono>
#include <stdexcept>

#include "lib/RetryableOperationCache.h"

namespace pulsar {

using IntFuture = Future<Result, int>;

static int wait(IntFuture future) {
    int value;
    auto result = future.get(value);
    if (result != ResultOk) {
        throw std::runtime_error(strResult(result));
    }
    return value;
}

class CountdownFunc {
    const int result_;
    const int totalRetryCount_ = 3;
    std::atomic_int current_{0};

   public:
    CountdownFunc(int result, int totalRetryCount = 3) : result_(result), totalRetryCount_(totalRetryCount) {}

    CountdownFunc(const CountdownFunc& rhs)
        : result_(rhs.result_), totalRetryCount_(rhs.totalRetryCount_), current_(rhs.current_.load()) {}

    IntFuture operator()() {
        Promise<Result, int> promise;
        if (++current_ < totalRetryCount_) {
            promise.setFailed(ResultRetryable);
        } else {
            promise.setValue(result_);
        }
        return promise.getFuture();
    }
};

class RetryableOperationCacheTest : public ::testing::Test {
   protected:
    void SetUp() override { provider_ = std::make_shared<ExecutorServiceProvider>(1); }

    void TearDown() override {
        provider_->close();
        futures_.clear();
    }

    template <typename T>
    size_t getSize(const RetryableOperationCache<T>& cache) {
        std::lock_guard<std::mutex> lock{cache.mutex_};
        return cache.operations_.size();
    }

    ExecutorServiceProviderPtr provider_;
    std::vector<IntFuture> futures_;
};

}  // namespace pulsar

using namespace pulsar;

TEST_F(RetryableOperationCacheTest, testRetry) {
    auto cache = RetryableOperationCache<int>::create(provider_, std::chrono::seconds(30));
    for (int i = 0; i < 10; i++) {
        futures_.emplace_back(cache->run("key-" + std::to_string(i), CountdownFunc{i * 100}));
    }
    ASSERT_EQ(getSize(*cache), 10);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(wait(futures_[i]), i * 100);
    }
    ASSERT_EQ(getSize(*cache), 0);
}

TEST_F(RetryableOperationCacheTest, testCache) {
    auto cache = RetryableOperationCache<int>::create(provider_, std::chrono::seconds(30));
    constexpr int numKeys = 5;
    for (int i = 0; i < 100; i++) {
        futures_.emplace_back(cache->run("key-" + std::to_string(i % numKeys), CountdownFunc{i * 100}));
    }
    ASSERT_EQ(getSize(*cache), numKeys);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(wait(futures_[i]), (i % numKeys) * 100);
    }
    ASSERT_EQ(getSize(*cache), 0);
}

TEST_F(RetryableOperationCacheTest, testTimeout) {
    auto cache = RetryableOperationCache<int>::create(provider_, std::chrono::seconds(1));
    auto future = cache->run("key", CountdownFunc{0, 1000 /* retry count */});
    try {
        wait(future);
        FAIL();
    } catch (const std::runtime_error& e) {
        ASSERT_STREQ(e.what(), strResult(ResultTimeout));
    }
}

TEST_F(RetryableOperationCacheTest, testClear) {
    auto cache = RetryableOperationCache<int>::create(provider_, std::chrono::seconds(30));
    for (int i = 0; i < 10; i++) {
        futures_.emplace_back(cache->run("key-" + std::to_string(i), CountdownFunc{100}));
    }
    ASSERT_EQ(getSize(*cache), 10);
    cache->clear();
    for (auto&& future : futures_) {
        int value;
        // All cancelled futures complete with ResultDisconnected and the default int value
        ASSERT_EQ(ResultDisconnected, future.get(value));
        ASSERT_EQ(value, 0);
    }
    ASSERT_EQ(getSize(*cache), 0);
}

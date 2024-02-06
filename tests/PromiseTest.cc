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

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "WaitUtils.h"
#include "lib/Future.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

TEST(PromiseTest, testSetValue) {
    Promise<int, std::string> promise;
    std::thread t{[promise] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        promise.setValue("hello");
    }};
    t.detach();

    std::string value;
    ASSERT_EQ(promise.getFuture().get(value), 0);
    ASSERT_EQ(value, "hello");
}

TEST(PromiseTest, testSetFailed) {
    Promise<int, std::string> promise;
    std::thread t{[promise] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        promise.setFailed(-1);
    }};
    t.detach();

    std::string value;
    ASSERT_EQ(promise.getFuture().get(value), -1);
    ASSERT_EQ(value, "");
}

TEST(PromiseTest, testListeners) {
    Promise<int, std::string> promise;
    auto future = promise.getFuture();

    bool resultSetFailed = true;
    bool resultSetValue = true;
    std::vector<int> results;
    std::vector<std::string> values;

    future
        .addListener([promise, &resultSetFailed, &results, &values](int result, const std::string& value) {
            resultSetFailed = promise.setFailed(-1L);
            results.emplace_back(result);
            values.emplace_back(value);
        })
        .addListener([promise, &resultSetValue, &results, &values](int result, const std::string& value) {
            resultSetValue = promise.setValue("WRONG");
            results.emplace_back(result);
            values.emplace_back(value);
        });

    promise.setValue("hello");
    std::string value;
    ASSERT_EQ(future.get(value), 0);
    ASSERT_EQ(value, "hello");

    ASSERT_FALSE(resultSetFailed);
    ASSERT_FALSE(resultSetValue);
    ASSERT_EQ(results, (std::vector<int>(2, 0)));
    ASSERT_EQ(values, (std::vector<std::string>(2, "hello")));
}

TEST(PromiseTest, testListenerDeadlock) {
    Promise<int, int> promise;
    auto future = promise.getFuture();
    auto mutex = std::make_shared<std::mutex>();
    auto done = std::make_shared<std::atomic_bool>(false);

    future.addListener([mutex, done](int, int) {
        LOG_INFO("Listener-1 before acquiring the lock");
        std::lock_guard<std::mutex> lock{*mutex};
        LOG_INFO("Listener-1 after acquiring the lock");
        done->store(true);
    });

    std::thread t1{[mutex, &future] {
        std::lock_guard<std::mutex> lock{*mutex};
        // Make it a great chance that `t2` executes `promise.setValue` first
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Since the future is completed, `Future::get` will be called in `addListener` to get the result
        LOG_INFO("Before adding Listener-2 (acquired the mutex)")
        future.addListener([](int, int) { LOG_INFO("Listener-2 is triggered"); });
        LOG_INFO("After adding Listener-2 (releasing the mutex)");
    }};
    t1.detach();
    std::thread t2{[mutex, promise] {
        // Make there a great chance that `t1` acquires `mutex` first
        std::this_thread::sleep_for(std::chrono::seconds(1));
        LOG_INFO("Before setting value");
        promise.setValue(0);  // the 1st listener is called, which is blocked at acquiring `mutex`
        LOG_INFO("After setting value");
    }};
    t2.detach();

    ASSERT_TRUE(waitUntil(std::chrono::seconds(5000), [done] { return done->load(); }));
}

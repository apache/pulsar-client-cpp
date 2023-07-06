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
#include <string>
#include <thread>
#include <vector>

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

TEST(PromiseTest, testTriggerListeners) {
    InternalState<int, int> state;
    state.addListener([](int, const int&) {
        LOG_INFO("Start task 1...");
        std::this_thread::sleep_for(std::chrono::seconds(1));
        LOG_INFO("Finish task 1...");
    });
    state.addListener([](int, const int&) {
        LOG_INFO("Start task 2...");
        std::this_thread::sleep_for(std::chrono::seconds(1));
        LOG_INFO("Finish task 2...");
    });

    auto start = std::chrono::high_resolution_clock::now();
    auto future1 = std::async(std::launch::async, [&state] { state.triggerListeners(0, 0); });
    auto future2 = std::async(std::launch::async, [&state] { state.triggerListeners(0, 0); });
    future1.wait();
    future2.wait();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::high_resolution_clock::now() - start)
                       .count();
    ASSERT_TRUE(elapsed > 2000) << "elapsed: " << elapsed << "ms";
}

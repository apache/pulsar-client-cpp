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

#include <pulsar/st/Expected.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

// INTERNAL. The set-once completion state behind Future<T>/Promise<T>. It lives
// in pulsar::st::detail and is not part of the public API; applications use
// Future<T> only. It must sit in a header because Future/Promise are templates.

namespace pulsar::st::detail {

template <typename T>
class SharedState {
   public:
    using Listener = std::function<void(const Expected<T>&)>;

    bool complete(Expected<T> result) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (result_.has_value()) {
            return false;
        }
        result_.emplace(std::move(result));
        cond_.notify_all();
        std::vector<Listener> listeners = std::move(listeners_);
        listeners_.clear();
        lock.unlock();
        for (auto& listener : listeners) {
            listener(*result_);
        }
        return true;
    }

    void addListener(Listener listener) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (result_.has_value()) {
            Expected<T> snapshot = *result_;
            lock.unlock();
            listener(snapshot);
        } else {
            listeners_.push_back(std::move(listener));
        }
    }

    Expected<T> get() {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return result_.has_value(); });
        return *result_;
    }

    template <typename Rep, typename Period>
    std::optional<Expected<T>> get(std::chrono::duration<Rep, Period> timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cond_.wait_for(lock, timeout, [this] { return result_.has_value(); })) {
            return std::nullopt;
        }
        return *result_;
    }

    bool isReady() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable cond_;
    std::optional<Expected<T>> result_;
    std::vector<Listener> listeners_;
};

}  // namespace pulsar::st::detail

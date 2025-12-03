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

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

using std::optional;

namespace pulsar {

class SharedFuture {
   public:
    SharedFuture(size_t size) : count_(std::make_shared<std::atomic_size_t>(size)) {}

    bool tryComplete() const { return --*count_ == 0; }

   private:
    std::shared_ptr<std::atomic_size_t> count_;
};

// V must be default constructible and copyable
template <typename K, typename V>
class SynchronizedHashMap {
    using MutexType = std::recursive_mutex;
    using Lock = std::lock_guard<MutexType>;

   public:
    using OptValue = optional<V>;
    using PairVector = std::vector<std::pair<K, V>>;
    using MapType = std::unordered_map<K, V>;
    using Iterator = typename MapType::iterator;

    SynchronizedHashMap() = default;

    SynchronizedHashMap(const PairVector& pairs) {
        for (auto&& kv : pairs) {
            data_.emplace(kv.first, kv.second);
        }
    }

    // Put a new key-value pair if the key does not exist.
    // Return an empty optional if the key already exists or the existing value.
    OptValue putIfAbsent(const K& key, const V& value) {
        Lock lock(mutex_);
        auto pair = data_.emplace(key, value);
        if (pair.second) {
            return {};
        } else {
            return pair.first->second;
        }
    }

    // Put a key-value pair no matter if the key exists.
    void put(const K& key, const V& value) {
        Lock lock(mutex_);
        data_[key] = value;
    }

    void forEach(std::function<void(const K&, const V&)> f) const {
        Lock lock(mutex_);
        for (const auto& kv : data_) {
            f(kv.first, kv.second);
        }
    }

    template <typename ValueFunc>
#if __cplusplus >= 202002L
    requires requires(ValueFunc&& each, const V& value) {
        each(value);
    }
#endif
    void forEachValue(ValueFunc&& each) {
        Lock lock{mutex_};
        for (auto&& kv : data_) {
            each(kv.second);
        }
    }

    // This override provides a convenient approach to execute tasks on each consumer concurrently and
    // supports checking if all tasks are done in the `each` callback.
    //
    // All map values will be passed as the 1st argument to the `each` function. The 2nd argument is a shared
    // future whose `tryComplete` method marks this task as completed. If users want to check if all task are
    // completed in the `each` function, this method must be called.
    //
    // For example, given a `SynchronizedHashMap<int, std::string>` object `m` and the following call:
    //
    // ```c++
    // m.forEachValue([](const std::string& s, SharedFuture future) {
    //   std::cout << s << std::endl;
    //   if (future.tryComplete()) {
    //     std::cout << "done" << std::endl;
    //   }
    // }, [] { std::cout << "empty map" << std::endl; });
    // ```
    //
    // If the map is empty, only "empty map" will be printed. Otherwise, all values will be printed
    // and "done" will be printed after that.
    template <typename ValueFunc, typename EmptyFunc>
#if __cplusplus >= 202002L
    requires requires(ValueFunc&& each, const V& value, SharedFuture count, EmptyFunc emptyFunc) {
        each(value, count);
        emptyFunc();
    }
#endif
    void forEachValue(ValueFunc&& each, EmptyFunc&& emptyFunc) {
        std::unique_lock<MutexType> lock{mutex_};
        if (data_.empty()) {
            lock.unlock();
            emptyFunc();
            return;
        }
        SharedFuture future{data_.size()};
        for (auto&& kv : data_) {
            const auto& value = kv.second;
            each(value, future);
        }
    }

    void clear() {
        Lock lock(mutex_);
        data_.clear();
    }

    // clear the map and apply `f` on each removed value
    void clear(std::function<void(const K&, const V&)> f) {
        MapType data = move();
        for (auto&& kv : data) {
            f(kv.first, kv.second);
        }
    }

    OptValue find(const K& key) const {
        Lock lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            return it->second;
        } else {
            return {};
        }
    }

    OptValue findFirstValueIf(std::function<bool(const V&)> f) const {
        Lock lock(mutex_);
        for (const auto& kv : data_) {
            if (f(kv.second)) {
                return kv.second;
            }
        }
        return {};
    }

    OptValue remove(const K& key) {
        Lock lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            auto result = std::make_optional(std::move(it->second));
            data_.erase(it);
            return result;
        } else {
            return {};
        }
    }

    // This method is only used for test
    PairVector toPairVector() const {
        Lock lock(mutex_);
        PairVector pairs;
        for (auto&& kv : data_) {
            pairs.emplace_back(kv);
        }
        return pairs;
    }

    size_t size() const noexcept {
        Lock lock(mutex_);
        return data_.size();
    }

    MapType move() noexcept {
        Lock lock(mutex_);
        MapType data;
        data_.swap(data);
        return data;
    }

   private:
    MapType data_;
    // Use recursive_mutex to allow methods being called in `forEach`
    mutable MutexType mutex_;
};

}  // namespace pulsar

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

#include <mutex>
#include <queue>

template <typename T>
class SynchronizedQueue {
   public:
    void push(const T& value) {
        std::lock_guard<std::mutex> lock{mutex_};
        queue_.push(value);
    }

    T pop() {
        std::lock_guard<std::mutex> lock{mutex_};
        auto value = queue_.front();
        queue_.pop();
        return value;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock{mutex_};
        return queue_.size();
    }

   private:
    mutable std::mutex mutex_;
    std::queue<T> queue_;
};

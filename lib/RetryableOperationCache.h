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
#include <unordered_map>

#include "ExecutorService.h"
#include "RetryableOperation.h"

namespace pulsar {

template <typename T>
class RetryableOperationCache;

template <typename T>
using RetryableOperationCachePtr = std::shared_ptr<RetryableOperationCache<T>>;

template <typename T>
class RetryableOperationCache : public std::enable_shared_from_this<RetryableOperationCache<T>> {
    friend class LookupServiceTest;
    friend class RetryableOperationCacheTest;
    struct PassKey {
        explicit PassKey() {}
    };

    RetryableOperationCache(ExecutorServiceProviderPtr executorProvider, int timeoutSeconds)
        : executorProvider_(executorProvider), timeoutSeconds_(timeoutSeconds) {}

    using Self = RetryableOperationCache<T>;

   public:
    template <typename... Args>
    explicit RetryableOperationCache(PassKey, Args&&... args)
        : RetryableOperationCache(std::forward<Args>(args)...) {}

    template <typename... Args>
    static std::shared_ptr<Self> create(Args&&... args) {
        return std::make_shared<Self>(PassKey{}, std::forward<Args>(args)...);
    }

    Future<Result, T> run(const std::string& key, std::function<Future<Result, T>()>&& func) {
        std::unique_lock<std::mutex> lock{mutex_};
        auto it = operations_.find(key);
        if (it == operations_.end()) {
            DeadlineTimerPtr timer;
            try {
                timer = executorProvider_->get()->createDeadlineTimer();
            } catch (const std::runtime_error& e) {
                LOG_ERROR("Failed to retry lookup for " << key << ": " << e.what());
                Promise<Result, T> promise;
                promise.setFailed(ResultConnectError);
                return promise.getFuture();
            }

            auto operation = RetryableOperation<T>::create(key, std::move(func), timeoutSeconds_, timer);
            auto future = operation->run();
            operations_[key] = operation;
            lock.unlock();

            std::weak_ptr<Self> weakSelf{this->shared_from_this()};
            future.addListener([this, weakSelf, key, operation](Result, const T&) {
                auto self = weakSelf.lock();
                if (!self) {
                    return;
                }
                std::lock_guard<std::mutex> lock{mutex_};
                operations_.erase(key);
                operation->cancel();
            });

            return future;
        } else {
            return it->second->run();
        }
    }

    void clear() {
        decltype(operations_) operations;
        {
            std::lock_guard<std::mutex> lock{mutex_};
            operations.swap(operations_);
        }
        // cancel() could trigger the listener to erase the key from operations, so we should use a swap way
        // to release the lock here
        for (auto&& kv : operations) {
            kv.second->cancel();
        }
    }

   private:
    ExecutorServiceProviderPtr executorProvider_;
    const int timeoutSeconds_;

    std::unordered_map<std::string, std::shared_ptr<RetryableOperation<T>>> operations_;
    mutable std::mutex mutex_;

    DECLARE_LOG_OBJECT()
};

}  // namespace pulsar

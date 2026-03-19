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

#include <pulsar/Result.h>

#include <atomic>
#include <functional>
#include <memory>

#include "AsioDefines.h"
#include "AsioTimer.h"
#include "Future.h"

namespace pulsar {

template <typename T>
class PendingRequest : public std::enable_shared_from_this<PendingRequest<T>> {
   public:
    PendingRequest(ASIO::steady_timer timer, std::function<void()> timeoutCallback)
        : timer_(std::move(timer)), timeoutCallback_(std::move(timeoutCallback)) {}

    void initialize() {
        timer_.async_wait([this, weakSelf{this->weak_from_this()}](const auto& error) {
            auto self = weakSelf.lock();
            if (!self || error || timeoutDisabled_.load(std::memory_order_acquire)) {
                return;
            }
            timeoutCallback_();
            promise_.setFailed(ResultTimeout);
        });
    }

    void complete(const T& value) {
        promise_.setValue(value);
        cancelTimer(timer_);
    }

    void fail(Result result) {
        promise_.setFailed(result);
        cancelTimer(timer_);
    }

    void disableTimeout() { timeoutDisabled_.store(true, std::memory_order_release); }

    auto getFuture() const { return promise_.getFuture(); }

    ~PendingRequest() { cancelTimer(timer_); }

   private:
    ASIO::steady_timer timer_;
    Promise<Result, T> promise_;
    std::function<void()> timeoutCallback_;
    std::atomic_bool timeoutDisabled_{false};
};

template <typename T>
using PendingRequestPtr = std::shared_ptr<PendingRequest<T>>;

}  // namespace pulsar

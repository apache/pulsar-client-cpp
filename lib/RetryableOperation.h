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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>

#include "Backoff.h"
#include "ExecutorService.h"
#include "Future.h"
#include "LogUtils.h"
#include "ResultUtils.h"
#include "TimeUtils.h"

namespace pulsar {

template <typename T>
class RetryableOperation : public std::enable_shared_from_this<RetryableOperation<T>> {
    struct PassKey {
        explicit PassKey() {}
    };

    RetryableOperation(const std::string& name, std::function<Future<Result, T>()>&& func,
                       TimeDuration timeout, DeadlineTimerPtr timer)
        : name_(name),
          func_(std::move(func)),
          timeout_(timeout),
          backoff_(std::chrono::milliseconds(100), timeout_ + timeout_, std::chrono::milliseconds(0)),
          timer_(timer) {}

   public:
    template <typename... Args>
    explicit RetryableOperation(PassKey, Args&&... args) : RetryableOperation(std::forward<Args>(args)...) {}

    template <typename... Args>
    static std::shared_ptr<RetryableOperation<T>> create(Args&&... args) {
        return std::make_shared<RetryableOperation<T>>(PassKey{}, std::forward<Args>(args)...);
    }

    Future<Result, T> run() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true)) {
            return promise_.getFuture();
        }
        return runImpl(timeout_);
    }

    void cancel() {
        promise_.setFailed(ResultDisconnected);
        ASIO_ERROR ec;
        timer_->cancel(ec);
    }

   private:
    const std::string name_;
    std::function<Future<Result, T>()> func_;
    const TimeDuration timeout_;
    Backoff backoff_;
    Promise<Result, T> promise_;
    std::atomic_bool started_{false};
    DeadlineTimerPtr timer_;

    // Fix the "declared with greater visibility" error for GCC <= 7
#ifdef __GNUC__
    __attribute__((visibility("hidden")))
#endif
    Future<Result, T>
    runImpl(TimeDuration remainingTime) {
        std::weak_ptr<RetryableOperation<T>> weakSelf{this->shared_from_this()};
        func_().addListener([this, weakSelf, remainingTime](Result result, const T& value) {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            if (result == ResultOk) {
                promise_.setValue(value);
                return;
            }
            if (!isResultRetryable(result)) {
                promise_.setFailed(result);
                return;
            }
            if (toMillis(remainingTime) <= 0) {
                promise_.setFailed(ResultTimeout);
                return;
            }

            auto delay = std::min(backoff_.next(), remainingTime);
            timer_->expires_from_now(delay);

            auto nextRemainingTime = remainingTime - delay;
            LOG_INFO("Reschedule " << name_ << " for " << toMillis(delay)
                                   << " ms, remaining time: " << toMillis(nextRemainingTime) << " ms");
            timer_->async_wait([this, weakSelf, nextRemainingTime](const ASIO_ERROR& ec) {
                auto self = weakSelf.lock();
                if (!self) {
                    return;
                }
                if (ec) {
                    if (ec == ASIO::error::operation_aborted) {
                        LOG_DEBUG("Timer for " << name_ << " is cancelled");
                        promise_.setFailed(ResultTimeout);
                    } else {
                        LOG_WARN("Timer for " << name_ << " failed: " << ec.message());
                    }
                } else {
                    LOG_DEBUG("Run operation " << name_ << ", remaining time: " << toMillis(nextRemainingTime)
                                               << " ms");
                    runImpl(nextRemainingTime);
                }
            });
        });
        return promise_.getFuture();
    }

    DECLARE_LOG_OBJECT()
};

}  // namespace pulsar

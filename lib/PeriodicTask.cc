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
#include "PeriodicTask.h"

#include <chrono>

namespace pulsar {

void PeriodicTask::start() {
    if (state_ != Pending) {
        return;
    }
    state_ = Ready;
    if (periodMs_ >= 0) {
        auto weakSelf = weak_from_this();
        timer_->expires_after(std::chrono::milliseconds(periodMs_));
        timer_->async_wait([weakSelf](const ErrorCode& ec) {
            if (auto self = weakSelf.lock()) {
                self->handleTimeout(ec);
            }
        });
    }
}

void PeriodicTask::stop() noexcept {
    State state = Ready;
    if (!state_.compare_exchange_strong(state, Closing)) {
        return;
    }
    cancelTimer(*timer_);
    state_ = Pending;
}

void PeriodicTask::handleTimeout(const ErrorCode& ec) {
    if (state_ != Ready || ec == ASIO::error::operation_aborted) {
        return;
    }

    callback_(ec);

    // state_ may be changed in handleTimeout, so we check state_ again
    if (state_ == Ready) {
        auto self = shared_from_this();
        timer_->expires_after(std::chrono::milliseconds(periodMs_));
        timer_->async_wait([this, self](const ErrorCode& ec) { handleTimeout(ec); });
    }
}

}  // namespace pulsar

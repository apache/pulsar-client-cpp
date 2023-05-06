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

#include "NegativeAcksTracker.h"

#include <functional>
#include <set>

#include "ClientImpl.h"
#include "ConsumerImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "MessageIdUtil.h"
DECLARE_LOG_OBJECT()

namespace pulsar {

NegativeAcksTracker::NegativeAcksTracker(ClientImplPtr client, ConsumerImpl &consumer,
                                         const ConsumerConfiguration &conf)
    : consumer_(consumer),
      timerInterval_(0),
      timer_(client->getIOExecutorProvider()->get()->createDeadlineTimer()) {
    static const long MIN_NACK_DELAY_MILLIS = 100;

    nackDelay_ =
        std::chrono::milliseconds(std::max(conf.getNegativeAckRedeliveryDelayMs(), MIN_NACK_DELAY_MILLIS));
    timerInterval_ = boost::posix_time::milliseconds((long)(nackDelay_.count() / 3));
    LOG_DEBUG("Created negative ack tracker with delay: " << nackDelay_.count()
                                                          << " ms - Timer interval: " << timerInterval_);
}

void NegativeAcksTracker::scheduleTimer() {
    if (closed_) {
        return;
    }
    timer_->expires_from_now(timerInterval_);
    timer_->async_wait(std::bind(&NegativeAcksTracker::handleTimer, this, std::placeholders::_1));
}

void NegativeAcksTracker::handleTimer(const boost::system::error_code &ec) {
    if (ec) {
        // Ignore cancelled events
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    if (nackedMessages_.empty() || !enabledForTesting_) {
        return;
    }

    // Group all the nacked messages into one single re-delivery request
    std::set<MessageId> messagesToRedeliver;

    auto now = Clock::now();

    for (auto it = nackedMessages_.begin(); it != nackedMessages_.end();) {
        if (it->second < now) {
            messagesToRedeliver.insert(it->first);
            it = nackedMessages_.erase(it);
        } else {
            ++it;
        }
    }
    lock.unlock();

    if (!messagesToRedeliver.empty()) {
        consumer_.onNegativeAcksSend(messagesToRedeliver);
        consumer_.redeliverUnacknowledgedMessages(messagesToRedeliver);
    }
    scheduleTimer();
}

void NegativeAcksTracker::add(const MessageId &m) {
    auto msgId = discardBatch(m);
    auto now = Clock::now();

    {
        std::lock_guard<std::mutex> lock{mutex_};
        // Erase batch id to group all nacks from same batch
        nackedMessages_[msgId] = now + nackDelay_;
    }

    scheduleTimer();
}

void NegativeAcksTracker::close() {
    closed_ = true;
    boost::system::error_code ec;
    timer_->cancel(ec);
    std::lock_guard<std::mutex> lock(mutex_);
    nackedMessages_.clear();
}

void NegativeAcksTracker::setEnabledForTesting(bool enabled) {
    enabledForTesting_ = enabled;

    if (enabledForTesting_) {
        scheduleTimer();
    }
}

}  // namespace pulsar

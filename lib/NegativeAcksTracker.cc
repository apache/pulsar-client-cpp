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

#include <cstdint>
#include <functional>
#include <set>
#include <utility>

#include "ClientImpl.h"
#include "ConsumerImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "MessageIdUtil.h"
#include "pulsar/MessageBuilder.h"
#include "pulsar/MessageId.h"
#include "pulsar/MessageIdBuilder.h"
DECLARE_LOG_OBJECT()

namespace pulsar {

NegativeAcksTracker::NegativeAcksTracker(const ClientImplPtr &client, ConsumerImpl &consumer,
                                         const ConsumerConfiguration &conf)
    : consumer_(consumer),
      timerInterval_(0),
      timer_(client->getIOExecutorProvider()->get()->createDeadlineTimer()) {
    static const long MIN_NACK_DELAY_MILLIS = 100;

    nackDelay_ =
        std::chrono::milliseconds(std::max(conf.getNegativeAckRedeliveryDelayMs(), MIN_NACK_DELAY_MILLIS));
    timerInterval_ = std::chrono::milliseconds((long)(nackDelay_.count() / 3));
    nackPrecisionBit_ = conf.getNegativeAckPrecisionBitCnt();
    LOG_DEBUG("Created negative ack tracker with delay: " << nackDelay_.count() << " ms - Timer interval: "
                                                          << timerInterval_.count());
}

void NegativeAcksTracker::scheduleTimer() {
    if (closed_) {
        return;
    }
    auto weakSelf = weak_from_this();
    timer_->expires_after(timerInterval_);
    timer_->async_wait([weakSelf](const ASIO_ERROR &ec) {
        if (auto self = weakSelf.lock()) {
            self->handleTimer(ec);
        }
    });
}

void NegativeAcksTracker::handleTimer(const ASIO_ERROR &ec) {
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

    // The map is sorted by time, so we can exit immediately when we traverse to a time that does not match
    for (auto it = nackedMessages_.begin(); it != nackedMessages_.end();) {
        if (it->first > now) {
            // We are done with all the messages that need to be redelivered
            break;
        }

        const auto &ledgerMap = it->second;
        for (auto ledgerIt = ledgerMap.begin(); ledgerIt != ledgerMap.end(); ++ledgerIt) {
            const auto &entrySet = ledgerIt->second;
            for (auto setIt = entrySet.begin(); setIt != entrySet.end(); ++setIt) {
                messagesToRedeliver.insert(
                    MessageIdBuilder().ledgerId(ledgerIt->first).entryId(*setIt).build());
            }
        }
        it = nackedMessages_.erase(it);
    }
    lock.unlock();

    if (!messagesToRedeliver.empty()) {
        consumer_.onNegativeAcksSend(messagesToRedeliver);
        consumer_.redeliverUnacknowledgedMessages(messagesToRedeliver);
    }
    scheduleTimer();
}

std::chrono::steady_clock::time_point trimLowerBit(const std::chrono::steady_clock::time_point &tp,
                                                   int bits) {
    // get origin timestamp in nanoseconds
    auto timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count();

    // trim lower bits
    auto trimmedTimestamp = timestamp & (~((1LL << bits) - 1));

    return std::chrono::steady_clock::time_point(std::chrono::nanoseconds(trimmedTimestamp));
}

void NegativeAcksTracker::add(const MessageId &m) {
    auto msgId = discardBatch(m);
    auto now = Clock::now();

    {
        std::lock_guard<std::mutex> lock{mutex_};
        auto trimmedTimestamp = trimLowerBit(now + nackDelay_, nackPrecisionBit_);
        // If the timestamp is already in the map, we can just add the message to the existing entry
        // Erase batch id to group all nacks from same batch
        nackedMessages_[trimmedTimestamp][msgId.ledgerId()].insert(msgId.entryId());
    }

    scheduleTimer();
}

void NegativeAcksTracker::close() {
    closed_ = true;
    cancelTimer(*timer_);
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

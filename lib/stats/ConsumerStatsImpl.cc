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

#include "ConsumerStatsImpl.h"

#include "lib/LogUtils.h"
#include "lib/Utils.h"

namespace pulsar {
DECLARE_LOG_OBJECT();

using Lock = std::unique_lock<std::mutex>;

ConsumerStatsImpl::ConsumerStatsImpl(const std::string& consumerStr, DeadlineTimerPtr timer,
                                     unsigned int statsIntervalInSeconds)
    : consumerStr_(consumerStr), timer_(std::move(timer)), statsIntervalInSeconds_(statsIntervalInSeconds) {}

ConsumerStatsImpl::ConsumerStatsImpl(const ConsumerStatsImpl& stats)
    : consumerStr_(stats.consumerStr_),
      numBytesRecieved_(stats.numBytesRecieved_),
      receivedMsgMap_(stats.receivedMsgMap_),
      ackedMsgMap_(stats.ackedMsgMap_),
      totalNumBytesRecieved_(stats.totalNumBytesRecieved_),
      totalReceivedMsgMap_(stats.totalReceivedMsgMap_),
      totalAckedMsgMap_(stats.totalAckedMsgMap_),
      statsIntervalInSeconds_(stats.statsIntervalInSeconds_) {}

void ConsumerStatsImpl::flushAndReset(const ASIO_ERROR& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }

    Lock lock(mutex_);
    std::ostringstream oss;
    oss << *this;
    numBytesRecieved_ = 0;
    receivedMsgMap_.clear();
    ackedMsgMap_.clear();
    lock.unlock();

    scheduleTimer();
    LOG_INFO(oss.str());
}

ConsumerStatsImpl::~ConsumerStatsImpl() { timer_->cancel(); }

void ConsumerStatsImpl::start() { scheduleTimer(); }

void ConsumerStatsImpl::receivedMessage(Message& msg, Result res) {
    Lock lock(mutex_);
    if (res == ResultOk) {
        totalNumBytesRecieved_ += msg.getLength();
        numBytesRecieved_ += msg.getLength();
    }
    receivedMsgMap_[res] += 1;
    totalReceivedMsgMap_[res] += 1;
}

void ConsumerStatsImpl::messageAcknowledged(Result res, CommandAck_AckType ackType, uint32_t ackNums) {
    Lock lock(mutex_);
    ackedMsgMap_[std::make_pair(res, ackType)] += ackNums;
    totalAckedMsgMap_[std::make_pair(res, ackType)] += ackNums;
}

void ConsumerStatsImpl::scheduleTimer() {
    timer_->expires_from_now(std::chrono::seconds(statsIntervalInSeconds_));
    std::weak_ptr<ConsumerStatsImpl> weakSelf{shared_from_this()};
    timer_->async_wait([this, weakSelf](const ASIO_ERROR& ec) {
        auto self = weakSelf.lock();
        if (!self) {
            return;
        }
        flushAndReset(ec);
    });
}

std::ostream& operator<<(std::ostream& os,
                         const std::map<std::pair<Result, CommandAck_AckType>, unsigned long>& m) {
    os << "{";
    for (std::map<std::pair<Result, CommandAck_AckType>, unsigned long>::const_iterator it = m.begin();
         it != m.end(); it++) {
        os << "[Key: {"
           << "Result: " << strResult((it->first).first) << ", ackType: " << (it->first).second
           << "}, Value: " << it->second << "], ";
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const ConsumerStatsImpl& obj) {
    os << "Consumer " << obj.consumerStr_ << ", ConsumerStatsImpl ("
       << "numBytesRecieved_ = " << obj.numBytesRecieved_
       << ", totalNumBytesRecieved_ = " << obj.totalNumBytesRecieved_
       << ", receivedMsgMap_ = " << obj.receivedMsgMap_ << ", ackedMsgMap_ = " << obj.ackedMsgMap_
       << ", totalReceivedMsgMap_ = " << obj.totalReceivedMsgMap_
       << ", totalAckedMsgMap_ = " << obj.totalAckedMsgMap_ << ")";
    return os;
}
} /* namespace pulsar */

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

#include "AckGroupingTrackerEnabled.h"

#include <climits>
#include <memory>
#include <mutex>

#include "ClientConnection.h"
#include "ClientImpl.h"
#include "Commands.h"
#include "ExecutorService.h"
#include "HandlerBase.h"
#include "MessageIdUtil.h"

namespace pulsar {

// Define a customized compare logic whose difference with the default compare logic of MessageId is:
// When two MessageId objects are in the same entry, if only one of them is a message in the batch, treat
// it as a smaller one.
static int compare(const MessageId& lhs, const MessageId& rhs) {
    int result = compareLedgerAndEntryId(lhs, rhs);
    if (result != 0) {
        return result;
    } else {
        return internal::compare(lhs.batchIndex() < 0 ? INT_MAX : lhs.batchIndex(),
                                 rhs.batchIndex() < 0 ? INT_MAX : rhs.batchIndex());
    }
}

void AckGroupingTrackerEnabled::start() { this->scheduleTimer(); }

bool AckGroupingTrackerEnabled::isDuplicate(const MessageId& msgId) {
    {
        // Check if the message ID is already ACKed by a previous (or pending) cumulative request.
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        if (compare(msgId, this->nextCumulativeAckMsgId_) <= 0) {
            return true;
        }
    }

    // Check existence in pending individual ACKs set.
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    return this->pendingIndividualAcks_.count(msgId) > 0;
}

void AckGroupingTrackerEnabled::addAcknowledge(const MessageId& msgId, const ResultCallback& callback) {
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    this->pendingIndividualAcks_.insert(msgId);
    if (waitResponse_) {
        this->pendingIndividualCallbacks_.emplace_back(callback);
    } else if (callback) {
        callback(ResultOk);
    }
    if (this->ackGroupingMaxSize_ > 0 && this->pendingIndividualAcks_.size() >= this->ackGroupingMaxSize_) {
        this->flush();
    }
}

void AckGroupingTrackerEnabled::addAcknowledgeList(const MessageIdList& msgIds,
                                                   const ResultCallback& callback) {
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    for (const auto& msgId : msgIds) {
        this->pendingIndividualAcks_.emplace(msgId);
    }
    if (waitResponse_) {
        this->pendingIndividualCallbacks_.emplace_back(callback);
    } else if (callback) {
        callback(ResultOk);
    }
    if (this->ackGroupingMaxSize_ > 0 && this->pendingIndividualAcks_.size() >= this->ackGroupingMaxSize_) {
        this->flush();
    }
}

void AckGroupingTrackerEnabled::addAcknowledgeCumulative(const MessageId& msgId,
                                                         const ResultCallback& callback) {
    std::unique_lock<std::mutex> lock(this->mutexCumulativeAckMsgId_);
    bool completeCallback = true;
    if (compare(msgId, this->nextCumulativeAckMsgId_) > 0) {
        this->nextCumulativeAckMsgId_ = msgId;
        this->requireCumulativeAck_ = true;
        // Trigger the previous pending callback
        if (latestCumulativeCallback_) {
            latestCumulativeCallback_(ResultOk);
        }
        if (waitResponse_) {
            // Move the callback to latestCumulativeCallback_ so that it will be triggered when receiving the
            // AckResponse or being replaced by a newer MessageId
            latestCumulativeCallback_ = callback;
            completeCallback = false;
        } else {
            latestCumulativeCallback_ = nullptr;
        }
    }
    lock.unlock();
    if (callback && completeCallback) {
        callback(ResultOk);
    }
}

AckGroupingTrackerEnabled::~AckGroupingTrackerEnabled() {
    isClosed_ = true;
    this->flush();
    std::lock_guard<std::mutex> lock(this->mutexTimer_);
    if (this->timer_) {
        ASIO_ERROR ec;
        this->timer_->cancel(ec);
    }
}

void AckGroupingTrackerEnabled::flush() {
    // Send ACK for cumulative ACK requests.
    {
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        if (this->requireCumulativeAck_) {
            this->doImmediateAck(this->nextCumulativeAckMsgId_, this->latestCumulativeCallback_,
                                 CommandAck_AckType_Cumulative);
            this->latestCumulativeCallback_ = nullptr;
            this->requireCumulativeAck_ = false;
        }
    }

    // Send ACK for individual ACK requests.
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    if (!this->pendingIndividualAcks_.empty()) {
        std::vector<ResultCallback> callbacks;
        callbacks.swap(this->pendingIndividualCallbacks_);
        auto callback = [callbacks](Result result) {
            for (auto&& callback : callbacks) {
                callback(result);
            }
        };
        this->doImmediateAck(this->pendingIndividualAcks_, callback);
        this->pendingIndividualAcks_.clear();
    }
}

void AckGroupingTrackerEnabled::flushAndClean() {
    this->flush();
    {
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        this->nextCumulativeAckMsgId_ = MessageId::earliest();
        this->latestCumulativeCallback_ = nullptr;
        this->requireCumulativeAck_ = false;
    }
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    this->pendingIndividualAcks_.clear();
}

void AckGroupingTrackerEnabled::scheduleTimer() {
    if (isClosed_) {
        return;
    }

    std::lock_guard<std::mutex> lock(this->mutexTimer_);
    this->timer_ = this->executor_->createDeadlineTimer();
    this->timer_->expires_from_now(std::chrono::milliseconds(std::max(1L, this->ackGroupingTimeMs_)));
    std::weak_ptr<AckGroupingTracker> weakSelf = shared_from_this();
    this->timer_->async_wait([this, weakSelf](const ASIO_ERROR& ec) -> void {
        auto self = weakSelf.lock();
        if (self && !ec) {
            this->flush();
            this->scheduleTimer();
        }
    });
}

}  // namespace pulsar

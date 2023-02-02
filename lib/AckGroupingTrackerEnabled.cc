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
#include <mutex>

#include "ClientConnection.h"
#include "ClientImpl.h"
#include "Commands.h"
#include "ExecutorService.h"
#include "HandlerBase.h"
#include "LogUtils.h"
#include "MessageIdUtil.h"

namespace pulsar {

DECLARE_LOG_OBJECT();

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

AckGroupingTrackerEnabled::AckGroupingTrackerEnabled(ClientImplPtr clientPtr,
                                                     const HandlerBasePtr& handlerPtr, uint64_t consumerId,
                                                     long ackGroupingTimeMs, long ackGroupingMaxSize)
    : AckGroupingTracker(),
      state_(NotStarted),
      handlerWeakPtr_(handlerPtr),
      consumerId_(consumerId),
      nextCumulativeAckMsgId_(MessageId::earliest()),
      requireCumulativeAck_(false),
      mutexCumulativeAckMsgId_(),
      pendingIndividualAcks_(),
      rmutexPendingIndAcks_(),
      ackGroupingTimeMs_(ackGroupingTimeMs),
      ackGroupingMaxSize_(ackGroupingMaxSize),
      executor_(clientPtr->getIOExecutorProvider()->get()),
      timer_(),
      mutexTimer_() {
    LOG_DEBUG("ACK grouping is enabled, grouping time " << ackGroupingTimeMs << "ms, grouping max size "
                                                        << ackGroupingMaxSize);
}

void AckGroupingTrackerEnabled::start() {
    state_ = Ready;
    this->scheduleTimer();
}

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

void AckGroupingTrackerEnabled::addAcknowledge(const MessageId& msgId) {
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    this->pendingIndividualAcks_.insert(msgId);
    if (this->ackGroupingMaxSize_ > 0 && this->pendingIndividualAcks_.size() >= this->ackGroupingMaxSize_) {
        this->flush();
    }
}

void AckGroupingTrackerEnabled::addAcknowledgeList(const MessageIdList& msgIds) {
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    for (const auto& msgId : msgIds) {
        this->pendingIndividualAcks_.emplace(msgId);
    }
    if (this->ackGroupingMaxSize_ > 0 && this->pendingIndividualAcks_.size() >= this->ackGroupingMaxSize_) {
        this->flush();
    }
}

void AckGroupingTrackerEnabled::addAcknowledgeCumulative(const MessageId& msgId) {
    std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
    if (compare(msgId, this->nextCumulativeAckMsgId_) > 0) {
        this->nextCumulativeAckMsgId_ = msgId;
        this->requireCumulativeAck_ = true;
    }
}

void AckGroupingTrackerEnabled::close() {
    state_ = Closed;
    this->flush();
    std::lock_guard<std::mutex> lock(this->mutexTimer_);
    if (this->timer_) {
        boost::system::error_code ec;
        this->timer_->cancel(ec);
    }
}

void AckGroupingTrackerEnabled::flush() {
    auto handler = handlerWeakPtr_.lock();
    if (!handler) {
        LOG_DEBUG("Reference to the HandlerBase is not valid.");
        return;
    }
    auto cnx = handler->getCnx().lock();
    if (cnx == nullptr) {
        LOG_DEBUG("Connection is not ready, grouping ACK failed.");
        return;
    }

    // Send ACK for cumulative ACK requests.
    {
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        if (this->requireCumulativeAck_) {
            if (!this->doImmediateAck(cnx, this->consumerId_, this->nextCumulativeAckMsgId_,
                                      CommandAck_AckType_Cumulative)) {
                // Failed to send ACK.
                LOG_WARN("Failed to send cumulative ACK.");
                return;
            }
            this->requireCumulativeAck_ = false;
        }
    }

    // Send ACK for individual ACK requests.
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    if (!this->pendingIndividualAcks_.empty()) {
        this->doImmediateAck(cnx, consumerId_, this->pendingIndividualAcks_);
        this->pendingIndividualAcks_.clear();
    }
}

void AckGroupingTrackerEnabled::flushAndClean() {
    this->flush();
    {
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        this->nextCumulativeAckMsgId_ = MessageId::earliest();
        this->requireCumulativeAck_ = false;
    }
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    this->pendingIndividualAcks_.clear();
}

void AckGroupingTrackerEnabled::scheduleTimer() {
    if (state_ != Ready) {
        return;
    }

    std::lock_guard<std::mutex> lock(this->mutexTimer_);
    this->timer_ = this->executor_->createDeadlineTimer();
    this->timer_->expires_from_now(boost::posix_time::milliseconds(std::max(1L, this->ackGroupingTimeMs_)));
    auto self = shared_from_this();
    this->timer_->async_wait([this, self](const boost::system::error_code& ec) -> void {
        if (!ec) {
            this->flush();
            this->scheduleTimer();
        }
    });
}

}  // namespace pulsar

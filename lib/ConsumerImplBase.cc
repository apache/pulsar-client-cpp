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
#include "ConsumerImplBase.h"

#include "ExecutorService.h"
#include "LogUtils.h"
#include "TimeUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

ConsumerImplBase::ConsumerImplBase(const ClientImplPtr& client, const std::string& topic, Backoff backoff,
                                   const ConsumerConfiguration& conf,
                                   const ExecutorServicePtr& listenerExecutor)
    : HandlerBase(client, topic, backoff),
      listenerExecutor_(listenerExecutor),
      batchReceivePolicy_(conf.getBatchReceivePolicy()),
      consumerName_(conf.getConsumerName()) {
    const auto& userBatchReceivePolicy = conf.getBatchReceivePolicy();
    if (userBatchReceivePolicy.getMaxNumMessages() > conf.getReceiverQueueSize()) {
        batchReceivePolicy_ =
            BatchReceivePolicy(conf.getReceiverQueueSize(), userBatchReceivePolicy.getMaxNumBytes(),
                               userBatchReceivePolicy.getTimeoutMs());
        LOG_WARN("BatchReceivePolicy maxNumMessages: {" << userBatchReceivePolicy.getMaxNumMessages()
                                                        << "} is greater than maxReceiverQueueSize: {"
                                                        << conf.getReceiverQueueSize()
                                                        << "}, reset to "
                                                           "maxReceiverQueueSize. ");
    }
    batchReceiveTimer_ = listenerExecutor_->createDeadlineTimer();
}

void ConsumerImplBase::triggerBatchReceiveTimerTask(long timeoutMs) {
    if (timeoutMs > 0) {
        batchReceiveTimer_->expires_from_now(std::chrono::milliseconds(timeoutMs));
        std::weak_ptr<ConsumerImplBase> weakSelf{shared_from_this()};
        batchReceiveTimer_->async_wait([weakSelf](const ASIO_ERROR& ec) {
            auto self = weakSelf.lock();
            if (self && !ec) {
                self->doBatchReceiveTimeTask();
            }
        });
    }
}

void ConsumerImplBase::doBatchReceiveTimeTask() {
    if (state_ != Ready) {
        return;
    }

    bool hasPendingReceives = false;
    long timeToWaitMs;

    Lock lock(batchPendingReceiveMutex_);
    while (!batchPendingReceives_.empty()) {
        OpBatchReceive& batchReceive = batchPendingReceives_.front();
        long diff =
            batchReceivePolicy_.getTimeoutMs() - (TimeUtils::currentTimeMillis() - batchReceive.createAt_);
        if (diff <= 0) {
            notifyBatchPendingReceivedCallback(popBatchReceiveCallback());
        } else {
            hasPendingReceives = true;
            timeToWaitMs = diff;
            break;
        }
    }
    lock.unlock();

    if (hasPendingReceives) {
        triggerBatchReceiveTimerTask(timeToWaitMs);
    }
}

void ConsumerImplBase::failPendingBatchReceiveCallback() {
    Lock lock(batchPendingReceiveMutex_);
    while (!batchPendingReceives_.empty()) {
        auto callback = popBatchReceiveCallback();
        listenerExecutor_->postWork([callback]() { callback(ResultAlreadyClosed, {}); });
    }
}

void ConsumerImplBase::notifyBatchPendingReceivedCallback() {
    Lock lock(batchPendingReceiveMutex_);
    if (!batchPendingReceives_.empty()) {
        auto callback = popBatchReceiveCallback();
        lock.unlock();
        notifyBatchPendingReceivedCallback(callback);
    }
}

void ConsumerImplBase::batchReceiveAsync(const BatchReceiveCallback& callback) {
    // fail the callback if consumer is closing or closed
    if (state_ != Ready) {
        callback(ResultAlreadyClosed, Messages());
        return;
    }

    Lock batchOptionLock(batchReceiveOptionMutex_);
    if (hasEnoughMessagesForBatchReceive()) {
        notifyBatchPendingReceivedCallback(callback);
        batchOptionLock.unlock();
    } else {
        OpBatchReceive opBatchReceive(callback);
        Lock lock(batchPendingReceiveMutex_);
        batchPendingReceives_.emplace(opBatchReceive);
        lock.unlock();
        triggerBatchReceiveTimerTask(batchReceivePolicy_.getTimeoutMs());
    }
}

OpBatchReceive::OpBatchReceive(const BatchReceiveCallback& batchReceiveCallback)
    : batchReceiveCallback_(batchReceiveCallback), createAt_(TimeUtils::currentTimeMillis()) {}

} /* namespace pulsar */

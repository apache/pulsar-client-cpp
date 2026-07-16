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
#include "ReceiveQueue.h"

#include <utility>

#include "MessageImpl.h"
#include "lib/ExecutorService.h"

namespace pulsar::st {

ReceiveQueue::ReceiveQueue(pulsar::ExecutorServicePtr executor, std::size_t capacity)
    : executor_(std::move(executor)), capacity_(capacity) {}

std::deque<detail::Promise<void>> ReceiveQueue::takeCapacityWaitersIfRoomLocked() {
    std::deque<detail::Promise<void>> toSignal;
    if (buffer_.size() < capacity_ && !capacityWaiters_.empty()) {
        toSignal = std::move(capacityWaiters_);
        capacityWaiters_.clear();
    }
    return toSignal;
}

Future<MessageImplPtr> ReceiveQueue::receiveAsync() {
    detail::Promise<MessageImplPtr> promise;
    MessageImplPtr message;
    std::deque<detail::Promise<void>> toSignal;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            promise.setError(Error{ResultAlreadyClosed, "consumer is closed"});
            return promise.getFuture();
        }
        if (!buffer_.empty()) {
            message = std::move(buffer_.front());
            buffer_.pop_front();
            toSignal = takeCapacityWaitersIfRoomLocked();
        } else {
            pendingReceives_.emplace(nextReceiveId_++, promise);
        }
    }
    for (auto& waiter : toSignal) waiter.setSuccess();
    if (message) promise.setValue(std::move(message));
    return promise.getFuture();
}

Future<MessageImplPtr> ReceiveQueue::receiveAsync(std::chrono::milliseconds timeout) {
    detail::Promise<MessageImplPtr> promise;
    MessageImplPtr message;
    std::deque<detail::Promise<void>> toSignal;
    std::uint64_t receiveId = 0;
    bool parked = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            promise.setError(Error{ResultAlreadyClosed, "consumer is closed"});
            return promise.getFuture();
        }
        if (!buffer_.empty()) {
            message = std::move(buffer_.front());
            buffer_.pop_front();
            toSignal = takeCapacityWaitersIfRoomLocked();
        } else {
            receiveId = nextReceiveId_++;
            pendingReceives_.emplace(receiveId, promise);
            parked = true;
        }
    }
    for (auto& waiter : toSignal) waiter.setSuccess();
    if (message) {
        promise.setValue(std::move(message));
        return promise.getFuture();
    }
    if (parked) {
        auto timer = executor_->createDeadlineTimer();
        timer->expires_from_now(timeout);
        auto self = shared_from_this();  // keep the queue alive until the timer fires
        timer->async_wait([self, receiveId, promise, timer](const ASIO_ERROR& ec) {
            if (ec) return;  // cancelled
            {
                std::lock_guard<std::mutex> lock(self->mutex_);
                auto it = self->pendingReceives_.find(receiveId);
                if (it == self->pendingReceives_.end()) return;  // a message was delivered first
                self->pendingReceives_.erase(it);
            }
            promise.setError(Error{ResultTimeout, "receive timed out"});
        });
    }
    return promise.getFuture();
}

Future<void> ReceiveQueue::offer(MessageImplPtr message) {
    detail::Promise<MessageImplPtr> receiver;
    bool deliver = false;
    detail::Promise<void> capacityPromise;
    bool hasRoom = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            hasRoom = true;
        } else {
            if (!pendingReceives_.empty()) {
                auto oldest = pendingReceives_.begin();  // FIFO: lowest id
                receiver = std::move(oldest->second);
                pendingReceives_.erase(oldest);
                deliver = true;
            } else {
                buffer_.push_back(std::move(message));
            }
            if (buffer_.size() < capacity_) {
                hasRoom = true;
            } else {
                capacityWaiters_.push_back(capacityPromise);
            }
        }
    }
    if (deliver) receiver.setValue(std::move(message));
    if (hasRoom) {
        detail::Promise<void> ready;
        ready.setSuccess();
        return ready.getFuture();
    }
    return capacityPromise.getFuture();
}

void ReceiveQueue::close() {
    std::map<std::uint64_t, detail::Promise<MessageImplPtr>> pending;
    std::deque<detail::Promise<void>> waiters;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) return;
        closed_ = true;
        pending.swap(pendingReceives_);
        waiters.swap(capacityWaiters_);
        buffer_.clear();
    }
    for (auto& [id, promise] : pending) promise.setError(Error{ResultAlreadyClosed, "consumer is closed"});
    for (auto& waiter : waiters) waiter.setSuccess();  // let segment loops re-arm and see closed
}

}  // namespace pulsar::st

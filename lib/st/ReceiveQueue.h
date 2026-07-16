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

#include <pulsar/st/Future.h>
#include <pulsar/st/detail/MessageCore.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>

namespace pulsar {
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;
}  // namespace pulsar

namespace pulsar::st {

/**
 * The fan-in mux behind a queue/stream consumer: many per-segment receive loops `offer()`
 * messages; the user `receiveAsync()`s them one at a time in FIFO order.
 *
 * Bounded to avoid unbounded buffering when the user is slow: `offer()` returns a future that
 * completes only once the queue has room, and each segment loop awaits it before re-arming its
 * own `receiveAsync()` — so a slow consumer throttles the underlying segment consumers' flow
 * control rather than piling messages up in memory.
 *
 * A default (untimed) receive parks a promise until a message arrives; a timed receive fails that
 * promise with `ResultTimeout` when the deadline elapses. `close()` fails every waiter.
 */
class ReceiveQueue : public std::enable_shared_from_this<ReceiveQueue> {
   public:
    ReceiveQueue(pulsar::ExecutorServicePtr executor, std::size_t capacity);

    Future<MessageImplPtr> receiveAsync();
    Future<MessageImplPtr> receiveAsync(std::chrono::milliseconds timeout);

    /** Deliver a message; the returned future completes when there is room for the next offer. */
    Future<void> offer(MessageImplPtr message);

    /** Fail every pending receive (and release capacity waiters). Idempotent. */
    void close();

   private:
    // Signal capacity waiters if the buffer has drained below capacity. Caller holds mutex_;
    // the returned promises must be completed after releasing it.
    std::deque<detail::Promise<void>> takeCapacityWaitersIfRoomLocked();

    const pulsar::ExecutorServicePtr executor_;
    const std::size_t capacity_;

    std::mutex mutex_;
    std::deque<MessageImplPtr> buffer_;                                         // guarded by mutex_
    std::map<std::uint64_t, detail::Promise<MessageImplPtr>> pendingReceives_;  // guarded; FIFO by id
    std::deque<detail::Promise<void>> capacityWaiters_;                         // guarded by mutex_
    std::uint64_t nextReceiveId_ = 0;                                           // guarded by mutex_
    bool closed_ = false;                                                       // guarded by mutex_
};

using ReceiveQueuePtr = std::shared_ptr<ReceiveQueue>;

}  // namespace pulsar::st

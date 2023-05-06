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

#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/MessageId.h>

#include <atomic>
#include <boost/asio/deadline_timer.hpp>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>

#include "TestUtil.h"

namespace pulsar {

class ConsumerImpl;
class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using DeadlineTimerPtr = std::shared_ptr<boost::asio::deadline_timer>;
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;

class NegativeAcksTracker {
   public:
    NegativeAcksTracker(ClientImplPtr client, ConsumerImpl &consumer, const ConsumerConfiguration &conf);

    NegativeAcksTracker(const NegativeAcksTracker &) = delete;

    NegativeAcksTracker &operator=(const NegativeAcksTracker &) = delete;

    void add(const MessageId &m);

    void close();

    void setEnabledForTesting(bool enabled);

   private:
    void scheduleTimer();
    void handleTimer(const boost::system::error_code &ec);

    ConsumerImpl &consumer_;
    std::mutex mutex_;

    std::chrono::milliseconds nackDelay_;
    boost::posix_time::milliseconds timerInterval_;
    typedef typename std::chrono::steady_clock Clock;
    std::map<MessageId, Clock::time_point> nackedMessages_;

    const DeadlineTimerPtr timer_;
    std::atomic_bool closed_{false};
    std::atomic_bool enabledForTesting_{true};  // to be able to test deterministically

    FRIEND_TEST(ConsumerTest, testNegativeAcksTrackerClose);
};

}  // namespace pulsar

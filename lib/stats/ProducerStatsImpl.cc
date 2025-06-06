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

#include "ProducerStatsImpl.h"

#include <array>
#include <chrono>

#include "lib/ExecutorService.h"
#include "lib/LogUtils.h"
#include "lib/TimeUtils.h"
#include "lib/Utils.h"

namespace pulsar {
DECLARE_LOG_OBJECT();

static const std::array<double, 4> probs = {{0.5, 0.9, 0.99, 0.999}};

std::string ProducerStatsImpl::latencyToString(const LatencyAccumulator& obj) {
    boost::accumulators::detail::extractor_result<
        LatencyAccumulator, boost::accumulators::tag::extended_p_square>::type latencies =
        boost::accumulators::extended_p_square(obj);
    std::stringstream os;
    os << "Latencies [ 50pct: " << latencies[0] / 1e3 << "ms"
       << ", 90pct: " << latencies[1] / 1e3 << "ms"
       << ", 99pct: " << latencies[2] / 1e3 << "ms"
       << ", 99.9pct: " << latencies[3] / 1e3 << "ms"
       << "]";
    return os.str();
}

ProducerStatsImpl::ProducerStatsImpl(std::string producerStr, const ExecutorServicePtr& executor,
                                     unsigned int statsIntervalInSeconds)
    : producerStr_(std::move(producerStr)),
      latencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs),
      totalLatencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs),
      timer_(executor->createDeadlineTimer()),
      statsIntervalInSeconds_(statsIntervalInSeconds) {}

ProducerStatsImpl::ProducerStatsImpl(const ProducerStatsImpl& stats)
    : producerStr_(stats.producerStr_),
      numMsgsSent_(stats.numMsgsSent_),
      numBytesSent_(stats.numBytesSent_),
      sendMap_(stats.sendMap_),
      latencyAccumulator_(stats.latencyAccumulator_),
      totalMsgsSent_(stats.totalMsgsSent_),
      totalBytesSent_(stats.totalBytesSent_),
      totalSendMap_(stats.totalSendMap_),
      totalLatencyAccumulator_(stats.totalLatencyAccumulator_),
      statsIntervalInSeconds_(stats.statsIntervalInSeconds_) {}

void ProducerStatsImpl::start() { scheduleTimer(); }

void ProducerStatsImpl::flushAndReset(const ASIO_ERROR& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    std::ostringstream oss;
    oss << *this;
    numMsgsSent_ = 0;
    numBytesSent_ = 0;
    sendMap_.clear();
    latencyAccumulator_ =
        LatencyAccumulator(boost::accumulators::tag::extended_p_square::probabilities = probs);
    lock.unlock();

    scheduleTimer();
    LOG_INFO(oss.str());
}

void ProducerStatsImpl::messageSent(const Message& msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    numMsgsSent_++;
    totalMsgsSent_++;
    numBytesSent_ += msg.getLength();
    totalBytesSent_ += msg.getLength();
}

void ProducerStatsImpl::messageReceived(Result res, const ptime& publishTime) {
    auto currentTime = TimeUtils::now();
    double diffInMicros =
        std::chrono::duration_cast<std::chrono::microseconds>(currentTime - publishTime).count();
    std::lock_guard<std::mutex> lock(mutex_);
    totalLatencyAccumulator_(diffInMicros);
    latencyAccumulator_(diffInMicros);
    sendMap_[res] += 1;       // Value will automatically be initialized to 0 in the constructor
    totalSendMap_[res] += 1;  // Value will automatically be initialized to 0 in the constructor
}

ProducerStatsImpl::~ProducerStatsImpl() { timer_->cancel(); }

void ProducerStatsImpl::scheduleTimer() {
    timer_->expires_from_now(std::chrono::seconds(statsIntervalInSeconds_));
    std::weak_ptr<ProducerStatsImpl> weakSelf{shared_from_this()};
    timer_->async_wait([this, weakSelf](const ASIO_ERROR& ec) {
        auto self = weakSelf.lock();
        if (!self) {
            return;
        }
        flushAndReset(ec);
    });
}

std::ostream& operator<<(std::ostream& os, const ProducerStatsImpl& obj) {
    os << "Producer " << obj.producerStr_ << ", ProducerStatsImpl ("
       << "numMsgsSent_ = " << obj.numMsgsSent_ << ", numBytesSent_ = " << obj.numBytesSent_
       << ", sendMap_ = " << obj.sendMap_
       << ", latencyAccumulator_ = " << ProducerStatsImpl::latencyToString(obj.latencyAccumulator_)
       << ", totalMsgsSent_ = " << obj.totalMsgsSent_ << ", totalBytesSent_ = " << obj.totalBytesSent_
       << ", totalAcksReceived_ = "
       << ", totalSendMap_ = " << obj.totalSendMap_
       << ", totalLatencyAccumulator_ = " << ProducerStatsImpl::latencyToString(obj.totalLatencyAccumulator_)
       << ")";
    return os;
}
}  // namespace pulsar

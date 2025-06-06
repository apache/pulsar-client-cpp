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

#ifndef PULSAR_PRODUCER_STATS_IMPL_HEADER
#define PULSAR_PRODUCER_STATS_IMPL_HEADER

#include <map>

#if BOOST_VERSION >= 106400
#include <boost/serialization/array_wrapper.hpp>
#endif
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/framework/accumulator_set.hpp>
#include <boost/accumulators/framework/features.hpp>
#include <boost/accumulators/statistics.hpp>
#include <boost/accumulators/statistics/extended_p_square.hpp>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "ProducerStatsBase.h"
#include "lib/AsioTimer.h"

namespace pulsar {

class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;

typedef boost::accumulators::accumulator_set<
    double,
    boost::accumulators::stats<boost::accumulators::tag::mean, boost::accumulators::tag::extended_p_square> >
    LatencyAccumulator;

class ProducerStatsImpl : public std::enable_shared_from_this<ProducerStatsImpl>, public ProducerStatsBase {
   private:
    std::string producerStr_;

    unsigned long numMsgsSent_ = 0;
    unsigned long numBytesSent_ = 0;
    std::map<Result, unsigned long> sendMap_;
    LatencyAccumulator latencyAccumulator_;

    unsigned long totalMsgsSent_ = 0;
    unsigned long totalBytesSent_ = 0;
    std::map<Result, unsigned long> totalSendMap_;
    LatencyAccumulator totalLatencyAccumulator_;

    const DeadlineTimerPtr timer_;
    std::mutex mutex_;
    unsigned int statsIntervalInSeconds_;

    friend std::ostream& operator<<(std::ostream&, const ProducerStatsImpl&);
    friend std::ostream& operator<<(std::ostream&, const std::map<Result, unsigned long>&);
    friend class PulsarFriend;

    static std::string latencyToString(const LatencyAccumulator&);

    void scheduleTimer();

   public:
    ProducerStatsImpl(std::string, const ExecutorServicePtr&, unsigned int);

    ProducerStatsImpl(const ProducerStatsImpl& stats);

    void start() override;

    void flushAndReset(const ASIO_ERROR&);

    void messageSent(const Message&) override;

    void messageReceived(Result, const ptime&) override;

    ~ProducerStatsImpl();

    inline unsigned long getNumMsgsSent() { return numMsgsSent_; }

    inline unsigned long getNumBytesSent() { return numBytesSent_; }

    inline std::map<Result, unsigned long> getSendMap() { return sendMap_; }

    inline unsigned long getTotalMsgsSent() { return totalMsgsSent_; }

    inline unsigned long getTotalBytesSent() { return totalBytesSent_; }

    inline std::map<Result, unsigned long> getTotalSendMap() { return totalSendMap_; }

    inline LatencyAccumulator getLatencyAccumulator() { return latencyAccumulator_; }

    inline LatencyAccumulator getTotalLatencyAccumulator() { return totalLatencyAccumulator_; }
};
typedef std::shared_ptr<ProducerStatsImpl> ProducerStatsImplPtr;
}  // namespace pulsar

#endif  // PULSAR_PRODUCER_STATS_IMPL_HEADER

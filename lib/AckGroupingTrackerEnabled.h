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
#ifndef LIB_ACKGROUPINGTRACKERENABLED_H_
#define LIB_ACKGROUPINGTRACKERENABLED_H_

#include <pulsar/MessageId.h>

#include <atomic>
#include <boost/asio/deadline_timer.hpp>
#include <cstdint>
#include <mutex>
#include <set>

#include "AckGroupingTracker.h"

namespace pulsar {

class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using DeadlineTimerPtr = std::shared_ptr<boost::asio::deadline_timer>;
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;
class HandlerBase;
using HandlerBasePtr = std::shared_ptr<HandlerBase>;
using HandlerBaseWeakPtr = std::weak_ptr<HandlerBase>;

/**
 * @class AckGroupingTrackerEnabled
 * Ack grouping tracker for consumers of persistent topics that enabled ACK grouping.
 */
class AckGroupingTrackerEnabled : public AckGroupingTracker {
   public:
    AckGroupingTrackerEnabled(std::function<ClientConnectionPtr()> connectionSupplier,
                              std::function<uint64_t()> requestIdSupplier, uint64_t consumerId,
                              bool waitResponse, long ackGroupingTimeMs, long ackGroupingMaxSize,
                              const ExecutorServicePtr& executor)
        : AckGroupingTracker(connectionSupplier, requestIdSupplier, consumerId, waitResponse),
          ackGroupingTimeMs_(ackGroupingTimeMs),
          ackGroupingMaxSize_(ackGroupingMaxSize),
          executor_(executor) {
        pendingIndividualCallbacks_.reserve(ackGroupingMaxSize);
    }

    virtual ~AckGroupingTrackerEnabled() { this->close(); }

    void start() override;
    bool isDuplicate(const MessageId& msgId) override;
    void addAcknowledge(const MessageId& msgId, ResultCallback callback) override;
    void addAcknowledgeList(const MessageIdList& msgIds, ResultCallback callback) override;
    void addAcknowledgeCumulative(const MessageId& msgId, ResultCallback callback) override;
    void close() override;
    void flush() override;
    void flushAndClean() override;

   protected:
    //! Method for scheduling grouping timer.
    void scheduleTimer();

    //! State
    std::atomic_bool isClosed_{false};

    //! Next message ID to be cumulatively cumulatively.
    MessageId nextCumulativeAckMsgId_{MessageId::earliest()};
    bool requireCumulativeAck_{false};
    ResultCallback latestCumulativeCallback_;
    std::mutex mutexCumulativeAckMsgId_;

    //! Individual ACK requests that have not been sent to broker.
    std::set<MessageId> pendingIndividualAcks_;
    std::vector<ResultCallback> pendingIndividualCallbacks_;
    std::recursive_mutex rmutexPendingIndAcks_;

    //! Time window in milliseconds for grouping ACK requests.
    const long ackGroupingTimeMs_;

    //! Max number of ACK requests can be grouped.
    const long ackGroupingMaxSize_;

    //! ACK request sender's scheduled executor.
    const ExecutorServicePtr executor_;

    //! Pointer to a deadline timer.
    DeadlineTimerPtr timer_;
    std::mutex mutexTimer_;
};  // class AckGroupingTrackerEnabled

}  // namespace pulsar
#endif /* LIB_ACKGROUPINGTRACKERENABLED_H_ */

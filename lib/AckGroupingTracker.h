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
#ifndef LIB_ACKGROUPINGTRACKER_H_
#define LIB_ACKGROUPINGTRACKER_H_

#include <pulsar/MessageId.h>
#include <pulsar/Result.h>

#include <cstdint>
#include <functional>
#include <set>

#include "ProtoApiEnums.h"

namespace pulsar {

class ClientConnection;
using ClientConnectionPtr = std::shared_ptr<ClientConnection>;
using ClientConnectionWeakPtr = std::weak_ptr<ClientConnection>;
using ResultCallback = std::function<void(Result)>;

/**
 * @class AckGroupingTracker
 * Default ACK grouping tracker, it actually neither tracks ACK requests nor sends them to brokers.
 * It can be directly used by consumers for non-persistent topics.
 */
class AckGroupingTracker : public std::enable_shared_from_this<AckGroupingTracker> {
   public:
    AckGroupingTracker(std::function<ClientConnectionPtr()> connectionSupplier,
                       std::function<uint64_t()> requestIdSupplier, uint64_t consumerId, bool waitResponse)
        : connectionSupplier_(connectionSupplier),
          requestIdSupplier_(requestIdSupplier),
          consumerId_(consumerId),
          waitResponse_(waitResponse) {}

    virtual ~AckGroupingTracker() = default;

    /**
     * Start tracking the ACK requests.
     */
    virtual void start() {}

    /**
     * Since ACK requests are grouped and delayed, we need to do some best-effort duplicate check to
     * discard messages that are being resent after a disconnection and for which the user has
     * already sent an acknowledgement.
     * @param[in] msgId message ID to be checked.
     * @return true if given message ID is grouped, otherwise false. If using cumulative ACK and the
     *  given message ID has been ACKed in previous cumulative ACK, it also returns true;
     */
    virtual bool isDuplicate(const MessageId& msgId) { return false; }

    /**
     * Adding message ID into ACK group for individual ACK.
     * @param[in] msgId ID of the message to be ACKed.
     * @param[in] callback the callback that is triggered when the message is acknowledged
     */
    virtual void addAcknowledge(const MessageId& msgId, ResultCallback callback) { callback(ResultOk); }

    /**
     * Adding message ID list into ACK group for individual ACK.
     * @param[in] msgIds of the message to be ACKed.
     * @param[in] callback the callback that is triggered when the messages are acknowledged
     */
    virtual void addAcknowledgeList(const MessageIdList& msgIds, ResultCallback callback) {
        callback(ResultOk);
    }

    /**
     * Adding message ID into ACK group for cumulative ACK.
     * @param[in] msgId ID of the message to be ACKed.
     * @param[in] callback the callback that is triggered when the message is acknowledged
     */
    virtual void addAcknowledgeCumulative(const MessageId& msgId, ResultCallback callback) {
        callback(ResultOk);
    }

    /**
     * Flush all the pending grouped ACKs (as flush() does), and stop period ACKs sending.
     */
    virtual void close() {}

    /**
     * Flush all the pending grouped ACKs and send them to the broker.
     */
    virtual void flush() {}

    /**
     * Flush all the pending grouped ACKs (as flush() does), and clean all records about ACKed
     * messages, such as last cumulative ACKed message ID.
     */
    virtual void flushAndClean() {}

   protected:
    void doImmediateAck(const MessageId& msgId, ResultCallback callback, CommandAck_AckType ackType) const;
    void doImmediateAck(const std::set<MessageId>& msgIds, ResultCallback callback) const;

   private:
    const std::function<ClientConnectionPtr()> connectionSupplier_;
    const std::function<uint64_t()> requestIdSupplier_;
    const uint64_t consumerId_;

   protected:
    const bool waitResponse_;

};  // class AckGroupingTracker

using AckGroupingTrackerPtr = std::shared_ptr<AckGroupingTracker>;

}  // namespace pulsar
#endif /* LIB_ACKGROUPINGTRACKER_H_ */

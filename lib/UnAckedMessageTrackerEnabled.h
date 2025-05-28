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
#ifndef LIB_UNACKEDMESSAGETRACKERENABLED_H_
#define LIB_UNACKEDMESSAGETRACKERENABLED_H_
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>

#include "AsioTimer.h"
#include "TestUtil.h"
#include "UnAckedMessageTrackerInterface.h"

namespace pulsar {

class ClientImpl;
class ConsumerImplBase;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using ClientImplWeakPtr = std::weak_ptr<ClientImpl>;

class UnAckedMessageTrackerEnabled : public std::enable_shared_from_this<UnAckedMessageTrackerEnabled>,
                                     public UnAckedMessageTrackerInterface {
   public:
    UnAckedMessageTrackerEnabled(long timeoutMs, const ClientImplPtr&, ConsumerImplBase&);
    UnAckedMessageTrackerEnabled(long timeoutMs, long tickDuration, const ClientImplPtr&, ConsumerImplBase&);
    void start() override;
    void stop() override;
    bool add(const MessageId& msgId) override;
    bool remove(const MessageId& msgId) override;
    void remove(const MessageIdList& msgIds) override;
    void removeMessagesTill(const MessageId& msgId) override;
    void removeTopicMessage(const std::string& topic) override;
    void timeoutHandler();

    void clear() override;

   protected:
    void timeoutHandlerHelper();
    bool isEmpty();
    long size();
    std::map<MessageId, std::set<MessageId>&> messageIdPartitionMap;
    std::deque<std::set<MessageId>> timePartitions;
    std::recursive_mutex lock_;
    ConsumerImplBase& consumerReference_;
    ClientImplWeakPtr client_;
    DeadlineTimerPtr timer_;  // DO NOT place this before client_!
    long timeoutMs_;
    long tickDurationInMs_;

    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testBatchUnAckedMessageTracker);
    FRIEND_TEST(ConsumerTest, testAcknowledgeCumulativeWithPartition);
    FRIEND_TEST(ConsumerTest, testMultiConsumerListenerAndAck);
};
}  // namespace pulsar

#endif /* LIB_UNACKEDMESSAGETRACKERENABLED_H_ */

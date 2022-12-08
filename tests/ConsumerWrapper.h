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

#include <gtest/gtest.h>
#include <pulsar/Client.h>

#include <chrono>
#include <thread>

#include "lib/ProtoApiEnums.h"

using namespace pulsar;

class ConsumerWrapper {
   public:
    void initialize(Client& client, const std::string& topic, const std::string& subscription) {
        // Enable the stats for cumulative ack
        ConsumerConfiguration conf;
        conf.setUnAckedMessagesTimeoutMs(10000);
        ASSERT_EQ(ResultOk, client.subscribe(topic, subscription, conf, consumer_));
    }

    const std::vector<MessageId>& messageIdList() const noexcept { return messageIdList_; }

    Result receive(Message& msg) { return consumer_.receive(msg, 3000); }

    void receiveAtMost(int numMessages) {
        Message msg;
        for (int i = 0; i < numMessages; i++) {
            ASSERT_EQ(ResultOk, consumer_.receive(msg, 3000));
            messageIdList_.emplace_back(msg.getMessageId());
        }
    }

    unsigned long getNumAcked(CommandAck_AckType ackType) const;

    void acknowledgeAndRedeliver(const std::vector<size_t>& indexes, CommandAck_AckType ackType) {
        for (size_t index : indexes) {
            if (ackType == CommandAck_AckType_Individual) {
                consumer_.acknowledge(messageIdList_.at(index));
            } else {
                consumer_.acknowledgeCumulative(messageIdList_.at(index));
            }
        }
        // Wait until the acknowledge command is sent
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        consumer_.redeliverUnacknowledgedMessages();
    }

   private:
    Consumer consumer_;
    std::vector<MessageId> messageIdList_;
};

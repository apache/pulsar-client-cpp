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

namespace pulsar {

enum class AckType
{
    INDIVIDUAL,
    INDIVIDUAL_LIST,
    CUMULATIVE
};

inline MessageIdList subMessageIdList(const MessageIdList& messageIdList,
                                      const std::vector<size_t>& indexes) {
    std::vector<MessageId> subMessageIdList;
    for (size_t index : indexes) {
        subMessageIdList.emplace_back(messageIdList.at(index));
    }
    return subMessageIdList;
}

class ConsumerWrapper {
   public:
    void initialize(Client& client, const std::string& topic, const std::string& subscription,
                    bool enableBatchIndexAck = false) {
        client_ = &client;
        topic_ = topic;
        subscription_ = subscription;
        // Enable the stats for cumulative ack
        conf_.setUnAckedMessagesTimeoutMs(10000);
        conf_.setBatchIndexAckEnabled(enableBatchIndexAck);
        conf_.setAckGroupingTimeMs(0);  // send ACK immediately
        ASSERT_EQ(ResultOk, client_->subscribe(topic_, subscription_, conf_, consumer_));
    }

    const std::vector<MessageId>& messageIdList() const noexcept { return messageIdList_; }

    void receiveAtMost(int numMessages) {
        Message msg;
        for (int i = 0; i < numMessages; i++) {
            ASSERT_EQ(ResultOk, consumer_.receive(msg, 3000));
            messageIdList_.emplace_back(msg.getMessageId());
        }
    }

    unsigned long getNumAcked(CommandAck_AckType ackType) const;

    void acknowledge(const std::vector<size_t>& indexes, AckType ackType) {
        auto msgIds = subMessageIdList(messageIdList_, indexes);
        if (ackType == AckType::INDIVIDUAL_LIST) {
            consumer_.acknowledge(msgIds);
        } else {
            for (auto&& msgId : msgIds) {
                if (ackType == AckType::CUMULATIVE) {
                    consumer_.acknowledgeCumulative(msgId);
                } else {
                    consumer_.acknowledge(msgId);
                }
            }
        }
        // Wait until the acknowledge command is sent
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void acknowledgeAndRedeliver(const std::vector<size_t>& indexes, AckType ackType) {
        acknowledge(indexes, ackType);
        consumer_.redeliverUnacknowledgedMessages();
    }

    // NOTE: Currently Pulsar broker doesn't support redelivery with batch index ACK well, so here we verify
    // the acknowledgment by restarting the consumer.
    void acknowledgeAndRestart(const std::vector<size_t>& indexes, AckType ackType) {
        acknowledge(indexes, ackType);
        restart();
    }

    void acknowledgeMessageIdAndRestart(MessageId msgId, AckType ackType) {
        if (ackType == AckType::CUMULATIVE) {
            consumer_.acknowledgeCumulative(msgId);
        } else {
            consumer_.acknowledge(msgId);
        }
        restart();
    }

    Consumer& getConsumer() noexcept { return consumer_; }

   private:
    Client* client_;
    std::string topic_;
    std::string subscription_;
    ConsumerConfiguration conf_;
    Consumer consumer_;
    std::vector<MessageId> messageIdList_;

    void restart() {
        messageIdList_.clear();
        consumer_.close();
        ASSERT_EQ(ResultOk, client_->subscribe(topic_, subscription_, conf_, consumer_));
    }
};

}  // namespace pulsar

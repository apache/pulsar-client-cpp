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

#include "AckGroupingTracker.h"

#include "ClientConnection.h"
#include "Commands.h"
#include "LogUtils.h"

namespace pulsar {

DECLARE_LOG_OBJECT();

inline void sendAck(ClientConnectionPtr cnx, uint64_t consumerId, const MessageId& msgId,
                    CommandAck_AckType ackType) {
    auto cmd = Commands::newAck(consumerId, msgId.ledgerId(), msgId.entryId(), ackType, -1);
    cnx->sendCommand(cmd);
    LOG_DEBUG("ACK request is sent for message - [" << msgId.ledgerId() << ", " << msgId.entryId() << "]");
}

bool AckGroupingTracker::doImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId,
                                        const MessageId& msgId, CommandAck_AckType ackType) {
    auto cnx = connWeakPtr.lock();
    if (cnx == nullptr) {
        LOG_DEBUG("Connection is not ready, ACK failed for message - [" << msgId.ledgerId() << ", "
                                                                        << msgId.entryId() << "]");
        return false;
    }
    sendAck(cnx, consumerId, msgId, ackType);
    return true;
}

static std::ostream& operator<<(std::ostream& os, const std::set<MessageId>& msgIds) {
    bool first = true;
    for (auto&& msgId : msgIds) {
        if (first) {
            first = false;
        } else {
            os << ", ";
        }
        os << "[" << msgId << "]";
    }
    return os;
}

bool AckGroupingTracker::doImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId,
                                        const std::set<MessageId>& msgIds) {
    auto cnx = connWeakPtr.lock();
    if (cnx == nullptr) {
        LOG_DEBUG("Connection is not ready, ACK failed.");
        return false;
    }

    if (Commands::peerSupportsMultiMessageAcknowledgement(cnx->getServerProtocolVersion())) {
        auto cmd = Commands::newMultiMessageAck(consumerId, msgIds);
        cnx->sendCommand(cmd);
        LOG_DEBUG("ACK request is sent for " << msgIds.size() << " messages: " << msgIds);
    } else {
        // Broker does not support multi-message ACK, use multiple individual ACKs instead.
        for (const auto& msgId : msgIds) {
            sendAck(cnx, consumerId, msgId, CommandAck_AckType_Individual);
        }
    }
    return true;
}

}  // namespace pulsar

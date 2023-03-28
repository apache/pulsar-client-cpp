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

#include <atomic>
#include <limits>

#include "BitSet.h"
#include "ClientConnection.h"
#include "Commands.h"
#include "LogUtils.h"
#include "MessageIdImpl.h"

namespace pulsar {

DECLARE_LOG_OBJECT();

void AckGroupingTracker::doImmediateAck(const MessageId& msgId, ResultCallback callback,
                                        CommandAck_AckType ackType) const {
    const auto cnx = connectionSupplier_();
    if (!cnx) {
        LOG_DEBUG("Connection is not ready, ACK failed for " << msgId);
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }
    const auto& ackSet = Commands::getMessageIdImpl(msgId)->getBitSet();
    if (waitResponse_) {
        const auto requestId = requestIdSupplier_();
        cnx->sendRequestWithId(
               Commands::newAck(consumerId_, msgId.ledgerId(), msgId.entryId(), ackSet, ackType, requestId),
               requestId)
            .addListener([callback](Result result, const ResponseData&) {
                if (callback) {
                    callback(result);
                }
            });
    } else {
        cnx->sendCommand(Commands::newAck(consumerId_, msgId.ledgerId(), msgId.entryId(), ackSet, ackType));
        if (callback) {
            callback(ResultOk);
        }
    }
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

void AckGroupingTracker::doImmediateAck(const std::set<MessageId>& msgIds, ResultCallback callback) const {
    const auto cnx = connectionSupplier_();
    if (!cnx) {
        LOG_DEBUG("Connection is not ready, ACK failed for " << msgIds);
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    if (Commands::peerSupportsMultiMessageAcknowledgement(cnx->getServerProtocolVersion())) {
        if (waitResponse_) {
            const auto requestId = requestIdSupplier_();
            cnx->sendRequestWithId(Commands::newMultiMessageAck(consumerId_, msgIds, requestId), requestId)
                .addListener([callback](Result result, const ResponseData&) {
                    if (callback) {
                        callback(result);
                    }
                });
        } else {
            cnx->sendCommand(Commands::newMultiMessageAck(consumerId_, msgIds));
            if (callback) {
                callback(ResultOk);
            }
        }
    } else {
        auto count = std::make_shared<std::atomic<size_t>>(msgIds.size());
        auto wrappedCallback = [callback, count](Result result) {
            if (--*count == 0 && callback) {
                callback(result);
            }
        };
        for (auto&& msgId : msgIds) {
            doImmediateAck(msgId, wrappedCallback, CommandAck_AckType_Individual);
        }
    }
}

}  // namespace pulsar

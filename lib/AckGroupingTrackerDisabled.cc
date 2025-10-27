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

#include "AckGroupingTrackerDisabled.h"

#include "ConsumerImpl.h"

namespace pulsar {

void AckGroupingTrackerDisabled::addAcknowledge(const MessageId& msgId, const ResultCallback& callback) {
    auto consumer = consumer_.lock();
    if (consumer && !consumer->isClosingOrClosed()) {
        consumer->doImmediateAck(msgId, callback, CommandAck_AckType_Individual);
    } else if (callback) {
        callback(ResultAlreadyClosed);
    }
}

void AckGroupingTrackerDisabled::addAcknowledgeList(const MessageIdList& msgIds,
                                                    const ResultCallback& callback) {
    auto consumer = consumer_.lock();
    if (consumer && !consumer->isClosingOrClosed()) {
        std::set<MessageId> uniqueMsgIds(msgIds.begin(), msgIds.end());
        for (auto&& msgId : msgIds) {
            uniqueMsgIds.insert(msgId);
        }
        consumer->doImmediateAck(uniqueMsgIds, callback);
    } else if (callback) {
        callback(ResultAlreadyClosed);
    }
}

void AckGroupingTrackerDisabled::addAcknowledgeCumulative(const MessageId& msgId,
                                                          const ResultCallback& callback) {
    auto consumer = consumer_.lock();
    if (consumer && !consumer->isClosingOrClosed()) {
        consumer->doImmediateAck(msgId, callback, CommandAck_AckType_Cumulative);
    } else if (callback) {
        callback(ResultAlreadyClosed);
    }
}

}  // namespace pulsar

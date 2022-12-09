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

#include "MessageIdImpl.h"

namespace pulsar {
class ChunkMessageIdImpl;
typedef std::shared_ptr<ChunkMessageIdImpl> ChunkMessageIdImplPtr;
class ChunkMessageIdImpl : public MessageIdImpl {
   public:
    ChunkMessageIdImpl() {}
//    ChunkMessageIdImpl(const MessageIdImpl& firstChunkMsgId, const MessageIdImpl& lastChunkMsgId)
//        : MessageIdImpl(lastChunkMsgId), firstChunkMsgId_(firstChunkMsgId) {}

    void setFirstChunkMessageId(const MessageId& msgId) {
        *firstChunkMsgId_.impl_ = *msgId.impl_;
    }

    void setLastChunkMessageId(const MessageId& msgId) {
        this->ledgerId_ = msgId.ledgerId();
        this->entryId_ = msgId.entryId();
        this->partition_ = msgId.partition();
    }

    const MessageId& getFirstChunkMessageId() const { return firstChunkMsgId_; }

    static MessageId buildMessageId(ChunkMessageIdImplPtr& msgIdImpl) {
        return MessageId{msgIdImpl};
    }

   private:
    MessageId firstChunkMsgId_;
};
}  // namespace pulsar

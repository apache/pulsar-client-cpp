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

#include <pulsar/MessageId.h>

#include "MessageIdImpl.h"

namespace pulsar {
class ChunkMessageIdImpl;
typedef std::shared_ptr<ChunkMessageIdImpl> ChunkMessageIdImplPtr;
class ChunkMessageIdImpl : public MessageIdImpl, public std::enable_shared_from_this<ChunkMessageIdImpl> {
   public:
    ChunkMessageIdImpl() : firstChunkMsgId_(std::make_shared<MessageIdImpl>()) {}

    void setFirstChunkMessageId(const MessageId& msgId) { *firstChunkMsgId_ = *msgId.impl_; }

    void setLastChunkMessageId(const MessageId& msgId) {
        this->ledgerId_ = msgId.ledgerId();
        this->entryId_ = msgId.entryId();
        this->partition_ = msgId.partition();
    }

    std::shared_ptr<const MessageIdImpl> getFirstChunkMessageId() const { return firstChunkMsgId_; }

    MessageId build() { return MessageId{std::dynamic_pointer_cast<MessageIdImpl>(shared_from_this())}; }

   private:
    std::shared_ptr<MessageIdImpl> firstChunkMsgId_;
};
}  // namespace pulsar

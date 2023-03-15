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

#include <pulsar/MessageId.h>
#include <pulsar/MessageIdBuilder.h>

#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>

#include "ChunkMessageIdImpl.h"
#include "MessageIdImpl.h"
#include "PulsarApi.pb.h"

namespace pulsar {

MessageId::MessageId() {
    static const MessageIdImplPtr emptyMessageId = std::make_shared<MessageIdImpl>();
    impl_ = emptyMessageId;
}

MessageId& MessageId::operator=(const MessageId& m) {
    impl_ = m.impl_;
    return *this;
}

MessageId::MessageId(int32_t partition, int64_t ledgerId, int64_t entryId, int32_t batchIndex)
    : impl_(std::make_shared<MessageIdImpl>(partition, ledgerId, entryId, batchIndex)) {}

MessageId::MessageId(const MessageIdImplPtr& impl) : impl_(impl) {}

const MessageId& MessageId::earliest() {
    static const auto _earliest = MessageIdBuilder().build();
    return _earliest;
}

const MessageId& MessageId::latest() {
    static const int64_t long_max = std::numeric_limits<int64_t>::max();
    static const auto _latest = MessageIdBuilder().ledgerId(long_max).entryId(long_max).build();
    return _latest;
}

void MessageId::serialize(std::string& result) const {
    proto::MessageIdData idData;
    idData.set_ledgerid(impl_->ledgerId_);
    idData.set_entryid(impl_->entryId_);
    if (impl_->partition_ != -1) {
        idData.set_partition(impl_->partition_);
    }

    if (impl_->batchIndex_ != -1) {
        idData.set_batch_index(impl_->batchIndex_);
    }

    if (impl_->batchSize_ != 0) {
        idData.set_batch_size(impl_->batchSize_);
    }

    auto chunkMsgId = std::dynamic_pointer_cast<ChunkMessageIdImpl>(impl_);
    if (chunkMsgId) {
        proto::MessageIdData& firstChunkIdData = *idData.mutable_first_chunk_message_id();
        auto firstChunkId = chunkMsgId->getFirstChunkMessageId();
        firstChunkIdData.set_ledgerid(firstChunkId->ledgerId_);
        firstChunkIdData.set_entryid(firstChunkId->entryId_);
        if (chunkMsgId->partition_ != -1) {
            firstChunkIdData.set_partition(firstChunkId->partition_);
        }
    }

    idData.SerializeToString(&result);
}

/**
 * Deserialize a message id from a binary string
 */
MessageId MessageId::deserialize(const std::string& serializedMessageId) {
    proto::MessageIdData idData;
    if (!idData.ParseFromString(serializedMessageId)) {
        throw std::invalid_argument("Failed to parse serialized message id");
    }

    MessageId msgId = MessageIdBuilder::from(idData).build();

    if (idData.has_first_chunk_message_id()) {
        ChunkMessageIdImplPtr chunkMsgId = std::make_shared<ChunkMessageIdImpl>();
        chunkMsgId->setFirstChunkMessageId(MessageIdBuilder::from(idData.first_chunk_message_id()).build());
        chunkMsgId->setLastChunkMessageId(msgId);
        return chunkMsgId->build();
    }

    return msgId;
}

int64_t MessageId::ledgerId() const { return impl_->ledgerId_; }

int64_t MessageId::entryId() const { return impl_->entryId_; }

int32_t MessageId::batchIndex() const { return impl_->batchIndex_; }

int32_t MessageId::partition() const { return impl_->partition_; }

int32_t MessageId::batchSize() const { return impl_->batchSize_; }

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const pulsar::MessageId& messageId) {
    auto chunkMsgId = std::dynamic_pointer_cast<ChunkMessageIdImpl>(messageId.impl_);
    if (chunkMsgId) {
        auto firstId = chunkMsgId->getFirstChunkMessageId();
        s << '(' << firstId->ledgerId_ << ',' << firstId->entryId_ << ',' << firstId->partition_ << ','
          << firstId->batchIndex_ << ");";
    }
    s << '(' << messageId.impl_->ledgerId_ << ',' << messageId.impl_->entryId_ << ','
      << messageId.impl_->partition_ << ',' << messageId.impl_->batchIndex_ << ')';
    return s;
}

PULSAR_PUBLIC bool MessageId::operator<(const MessageId& other) const {
    if (impl_->ledgerId_ < other.impl_->ledgerId_) {
        return true;
    } else if (impl_->ledgerId_ > other.impl_->ledgerId_) {
        return false;
    }

    if (impl_->entryId_ < other.impl_->entryId_) {
        return true;
    } else if (impl_->entryId_ > other.impl_->entryId_) {
        return false;
    }

    if (impl_->batchIndex_ < other.impl_->batchIndex_) {
        return true;
    } else {
        return false;
    }
}

PULSAR_PUBLIC bool MessageId::operator<=(const MessageId& other) const {
    return *this < other || *this == other;
}

PULSAR_PUBLIC bool MessageId::operator>(const MessageId& other) const { return !(*this <= other); }

PULSAR_PUBLIC bool MessageId::operator>=(const MessageId& other) const { return !(*this < other); }

PULSAR_PUBLIC bool MessageId::operator==(const MessageId& other) const {
    return impl_->ledgerId_ == other.impl_->ledgerId_ && impl_->entryId_ == other.impl_->entryId_ &&
           impl_->batchIndex_ == other.impl_->batchIndex_ && impl_->partition_ == other.impl_->partition_;
}

PULSAR_PUBLIC bool MessageId::operator!=(const MessageId& other) const { return !(*this == other); }

PULSAR_PUBLIC const std::string& MessageId::getTopicName() const { return impl_->getTopicName(); }

PULSAR_PUBLIC void MessageId::setTopicName(const std::string& topicName) {
    return setTopicName(std::make_shared<std::string>(topicName));
}

void MessageId::setTopicName(const std::shared_ptr<std::string>& topic) { return impl_->setTopicName(topic); }

}  // namespace pulsar

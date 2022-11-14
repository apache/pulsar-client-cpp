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
#include <assert.h>
#include <pulsar/MessageIdBuilder.h>

#include "MessageIdImpl.h"
#include "PulsarApi.pb.h"

namespace pulsar {

MessageIdBuilder::MessageIdBuilder() : impl_(std::make_shared<MessageIdImpl>()) {}

MessageIdBuilder MessageIdBuilder::from(const MessageId& messageId) {
    MessageIdBuilder builder;
    *builder.impl_ = *(messageId.impl_);
    return builder;
}

MessageIdBuilder MessageIdBuilder::from(const proto::MessageIdData& messageIdData) {
    return MessageIdBuilder()
        .ledgerId(messageIdData.ledgerid())
        .entryId(messageIdData.entryid())
        .partition(messageIdData.partition())
        .batchIndex(messageIdData.batch_index())
        .batchSize(messageIdData.batch_size());
}

MessageId MessageIdBuilder::build() const {
    assert(impl_->batchIndex_ < 0 || (impl_->batchSize_ > impl_->batchIndex_));
    return MessageId{impl_};
}

MessageIdBuilder& MessageIdBuilder::ledgerId(int64_t ledgerId) {
    impl_->ledgerId_ = ledgerId;
    return *this;
}

MessageIdBuilder& MessageIdBuilder::entryId(int64_t entryId) {
    impl_->entryId_ = entryId;
    return *this;
}

MessageIdBuilder& MessageIdBuilder::partition(int32_t partition) {
    impl_->partition_ = partition;
    return *this;
}

MessageIdBuilder& MessageIdBuilder::batchIndex(int32_t batchIndex) {
    impl_->batchIndex_ = batchIndex;
    return *this;
}

MessageIdBuilder& MessageIdBuilder::batchSize(int32_t batchSize) {
    impl_->batchSize_ = batchSize;
    return *this;
}

}  // namespace pulsar

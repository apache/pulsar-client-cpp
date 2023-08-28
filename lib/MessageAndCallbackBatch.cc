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
#include "MessageAndCallbackBatch.h"

#include <pulsar/MessageIdBuilder.h>

#include "ClientConnection.h"
#include "Commands.h"
#include "CompressionCodec.h"
#include "MessageCrypto.h"
#include "OpSendMsg.h"
#include "PulsarApi.pb.h"

namespace pulsar {

MessageAndCallbackBatch::MessageAndCallbackBatch() {}

MessageAndCallbackBatch::~MessageAndCallbackBatch() {}

void MessageAndCallbackBatch::add(const Message& msg, const SendCallback& callback) {
    if (callbacks_.empty()) {
        metadata_.reset(new proto::MessageMetadata);
        Commands::initBatchMessageMetadata(msg, *metadata_);
        sequenceId_ = metadata_->sequence_id();
    }
    messages_.emplace_back(msg);
    callbacks_.emplace_back(callback);
    messagesSize_ += msg.getLength();
}

std::unique_ptr<OpSendMsg> MessageAndCallbackBatch::createOpSendMsg(
    uint64_t producerId, const ProducerConfiguration& producerConfig, MessageCrypto* crypto) {
    auto callback = createSendCallback();
    if (empty()) {
        return OpSendMsg::create(ResultOperationNotSupported, std::move(callback));
    }

    // The magic number 64 is just an estimated size increment after setting some fields of the
    // SingleMessageMetadata. It does not have to be accurate because it's only used to reduce the
    // reallocation of the payload buffer.
    static const size_t kEstimatedHeaderSize =
        sizeof(uint32_t) + proto::MessageMetadata{}.ByteSizeLong() + 64;
    const auto maxMessageSize = ClientConnection::getMaxMessageSize();
    // Estimate the buffer size just to avoid resizing the buffer
    size_t maxBufferSize = kEstimatedHeaderSize * messages_.size();
    for (const auto& msg : messages_) {
        maxBufferSize += msg.getLength();
    }
    auto payload = SharedBuffer::allocate(maxBufferSize);
    for (const auto& msg : messages_) {
        sequenceId_ = Commands::serializeSingleMessageInBatchWithPayload(msg, payload, maxMessageSize);
    }
    metadata_->set_sequence_id(sequenceId_);
    metadata_->set_num_messages_in_batch(messages_.size());
    auto compressionType = producerConfig.getCompressionType();
    if (compressionType != CompressionNone) {
        metadata_->set_compression(static_cast<proto::CompressionType>(compressionType));
        metadata_->set_uncompressed_size(payload.readableBytes());
    }
    payload = CompressionCodecProvider::getCodec(compressionType).encode(payload);

    if (producerConfig.isEncryptionEnabled() && crypto) {
        SharedBuffer encryptedPayload;
        if (!crypto->encrypt(producerConfig.getEncryptionKeys(), producerConfig.getCryptoKeyReader(),
                             *metadata_, payload, encryptedPayload)) {
            return OpSendMsg::create(ResultCryptoError, std::move(callback));
        }
        payload = encryptedPayload;
    }

    if (payload.readableBytes() > ClientConnection::getMaxMessageSize()) {
        return OpSendMsg::create(ResultMessageTooBig, std::move(callback));
    }

    auto op = OpSendMsg::create(*metadata_, callbacks_.size(), messagesSize_, producerConfig.getSendTimeout(),
                                std::move(callback), nullptr, producerId, payload);
    clear();
    return op;
}

void MessageAndCallbackBatch::clear() {
    messages_.clear();
    callbacks_.clear();
    messagesSize_ = 0;
}

static void completeSendCallbacks(const std::vector<SendCallback>& callbacks, Result result,
                                  const MessageId& id) {
    int32_t numOfMessages = static_cast<int32_t>(callbacks.size());
    for (int32_t i = 0; i < numOfMessages; i++) {
        callbacks[i](result, MessageIdBuilder::from(id).batchIndex(i).batchSize(numOfMessages).build());
    }
}

SendCallback MessageAndCallbackBatch::createSendCallback() const {
    const auto& callbacks = callbacks_;
    return [callbacks](Result result, const MessageId& id) { completeSendCallbacks(callbacks, result, id); };
}

}  // namespace pulsar

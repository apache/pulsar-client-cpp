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
#include "BatchMessageContainerBase.h"

#include "ClientConnection.h"
#include "CompressionCodec.h"
#include "MessageAndCallbackBatch.h"
#include "MessageCrypto.h"
#include "MessageImpl.h"
#include "OpSendMsg.h"
#include "ProducerImpl.h"
#include "PulsarApi.pb.h"
#include "SharedBuffer.h"

namespace pulsar {

BatchMessageContainerBase::BatchMessageContainerBase(const ProducerImpl& producer)
    : topicName_(producer.topic_),
      producerConfig_(producer.conf_),
      producerName_(producer.producerName_),
      producerId_(producer.producerId_),
      msgCryptoWeakPtr_(producer.msgCrypto_) {}

BatchMessageContainerBase::~BatchMessageContainerBase() {}

std::unique_ptr<OpSendMsg> BatchMessageContainerBase::createOpSendMsgHelper(
    const FlushCallback& flushCallback, const MessageAndCallbackBatch& batch) const {
    auto sendCallback = batch.createSendCallback(flushCallback);
    if (batch.empty()) {
        return OpSendMsg::create(ResultOperationNotSupported, std::move(sendCallback));
    }

    MessageImplPtr impl = batch.msgImpl();
    impl->metadata.set_num_messages_in_batch(batch.size());
    auto compressionType = producerConfig_.getCompressionType();
    if (compressionType != CompressionNone) {
        impl->metadata.set_compression(static_cast<proto::CompressionType>(compressionType));
        impl->metadata.set_uncompressed_size(impl->payload.readableBytes());
    }
    impl->payload = CompressionCodecProvider::getCodec(compressionType).encode(impl->payload);

    auto msgCrypto = msgCryptoWeakPtr_.lock();
    if (msgCrypto && producerConfig_.isEncryptionEnabled()) {
        SharedBuffer encryptedPayload;
        if (!msgCrypto->encrypt(producerConfig_.getEncryptionKeys(), producerConfig_.getCryptoKeyReader(),
                                impl->metadata, impl->payload, encryptedPayload)) {
            return OpSendMsg::create(ResultCryptoError, std::move(sendCallback));
        }
        impl->payload = encryptedPayload;
    }

    if (impl->payload.readableBytes() > ClientConnection::getMaxMessageSize()) {
        return OpSendMsg::create(ResultMessageTooBig, std::move(sendCallback));
    }

    return OpSendMsg::create(impl->metadata, batch.messagesCount(), batch.messagesSize(),
                             producerConfig_.getSendTimeout(), batch.createSendCallback(flushCallback),
                             nullptr, producerId_, impl->payload);
}

}  // namespace pulsar

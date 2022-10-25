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

Result BatchMessageContainerBase::createOpSendMsgHelper(OpSendMsg& opSendMsg,
                                                        const FlushCallback& flushCallback,
                                                        const MessageAndCallbackBatch& batch) const {
    opSendMsg.sendCallback_ = batch.createSendCallback();
    opSendMsg.messagesCount_ = batch.messagesCount();
    opSendMsg.messagesSize_ = batch.messagesSize();

    if (flushCallback) {
        auto sendCallback = opSendMsg.sendCallback_;
        opSendMsg.sendCallback_ = [sendCallback, flushCallback](Result result, const MessageId& id) {
            sendCallback(result, id);
            flushCallback(result);
        };
    }

    if (batch.empty()) {
        return ResultOperationNotSupported;
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
            return ResultCryptoError;
        }
        impl->payload = encryptedPayload;
    }

    if (impl->payload.readableBytes() > ClientConnection::getMaxMessageSize()) {
        return ResultMessageTooBig;
    }

    opSendMsg.metadata_ = impl->metadata;
    opSendMsg.payload_ = impl->payload;
    opSendMsg.sequenceId_ = impl->metadata.sequence_id();
    opSendMsg.producerId_ = producerId_;
    opSendMsg.timeout_ = TimeUtils::now() + milliseconds(producerConfig_.getSendTimeout());

    return ResultOk;
}

void BatchMessageContainerBase::processAndClear(
    std::function<void(Result, const OpSendMsg&)> opSendMsgCallback, FlushCallback flushCallback) {
    if (isEmpty()) {
        if (flushCallback) {
            flushCallback(ResultOk);
        }
    } else {
        const auto numBatches = getNumBatches();
        if (numBatches == 1) {
            OpSendMsg opSendMsg;
            Result result = createOpSendMsg(opSendMsg, flushCallback);
            opSendMsgCallback(result, opSendMsg);
        } else if (numBatches > 1) {
            std::vector<OpSendMsg> opSendMsgs;
            std::vector<Result> results = createOpSendMsgs(opSendMsgs, flushCallback);
            for (size_t i = 0; i < results.size(); i++) {
                opSendMsgCallback(results[i], opSendMsgs[i]);
            }
        }  // else numBatches is 0, do nothing
    }
    clear();
}

}  // namespace pulsar

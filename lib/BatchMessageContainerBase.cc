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

#include "MessageAndCallbackBatch.h"
#include "MessageCrypto.h"
#include "OpSendMsg.h"
#include "ProducerImpl.h"
#include "SharedBuffer.h"

namespace pulsar {

BatchMessageContainerBase::BatchMessageContainerBase(const ProducerImpl& producer)
    : topicName_(producer.topic()),
      producerConfig_(producer.conf_),
      producerName_(producer.producerName_),
      producerId_(producer.producerId_),
      msgCryptoWeakPtr_(producer.msgCrypto_) {}

BatchMessageContainerBase::~BatchMessageContainerBase() {}

std::unique_ptr<OpSendMsg> BatchMessageContainerBase::createOpSendMsgHelper(
    MessageAndCallbackBatch& batch) const {
    auto crypto = msgCryptoWeakPtr_.lock();
    return batch.createOpSendMsg(producerId_, producerConfig_, crypto.get());
}

}  // namespace pulsar

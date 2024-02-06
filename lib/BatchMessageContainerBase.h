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
#ifndef LIB_BATCHMESSAGECONTAINERBASE_H_
#define LIB_BATCHMESSAGECONTAINERBASE_H_

#include <pulsar/Message.h>
#include <pulsar/Producer.h>
#include <pulsar/ProducerConfiguration.h>
#include <pulsar/Result.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <stdexcept>
#include <vector>

namespace pulsar {

class MessageCrypto;
class ProducerImpl;
class SharedBuffer;
struct OpSendMsg;
class MessageAndCallbackBatch;

namespace proto {
class MessageMetadata;
}  // namespace proto

class BatchMessageContainerBase : public boost::noncopyable {
   public:
    BatchMessageContainerBase(const ProducerImpl& producer);

    virtual ~BatchMessageContainerBase();

    virtual bool hasMultiOpSendMsgs() const = 0;

    /**
     * Check the message will be the 1st message to be added to the batch
     *
     * This method is used to check if the reversed spot should be released. Because we won't released the
     * reserved spot for 1st message. The released spot is to contain the whole batched message.
     *
     * @param msg the message to be added to the batch
     * @return true if `msg` is the 1st message to be added to the batch
     */
    virtual bool isFirstMessageToAdd(const Message& msg) const = 0;

    /**
     * Add a message to the batch message container
     *
     * @param msg message will add to the batch message container
     * @param callback message send callback
     * @return true if the batch is full, otherwise false
     */
    virtual bool add(const Message& msg, const SendCallback& callback) = 0;

    virtual std::unique_ptr<OpSendMsg> createOpSendMsg(const FlushCallback& flushCallback = nullptr) {
        throw std::runtime_error("createOpSendMsg is not supported");
    }

    virtual std::vector<std::unique_ptr<OpSendMsg>> createOpSendMsgs(
        const FlushCallback& flushCallback = nullptr) {
        throw std::runtime_error("createOpSendMsgs is not supported");
    }

    /**
     * Serialize into a std::ostream for logging
     *
     * @param os the std::ostream to serialize current batch container
     */
    virtual void serialize(std::ostream& os) const = 0;

    bool hasEnoughSpace(const Message& msg) const noexcept;
    bool isEmpty() const noexcept;

   protected:
    // references to ProducerImpl's fields
    const std::string topicName_;
    const ProducerConfiguration& producerConfig_;
    const std::string& producerName_;
    const uint64_t& producerId_;
    const std::weak_ptr<MessageCrypto> msgCryptoWeakPtr_;

    unsigned int getMaxNumMessages() const noexcept { return producerConfig_.getBatchingMaxMessages(); }
    unsigned long getMaxSizeInBytes() const noexcept {
        return producerConfig_.getBatchingMaxAllowedSizeInBytes();
    }

    unsigned int numMessages_ = 0;
    unsigned long sizeInBytes_ = 0;

    bool isFull() const noexcept;

    void updateStats(const Message& msg);
    void resetStats();

    std::unique_ptr<OpSendMsg> createOpSendMsgHelper(MessageAndCallbackBatch& flushCallback) const;

    virtual void clear() = 0;
};

inline bool BatchMessageContainerBase::hasEnoughSpace(const Message& msg) const noexcept {
    return (numMessages_ < getMaxNumMessages()) && (sizeInBytes_ + msg.getLength() <= getMaxSizeInBytes());
}

inline bool BatchMessageContainerBase::isFull() const noexcept {
    return (numMessages_ >= getMaxNumMessages()) || (sizeInBytes_ >= getMaxSizeInBytes());
}

inline bool BatchMessageContainerBase::isEmpty() const noexcept { return numMessages_ == 0; }

inline void BatchMessageContainerBase::updateStats(const Message& msg) {
    numMessages_++;
    sizeInBytes_ += msg.getLength();
}

inline void BatchMessageContainerBase::resetStats() {
    numMessages_ = 0;
    sizeInBytes_ = 0;
}

inline std::ostream& operator<<(std::ostream& os, const BatchMessageContainerBase& container) {
    container.serialize(os);
    return os;
}

}  // namespace pulsar

#endif  // LIB_BATCHMESSAGECONTAINERBASE_H_

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
#ifndef LIB_MESSAGEANDCALLBACK_BATCH_H_
#define LIB_MESSAGEANDCALLBACK_BATCH_H_

#include <pulsar/Message.h>
#include <pulsar/ProducerConfiguration.h>

#include <atomic>
#include <boost/noncopyable.hpp>
#include <memory>
#include <vector>

namespace pulsar {

struct OpSendMsg;
class MessageCrypto;
using FlushCallback = std::function<void(Result)>;

namespace proto {
class MessageMetadata;
}

class MessageAndCallbackBatch final : public boost::noncopyable {
   public:
    MessageAndCallbackBatch();
    ~MessageAndCallbackBatch();

    // Wrapper methods of STL container
    bool empty() const noexcept { return callbacks_.empty(); }
    size_t size() const noexcept { return callbacks_.size(); }

    /**
     * Add a message and the associated send callback to the batch
     *
     * @param message
     * @callback the associated send callback
     */
    void add(const Message& msg, const SendCallback& callback);

    std::unique_ptr<OpSendMsg> createOpSendMsg(uint64_t producerId,
                                               const ProducerConfiguration& producerConfig,
                                               MessageCrypto* crypto);

    uint64_t sequenceId() const noexcept { return sequenceId_; }

    void clear();

   private:
    std::unique_ptr<proto::MessageMetadata> metadata_;
    std::vector<Message> messages_;
    std::vector<SendCallback> callbacks_;
    std::atomic<uint64_t> sequenceId_{static_cast<uint64_t>(-1L)};
    uint64_t messagesSize_{0ull};

    SendCallback createSendCallback() const;
};

}  // namespace pulsar

#endif

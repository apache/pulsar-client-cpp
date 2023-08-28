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
#ifndef LIB_OPSENDMSG_H_
#define LIB_OPSENDMSG_H_

#include <pulsar/Message.h>
#include <pulsar/Producer.h>
#include <pulsar/Result.h>

#include <boost/date_time/posix_time/ptime.hpp>

#include "ChunkMessageIdImpl.h"
#include "PulsarApi.pb.h"
#include "SharedBuffer.h"
#include "TimeUtils.h"

namespace pulsar {

struct SendArguments {
    const uint64_t producerId;
    const uint64_t sequenceId;
    const proto::MessageMetadata metadata;
    SharedBuffer payload;

    SendArguments(uint64_t producerId, uint64_t sequenceId, const proto::MessageMetadata& metadata,
                  const SharedBuffer& payload)
        : producerId(producerId), sequenceId(sequenceId), metadata(metadata), payload(payload) {}
    SendArguments(const SendArguments&) = delete;
    SendArguments& operator=(const SendArguments&) = delete;
};

struct OpSendMsg {
    const Result result;
    const int32_t chunkId;
    const int32_t numChunks;
    const uint32_t messagesCount;
    const uint64_t messagesSize;
    const boost::posix_time::ptime timeout;
    const SendCallback sendCallback;
    std::vector<std::function<void(Result)>> trackerCallbacks;
    ChunkMessageIdImplPtr chunkedMessageId;
    // Use shared_ptr here because producer might resend the message with the same arguments
    const std::shared_ptr<SendArguments> sendArgs;

    template <typename... Args>
    static std::unique_ptr<OpSendMsg> create(Args&&... args) {
        return std::unique_ptr<OpSendMsg>(new OpSendMsg(std::forward<Args>(args)...));
    }

    void complete(Result result, const MessageId& messageId) const {
        if (sendCallback) {
            sendCallback(result, messageId);
        }
        for (const auto& trackerCallback : trackerCallbacks) {
            trackerCallback(result);
        }
    }

    void addTrackerCallback(std::function<void(Result)> trackerCallback) {
        if (trackerCallback) {
            trackerCallbacks.emplace_back(trackerCallback);
        }
    }

   private:
    OpSendMsg(Result result, SendCallback&& callback)
        : result(result),
          chunkId(-1),
          numChunks(-1),
          messagesCount(0),
          messagesSize(0),
          sendCallback(std::move(callback)),
          sendArgs(nullptr) {}

    OpSendMsg(const proto::MessageMetadata& metadata, uint32_t messagesCount, uint64_t messagesSize,
              int sendTimeoutMs, SendCallback&& callback, ChunkMessageIdImplPtr chunkedMessageId,
              uint64_t producerId, SharedBuffer payload)
        : result(ResultOk),
          chunkId(metadata.chunk_id()),
          numChunks(metadata.num_chunks_from_msg()),
          messagesCount(messagesCount),
          messagesSize(messagesSize),
          timeout(TimeUtils::now() + boost::posix_time::milliseconds(sendTimeoutMs)),
          sendCallback(std::move(callback)),
          chunkedMessageId(chunkedMessageId),
          sendArgs(new SendArguments(producerId, metadata.sequence_id(), metadata, payload)) {}
};

}  // namespace pulsar

#endif

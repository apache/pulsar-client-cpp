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

#include <memory>

namespace pulsar {

namespace proto {
class MessageIdData;
}

/**
 * The builder to build a MessageId.
 *
 * Example of building a single MessageId:
 *
 * ```c++
 * MessageId msgId = MessageIdBuilder()
 *                       .ledgerId(0L)
 *                       .entryId(0L)
 *                       .build();
 * ```
 *
 * Example of building a batched MessageId:
 *
 * ```c++
 * MessageId msgId = MessageIdBuilder()
 *                       .ledgerId(0L)
 *                       .entryId(0L)
 *                       .batchIndex(0)
 *                       .batchSize(2)
 *                       .build();
 * ```
 */
class PULSAR_PUBLIC MessageIdBuilder {
   public:
    explicit MessageIdBuilder();

    /**
     * Create an instance that copies the data from messageId.
     */
    static MessageIdBuilder from(const MessageId& messageId);

    /**
     * Create an instance from the proto::MessageIdData instance.
     *
     * @note It's an internal API that converts the MessageIdData defined by PulsarApi.proto
     * @see https://github.com/apache/pulsar-client-cpp/blob/main/proto/PulsarApi.proto
     */
    static MessageIdBuilder from(const proto::MessageIdData& messageIdData);

    /**
     * Build a MessageId.
     */
    MessageId build() const;

    /**
     * Set the ledger ID field.
     *
     * Default: -1L
     */
    MessageIdBuilder& ledgerId(int64_t ledgerId);

    /**
     * Set the entry ID field.
     *
     * Default: -1L
     */
    MessageIdBuilder& entryId(int64_t entryId);

    /**
     * Set the partition index.
     *
     * Default: -1
     */
    MessageIdBuilder& partition(int32_t partition);

    /**
     * Set the batch index.
     *
     * Default: -1
     */
    MessageIdBuilder& batchIndex(int32_t batchIndex);

    /**
     * Set the batch size.
     *
     * Default: 0
     */
    MessageIdBuilder& batchSize(int32_t batchSize);

   private:
    std::shared_ptr<MessageIdImpl> impl_;
};

}  // namespace pulsar

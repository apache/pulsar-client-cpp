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
#ifndef MESSAGE_ID_H
#define MESSAGE_ID_H

#include <pulsar/defines.h>
#include <stdint.h>

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

namespace pulsar {

class MessageIdImpl;

class PULSAR_PUBLIC MessageId {
   public:
    MessageId& operator=(const MessageId&);
    MessageId();

    /**
     * @deprecated
     *
     * Construct the MessageId
     *
     * NOTE: This API still exists for backward compatibility, use MessageIdBuilder instead.
     *
     * @param partition the partition number of a topic
     * @param ledgerId the ledger id
     * @param entryId the entry id
     * @param batchIndex the batch index of a single message in a batch
     */
    explicit MessageId(int32_t partition, int64_t ledgerId, int64_t entryId, int32_t batchIndex);

    /**
     * MessageId representing the "earliest" or "oldest available" message stored in the topic
     */
    static const MessageId& earliest();

    /**
     * MessageId representing the "latest" or "last published" message in the topic
     */
    static const MessageId& latest();

    /**
     * Serialize the message id into a binary string for storing
     */
    void serialize(std::string& result) const;

    /**
     * Get the topic Name from which this message originated from
     *
     * @return the topic name or an empty string if there is no topic name
     */
    const std::string& getTopicName() const;

    /**
     * Set the topicName
     * @deprecated This method will be eventually removed
     */
    void setTopicName(const std::string& topicName);

    /**
     * Deserialize a message id from a binary string
     */
    static MessageId deserialize(const std::string& serializedMessageId);

    // These functions compare the message order as stored in bookkeeper
    bool operator<(const MessageId& other) const;
    bool operator<=(const MessageId& other) const;
    bool operator>(const MessageId& other) const;
    bool operator>=(const MessageId& other) const;
    bool operator==(const MessageId& other) const;
    bool operator!=(const MessageId& other) const;

    int64_t ledgerId() const;
    int64_t entryId() const;
    int32_t batchIndex() const;
    int32_t partition() const;
    int32_t batchSize() const;

   private:
    friend class ConsumerImpl;
    friend class ReaderImpl;
    friend class Message;
    friend class MessageImpl;
    friend class Commands;
    friend class PartitionedProducerImpl;
    friend class MultiTopicsConsumerImpl;
    friend class UnAckedMessageTrackerEnabled;
    friend class BatchAcknowledgementTracker;
    friend class PulsarWrapper;
    friend class PulsarFriend;
    friend class NegativeAcksTracker;
    friend class MessageIdBuilder;
    friend class ChunkMessageIdImpl;

    void setTopicName(const std::shared_ptr<std::string>& topic);

    friend PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const MessageId& messageId);

    typedef std::shared_ptr<MessageIdImpl> MessageIdImplPtr;
    MessageIdImplPtr impl_;

    explicit MessageId(const MessageIdImplPtr& impl);
};

typedef std::vector<MessageId> MessageIdList;
}  // namespace pulsar

#endif  // MESSAGE_ID_H

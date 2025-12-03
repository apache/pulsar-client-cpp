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
#ifndef LIB_MESSAGEIMPL_H_
#define LIB_MESSAGEIMPL_H_

#include <pulsar/EncryptionContext.h>
#include <pulsar/Message.h>
#include <pulsar/MessageId.h>

#include <memory>
#include <optional>

#include "KeyValueImpl.h"
#include "PulsarApi.pb.h"
#include "SharedBuffer.h"

using std::optional;

namespace pulsar {

class PulsarWrapper;
class ClientConnection;
class BatchMessageContainer;

class MessageImpl {
   public:
    explicit MessageImpl() : encryptionContext_(std::nullopt) {}
    MessageImpl(const MessageId& messageId, const proto::BrokerEntryMetadata& brokerEntryMetadata,
                const proto::MessageMetadata& metadata, const SharedBuffer& payload,
                const optional<proto::SingleMessageMetadata>& singleMetadata,
                const std::shared_ptr<std::string>& topicName, optional<EncryptionContext> encryptionContext);

    const Message::StringMap& properties();

    const std::string& getPartitionKey() const;
    bool hasPartitionKey() const;

    const std::string& getOrderingKey() const;
    bool hasOrderingKey() const;

    uint64_t getPublishTimestamp() const;
    uint64_t getEventTimestamp() const;

    /**
     * Get the topic Name from which this message originated from
     */
    const std::string& getTopicName();

    /**
     * Set a valid topicName
     */
    void setTopicName(const std::shared_ptr<std::string>& topicName);

    int getRedeliveryCount();
    void setRedeliveryCount(int count);

    bool hasSchemaVersion() const;
    const std::string& getSchemaVersion() const;
    void setSchemaVersion(const std::string& value);
    void convertKeyValueToPayload(const SchemaInfo& schemaInfo);
    void convertPayloadToKeyValue(const SchemaInfo& schemaInfo);
    KeyValueEncodingType getKeyValueEncodingType(const SchemaInfo& schemaInfo);

    friend class PulsarWrapper;
    friend class MessageBuilder;

    MessageId messageId;
    proto::BrokerEntryMetadata brokerEntryMetadata;
    proto::MessageMetadata metadata;
    SharedBuffer payload;
    std::shared_ptr<KeyValueImpl> keyValuePtr;
    ClientConnection* cnx_;
    std::shared_ptr<std::string> topicName_;
    int redeliveryCount_;
    bool hasSchemaVersion_;
    const std::string* schemaVersion_;
    std::weak_ptr<class ConsumerImpl> consumerPtr_;
    const optional<EncryptionContext> encryptionContext_;

   private:
    void setReplicationClusters(const std::vector<std::string>& clusters);
    void setProperty(const std::string& name, const std::string& value);
    void disableReplication(bool flag);
    void setPartitionKey(const std::string& partitionKey);
    void setOrderingKey(const std::string& orderingKey);
    void setEventTimestamp(uint64_t eventTimestamp);
    Message::StringMap properties_;
};
}  // namespace pulsar

#endif /* LIB_MESSAGEIMPL_H_ */

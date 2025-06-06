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
#ifndef LIB_COMMANDS_H_
#define LIB_COMMANDS_H_

#include <pulsar/Authentication.h>
#include <pulsar/KeySharedPolicy.h>
#include <pulsar/Message.h>
#include <pulsar/Schema.h>
#include <pulsar/defines.h>

#include <boost/optional.hpp>
#include <set>

#include "ProtoApiEnums.h"
#include "SharedBuffer.h"

using namespace pulsar;

namespace pulsar {

class BatchMessageAcker;
using BatchMessageAckerPtr = std::shared_ptr<BatchMessageAcker>;
class MessageIdImpl;
using MessageIdImplPtr = std::shared_ptr<MessageIdImpl>;
class BitSet;
struct SendArguments;

namespace proto {
class BaseCommand;
class MessageIdData;
class MessageMetadata;
}  // namespace proto

typedef std::shared_ptr<proto::MessageMetadata> MessageMetadataPtr;

/**
 * Construct buffers ready to send for Pulsar client commands.
 *
 * Buffer are already including the 4 byte size at the beginning
 */
class Commands {
   public:
    enum ChecksumType
    {
        Crc32c,
        None
    };
    enum WireFormatConstant
    {
        DefaultMaxMessageSize = (5 * 1024 * 1024 - (10 * 1024)),
        MaxFrameSize = (5 * 1024 * 1024)
    };

    enum SubscriptionMode
    {
        // Make the subscription to be backed by a durable cursor that will retain messages and persist the
        // current
        // position
        SubscriptionModeDurable,

        // Lightweight subscription mode that doesn't have a durable cursor associated
        SubscriptionModeNonDurable
    };

    const static uint16_t magicCrc32c = 0x0e01;

    const static uint16_t magicBrokerEntryMetadata = 0x0e02;

    const static int checksumSize = 4;

    static SharedBuffer newConnect(const AuthenticationPtr& authentication, const std::string& logicalAddress,
                                   bool connectingThroughProxy, const std::string& clientVersion,
                                   Result& result);

    static SharedBuffer newAuthResponse(const AuthenticationPtr& authentication, Result& result);

    static SharedBuffer newPartitionMetadataRequest(const std::string& topic, uint64_t requestId);

    static SharedBuffer newLookup(const std::string& topic, const bool authoritative, uint64_t requestId,
                                  const std::string& listenerName);

    static SharedBuffer newGetSchema(const std::string& topic, const std::string& version,
                                     uint64_t requestId);

    static PairSharedBuffer newSend(SharedBuffer& headers, proto::BaseCommand& cmd, ChecksumType checksumType,
                                    const SendArguments& args);

    static SharedBuffer newSubscribe(
        const std::string& topic, const std::string& subscription, uint64_t consumerId, uint64_t requestId,
        CommandSubscribe_SubType subType, const std::string& consumerName, SubscriptionMode subscriptionMode,
        boost::optional<MessageId> startMessageId, bool readCompacted,
        const std::map<std::string, std::string>& metadata,
        const std::map<std::string, std::string>& subscriptionProperties, const SchemaInfo& schemaInfo,
        CommandSubscribe_InitialPosition subscriptionInitialPosition, bool replicateSubscriptionState,
        const KeySharedPolicy& keySharedPolicy, int priorityLevel = 0);

    static SharedBuffer newUnsubscribe(uint64_t consumerId, uint64_t requestId);

    static SharedBuffer newProducer(const std::string& topic, uint64_t producerId,
                                    const std::string& producerName, uint64_t requestId,
                                    const std::map<std::string, std::string>& metadata,
                                    const SchemaInfo& schemaInfo, uint64_t epoch,
                                    bool userProvidedProducerName, bool encrypted,
                                    ProducerAccessMode accessMode, boost::optional<uint64_t> topicEpoch,
                                    const std::string& initialSubscriptionName);

    static SharedBuffer newAck(uint64_t consumerId, int64_t ledgerId, int64_t entryId, const BitSet& ackSet,
                               CommandAck_AckType ackType);
    static SharedBuffer newAck(uint64_t consumerId, int64_t ledgerId, int64_t entryId, const BitSet& ackSet,
                               CommandAck_AckType ackType, uint64_t requestId);
    static SharedBuffer newAck(uint64_t consumerId, int64_t ledgerId, int64_t entryId, const BitSet& ackSet,
                               CommandAck_AckType ackType, CommandAck_ValidationError validationError);

    static SharedBuffer newMultiMessageAck(uint64_t consumerId, const std::set<MessageId>& msgIds);
    static SharedBuffer newMultiMessageAck(uint64_t consumerId, const std::set<MessageId>& msgIds,
                                           uint64_t requestId);

    static SharedBuffer newFlow(uint64_t consumerId, uint32_t messagePermits);

    static SharedBuffer newCloseProducer(uint64_t producerId, uint64_t requestId);

    static SharedBuffer newCloseConsumer(uint64_t consumerId, uint64_t requestId);

    static SharedBuffer newPing();
    static SharedBuffer newPong();

    static SharedBuffer newRedeliverUnacknowledgedMessages(uint64_t consumerId,
                                                           const std::set<MessageId>& messageIds);

    static std::string messageType(BaseCommand_Type type);

    static void initBatchMessageMetadata(const Message& msg, pulsar::proto::MessageMetadata& batchMetadata);

    static uint64_t serializeSingleMessagesToBatchPayload(SharedBuffer& batchPayload,
                                                          const std::vector<Message>& messages);

    static Message deSerializeSingleMessageInBatch(Message& batchedMessage, int32_t batchIndex,
                                                   int32_t batchSize, const BatchMessageAckerPtr& acker);

    static MessageIdImplPtr getMessageIdImpl(const MessageId& messageId);

    static SharedBuffer newConsumerStats(uint64_t consumerId, uint64_t requestId);

    static SharedBuffer newSeek(uint64_t consumerId, uint64_t requestId, const MessageId& messageId);
    static SharedBuffer newSeek(uint64_t consumerId, uint64_t requestId, uint64_t timestamp);
    static SharedBuffer newGetLastMessageId(uint64_t consumerId, uint64_t requestId);
    static SharedBuffer newGetTopicsOfNamespace(const std::string& nsName,
                                                CommandGetTopicsOfNamespace_Mode mode, uint64_t requestId);

    static bool peerSupportsGetLastMessageId(int32_t peerVersion);
    static bool peerSupportsActiveConsumerListener(int32_t peerVersion);
    static bool peerSupportsMultiMessageAcknowledgement(int32_t peerVersion);
    static bool peerSupportsJsonSchemaAvroFormat(int32_t peerVersion);
    static bool peerSupportsGetOrCreateSchema(int32_t peerVersion);

   private:
    Commands();

    static SharedBuffer writeMessageWithSize(const proto::BaseCommand& cmd);
};

} /* namespace pulsar */

#endif /* LIB_COMMANDS_H_ */

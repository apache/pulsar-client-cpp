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
#ifndef LIB_CONSUMERIMPL_H_
#define LIB_CONSUMERIMPL_H_

#include <pulsar/Reader.h>

#include <boost/optional.hpp>
#include <functional>
#include <memory>
#include <utility>

#include "BrokerConsumerStatsImpl.h"
#include "Commands.h"
#include "CompressionCodec.h"
#include "ConsumerImplBase.h"
#include "ConsumerInterceptors.h"
#include "MapCache.h"
#include "MessageIdImpl.h"
#include "NegativeAcksTracker.h"
#include "Synchronized.h"
#include "TestUtil.h"
#include "TimeUtils.h"
#include "UnboundedBlockingQueue.h"
#include "lib/SynchronizedHashMap.h"

namespace pulsar {
class UnAckedMessageTrackerInterface;
class ExecutorService;
class ConsumerImpl;
class MessageCrypto;
class GetLastMessageIdResponse;
typedef std::shared_ptr<MessageCrypto> MessageCryptoPtr;
typedef std::shared_ptr<Backoff> BackoffPtr;
typedef std::function<void(bool processSuccess)> ProcessDLQCallBack;

class AckGroupingTracker;
using AckGroupingTrackerPtr = std::shared_ptr<AckGroupingTracker>;
class BitSet;
class ConsumerStatsBase;
using ConsumerStatsBasePtr = std::shared_ptr<ConsumerStatsBase>;
class UnAckedMessageTracker;
using UnAckedMessageTrackerPtr = std::shared_ptr<UnAckedMessageTrackerInterface>;

namespace proto {
class CommandMessage;
class BrokerEntryMetadata;
class MessageMetadata;
}  // namespace proto

enum ConsumerTopicType
{
    NonPartitioned,
    Partitioned
};

const static std::string SYSTEM_PROPERTY_REAL_TOPIC = "REAL_TOPIC";
const static std::string PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
const static std::string DLQ_GROUP_TOPIC_SUFFIX = "-DLQ";

class ConsumerImpl : public ConsumerImplBase {
   public:
    ConsumerImpl(const ClientImplPtr client, const std::string& topic, const std::string& subscriptionName,
                 const ConsumerConfiguration&, bool isPersistent, const ConsumerInterceptorsPtr& interceptors,
                 const ExecutorServicePtr listenerExecutor = ExecutorServicePtr(), bool hasParent = false,
                 const ConsumerTopicType consumerTopicType = NonPartitioned,
                 Commands::SubscriptionMode = Commands::SubscriptionModeDurable,
                 boost::optional<MessageId> startMessageId = boost::none);
    ~ConsumerImpl();
    void setPartitionIndex(int partitionIndex);
    int getPartitionIndex();
    void sendFlowPermitsToBroker(const ClientConnectionPtr& cnx, int numMessages);
    uint64_t getConsumerId();
    void messageReceived(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                         bool& isChecksumValid, proto::BrokerEntryMetadata& brokerEntryMetadata,
                         proto::MessageMetadata& msgMetadata, SharedBuffer& payload);
    void messageProcessed(Message& msg, bool track = true);
    void activeConsumerChanged(bool isActive);
    inline CommandSubscribe_SubType getSubType();
    inline CommandSubscribe_InitialPosition getInitialPosition();

    std::pair<MessageId, bool /* readyToAck */> prepareIndividualAck(const MessageId& messageId);
    std::pair<MessageId, bool /* readyToAck */> prepareCumulativeAck(const MessageId& messageId);

    // overrided methods from ConsumerImplBase
    Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture() override;
    const std::string& getSubscriptionName() const override;
    const std::string& getTopic() const override;
    Result receive(Message& msg) override;
    Result receive(Message& msg, int timeout) override;
    void receiveAsync(ReceiveCallback callback) override;
    void unsubscribeAsync(ResultCallback callback) override;
    void acknowledgeAsync(const MessageId& msgId, ResultCallback callback) override;
    void acknowledgeAsync(const MessageIdList& messageIdList, ResultCallback callback) override;
    void acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) override;
    void closeAsync(ResultCallback callback) override;
    void start() override;
    void shutdown() override;
    bool isClosed() override;
    bool isOpen() override;
    Result pauseMessageListener() override;
    Result resumeMessageListener() override;
    void redeliverUnacknowledgedMessages() override;
    void redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) override;
    const std::string& getName() const override;
    int getNumOfPrefetchedMessages() const override;
    void getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) override;
    void getLastMessageIdAsync(BrokerGetLastMessageIdCallback callback) override;
    void seekAsync(const MessageId& msgId, ResultCallback callback) override;
    void seekAsync(uint64_t timestamp, ResultCallback callback) override;
    void negativeAcknowledge(const MessageId& msgId) override;
    bool isConnected() const override;
    uint64_t getNumberOfConnectedConsumer() override;
    void hasMessageAvailableAsync(HasMessageAvailableCallback callback) override;

    virtual void disconnectConsumer();
    Result fetchSingleMessageFromBroker(Message& msg);

    virtual bool isCumulativeAcknowledgementAllowed(ConsumerType consumerType);

    virtual void redeliverMessages(const std::set<MessageId>& messageIds);

    virtual bool isReadCompacted();
    void beforeConnectionChange(ClientConnection& cnx) override;
    void onNegativeAcksSend(const std::set<MessageId>& messageIds);

   protected:
    // overrided methods from HandlerBase
    void connectionOpened(const ClientConnectionPtr& cnx) override;
    void connectionFailed(Result result) override;

    // impl methods from ConsumerImpl base
    bool hasEnoughMessagesForBatchReceive() const override;
    void notifyBatchPendingReceivedCallback(const BatchReceiveCallback& callback) override;

    void handleCreateConsumer(const ClientConnectionPtr& cnx, Result result);

    void internalListener();

    void internalConsumerChangeListener(bool isActive);

    void cancelTimers() noexcept;

    ConsumerStatsBasePtr consumerStatsBasePtr_;

   private:
    std::atomic_bool waitingForZeroQueueSizeMessage;
    std::shared_ptr<ConsumerImpl> get_shared_this_ptr();
    bool uncompressMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::MessageIdData& messageIdData,
                                   const proto::MessageMetadata& metadata, SharedBuffer& payload,
                                   bool checkMaxMessageSize);
    void discardCorruptedMessage(const ClientConnectionPtr& cnx, const proto::MessageIdData& messageId,
                                 CommandAck_ValidationError validationError);
    void increaseAvailablePermits(const ClientConnectionPtr& currentCnx, int delta = 1);
    void drainIncomingMessageQueue(size_t count);
    uint32_t receiveIndividualMessagesFromBatch(const ClientConnectionPtr& cnx, Message& batchedMessage,
                                                const BitSet& ackSet, int redeliveryCount);
    bool isPriorBatchIndex(int32_t idx);
    bool isPriorEntryIndex(int64_t idx);
    void brokerConsumerStatsListener(Result, BrokerConsumerStatsImpl, BrokerConsumerStatsCallback);

    bool decryptMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                const proto::MessageMetadata& metadata, SharedBuffer& payload);

    // TODO - Convert these functions to lambda when we move to C++11
    Result receiveHelper(Message& msg);
    Result receiveHelper(Message& msg, int timeout);
    void executeNotifyCallback(Message& msg);
    void notifyPendingReceivedCallback(Result result, Message& message, const ReceiveCallback& callback);
    void failPendingReceiveCallback();
    void setNegativeAcknowledgeEnabledForTesting(bool enabled) override;
    void trackMessage(const MessageId& messageId);
    void internalGetLastMessageIdAsync(const BackoffPtr& backoff, TimeDuration remainTime,
                                       const DeadlineTimerPtr& timer,
                                       BrokerGetLastMessageIdCallback callback);

    boost::optional<MessageId> clearReceiveQueue();
    void seekAsyncInternal(long requestId, SharedBuffer seek, const MessageId& seekId, long timestamp,
                           ResultCallback callback);
    void processPossibleToDLQ(const MessageId& messageId, ProcessDLQCallBack cb);

    std::mutex mutexForReceiveWithZeroQueueSize;
    const ConsumerConfiguration config_;
    DeadLetterPolicy deadLetterPolicy_;
    const std::string subscription_;
    std::string originalSubscriptionName_;
    const bool isPersistent_;
    MessageListener messageListener_;
    ConsumerEventListenerPtr eventListener_;
    bool hasParent_;
    ConsumerTopicType consumerTopicType_;

    const Commands::SubscriptionMode subscriptionMode_;

    UnboundedBlockingQueue<Message> incomingMessages_;
    std::atomic_int incomingMessagesSize_ = {0};
    std::queue<ReceiveCallback> pendingReceives_;
    std::atomic_int availablePermits_;
    const int receiverQueueRefillThreshold_;
    uint64_t consumerId_;
    std::string consumerName_;
    std::string consumerStr_;
    int32_t partitionIndex_ = -1;
    Promise<Result, ConsumerImplBaseWeakPtr> consumerCreatedPromise_;
    std::atomic_bool messageListenerRunning_;
    CompressionCodecProvider compressionCodecProvider_;
    UnAckedMessageTrackerPtr unAckedMessageTrackerPtr_;
    BrokerConsumerStatsImpl brokerConsumerStats_;
    NegativeAcksTracker negativeAcksTracker_;
    AckGroupingTrackerPtr ackGroupingTrackerPtr_;

    MessageCryptoPtr msgCrypto_;
    const bool readCompacted_;

    SynchronizedHashMap<MessageId, std::vector<Message>> possibleSendToDeadLetterTopicMessages_;
    std::shared_ptr<Promise<Result, Producer>> deadLetterProducer_;
    std::mutex createProducerLock_;

    // Make the access to `lastDequedMessageId_` and `lastMessageIdInBroker_` thread safe
    mutable std::mutex mutexForMessageId_;
    MessageId lastDequedMessageId_{MessageId::earliest()};
    MessageId lastMessageIdInBroker_{MessageId::earliest()};

    std::atomic_bool duringSeek_{false};
    Synchronized<boost::optional<MessageId>> startMessageId_;
    Synchronized<MessageId> seekMessageId_{MessageId::earliest()};

    class ChunkedMessageCtx {
       public:
        ChunkedMessageCtx() : totalChunks_(0) {}
        ChunkedMessageCtx(int totalChunks, int totalChunkMessageSize)
            : totalChunks_(totalChunks), chunkedMsgBuffer_(SharedBuffer::allocate(totalChunkMessageSize)) {
            chunkedMessageIds_.reserve(totalChunks);
        }

        ChunkedMessageCtx(const ChunkedMessageCtx&) = delete;
        // Here we don't use =default to be compatible with GCC 4.8
        ChunkedMessageCtx(ChunkedMessageCtx&& rhs) noexcept
            : totalChunks_(rhs.totalChunks_),
              chunkedMsgBuffer_(std::move(rhs.chunkedMsgBuffer_)),
              chunkedMessageIds_(std::move(rhs.chunkedMessageIds_)) {}

        bool validateChunkId(int chunkId) const noexcept { return chunkId == numChunks(); }

        void appendChunk(const MessageId& messageId, const SharedBuffer& payload) {
            chunkedMessageIds_.emplace_back(messageId);
            chunkedMsgBuffer_.write(payload.data(), payload.readableBytes());
            receivedTimeMs_ = TimeUtils::currentTimeMillis();
        }

        bool isCompleted() const noexcept { return totalChunks_ == numChunks(); }

        const SharedBuffer& getBuffer() const noexcept { return chunkedMsgBuffer_; }

        const std::vector<MessageId>& getChunkedMessageIds() const noexcept { return chunkedMessageIds_; }

        long getReceivedTimeMs() const noexcept { return receivedTimeMs_; }

        friend std::ostream& operator<<(std::ostream& os, const ChunkedMessageCtx& ctx) {
            return os << "ChunkedMessageCtx " << ctx.chunkedMsgBuffer_.readableBytes() << " of "
                      << ctx.chunkedMsgBuffer_.writerIndex() << " bytes, " << ctx.numChunks() << " of "
                      << ctx.totalChunks_ << " chunks";
        }

       private:
        const int totalChunks_;
        SharedBuffer chunkedMsgBuffer_;
        std::vector<MessageId> chunkedMessageIds_;
        long receivedTimeMs_;

        int numChunks() const noexcept { return static_cast<int>(chunkedMessageIds_.size()); }
    };

    const size_t maxPendingChunkedMessage_;
    // if queue size is reasonable (most of the time equal to number of producers try to publish messages
    // concurrently on the topic) then it guards against broken chunked message which was not fully published
    const bool autoAckOldestChunkedMessageOnQueueFull_;

    // The key is UUID, value is the associated ChunkedMessageCtx of the chunked message.
    std::unordered_map<std::string, ChunkedMessageCtx> chunkedMessagesMap_;
    // This list contains all the keys of `chunkedMessagesMap_`, each key is an UUID that identifies a pending
    // chunked message. Once the number of pending chunked messages exceeds the limit, the oldest UUIDs and
    // the associated ChunkedMessageCtx will be removed.
    std::list<std::string> pendingChunkedMessageUuidQueue_;

    // The key is UUID, value is the associated ChunkedMessageCtx of the chunked message.
    MapCache<std::string, ChunkedMessageCtx> chunkedMessageCache_;
    mutable std::mutex chunkProcessMutex_;

    const long expireTimeOfIncompleteChunkedMessageMs_;
    DeadlineTimerPtr checkExpiredChunkedTimer_;
    std::atomic_bool expireChunkMessageTaskScheduled_{false};

    ConsumerInterceptorsPtr interceptors_;

    void triggerCheckExpiredChunkedTimer();
    void discardChunkMessages(std::string uuid, MessageId messageId, bool autoAck);

    /**
     * Process a chunk. If the chunk is the last chunk of a message, concatenate all buffered chunks into the
     * payload and return it.
     *
     * @param payload the payload of a chunk
     * @param metadata the message metadata
     * @param messageIdData
     * @param cnx
     * @param messageId
     *
     * @return the concatenated payload if chunks are concatenated into a completed message payload
     *   successfully, else Optional::empty()
     */
    boost::optional<SharedBuffer> processMessageChunk(const SharedBuffer& payload,
                                                      const proto::MessageMetadata& metadata,
                                                      const proto::MessageIdData& messageIdData,
                                                      const ClientConnectionPtr& cnx, MessageId& messageId);

    friend class PulsarFriend;

    // these two declared friend to access setNegativeAcknowledgeEnabledForTesting
    friend class MultiTopicsConsumerImpl;

    FRIEND_TEST(ConsumerTest, testRedeliveryOfDecryptionFailedMessages);
    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testBatchUnAckedMessageTracker);
    FRIEND_TEST(ConsumerTest, testNegativeAcksTrackerClose);
    FRIEND_TEST(DeadLetterQueueTest, testAutoSetDLQTopicName);
};

} /* namespace pulsar */

#endif /* LIB_CONSUMERIMPL_H_ */

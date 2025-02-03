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
#include "ConsumerImpl.h"

#include <pulsar/DeadLetterPolicyBuilder.h>
#include <pulsar/MessageIdBuilder.h>

#include <algorithm>

#include "AckGroupingTracker.h"
#include "AckGroupingTrackerDisabled.h"
#include "AckGroupingTrackerEnabled.h"
#include "AsioDefines.h"
#include "BatchMessageAcker.h"
#include "BatchedMessageIdImpl.h"
#include "BitSet.h"
#include "ChunkMessageIdImpl.h"
#include "ClientConnection.h"
#include "ClientImpl.h"
#include "Commands.h"
#include "ExecutorService.h"
#include "GetLastMessageIdResponse.h"
#include "LogUtils.h"
#include "MessageCrypto.h"
#include "MessageIdUtil.h"
#include "MessageImpl.h"
#include "MessagesImpl.h"
#include "ProducerConfigurationImpl.h"
#include "PulsarApi.pb.h"
#include "ResultUtils.h"
#include "TimeUtils.h"
#include "TopicName.h"
#include "UnAckedMessageTrackerDisabled.h"
#include "UnAckedMessageTrackerEnabled.h"
#include "Utils.h"
#include "stats/ConsumerStatsDisabled.h"
#include "stats/ConsumerStatsImpl.h"

namespace pulsar {

DECLARE_LOG_OBJECT()

using std::chrono::milliseconds;
using std::chrono::seconds;

static boost::optional<MessageId> getStartMessageId(const boost::optional<MessageId>& startMessageId,
                                                    bool inclusive) {
    if (!inclusive || !startMessageId) {
        return startMessageId;
    }
    // The default ledger id and entry id of a chunked message refer the fields of the last chunk. When the
    // start message id is inclusive, we need to start from the first chunk.
    auto chunkMsgIdImpl =
        dynamic_cast<const ChunkMessageIdImpl*>(Commands::getMessageIdImpl(startMessageId.value()).get());
    if (chunkMsgIdImpl) {
        return boost::optional<MessageId>{chunkMsgIdImpl->getChunkedMessageIds().front()};
    }
    return startMessageId;
}

ConsumerImpl::ConsumerImpl(const ClientImplPtr client, const std::string& topic,
                           const std::string& subscriptionName, const ConsumerConfiguration& conf,
                           bool isPersistent, const ConsumerInterceptorsPtr& interceptors,
                           const ExecutorServicePtr listenerExecutor /* = NULL by default */,
                           bool hasParent /* = false by default */,
                           const ConsumerTopicType consumerTopicType /* = NonPartitioned by default */,
                           Commands::SubscriptionMode subscriptionMode,
                           boost::optional<MessageId> startMessageId)
    : ConsumerImplBase(
          client, topic,
          Backoff(milliseconds(client->getClientConfig().getInitialBackoffIntervalMs()),
                  milliseconds(client->getClientConfig().getMaxBackoffIntervalMs()), milliseconds(0)),
          conf, listenerExecutor ? listenerExecutor : client->getListenerExecutorProvider()->get()),
      waitingForZeroQueueSizeMessage(false),
      config_(conf),
      subscription_(subscriptionName),
      originalSubscriptionName_(subscriptionName),
      isPersistent_(isPersistent),
      messageListener_(config_.getMessageListener()),
      eventListener_(config_.getConsumerEventListener()),
      hasParent_(hasParent),
      consumerTopicType_(consumerTopicType),
      subscriptionMode_(subscriptionMode),
      // This is the initial capacity of the queue
      incomingMessages_(std::max(config_.getReceiverQueueSize(), 1)),
      availablePermits_(0),
      receiverQueueRefillThreshold_(config_.getReceiverQueueSize() / 2),
      consumerId_(client->newConsumerId()),
      consumerStr_("[" + topic + ", " + subscriptionName + ", " + std::to_string(consumerId_) + "] "),
      messageListenerRunning_(!conf.isStartPaused()),
      negativeAcksTracker_(std::make_shared<NegativeAcksTracker>(client, *this, conf)),
      readCompacted_(conf.isReadCompacted()),
      startMessageId_(getStartMessageId(startMessageId, conf.isStartMessageIdInclusive())),
      maxPendingChunkedMessage_(conf.getMaxPendingChunkedMessage()),
      autoAckOldestChunkedMessageOnQueueFull_(conf.isAutoAckOldestChunkedMessageOnQueueFull()),
      expireTimeOfIncompleteChunkedMessageMs_(conf.getExpireTimeOfIncompleteChunkedMessageMs()),
      interceptors_(interceptors) {
    // Initialize un-ACKed messages OT tracker.
    if (conf.getUnAckedMessagesTimeoutMs() != 0) {
        if (conf.getTickDurationInMs() > 0) {
            unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
                conf.getUnAckedMessagesTimeoutMs(), conf.getTickDurationInMs(), client, *this));
        } else {
            unAckedMessageTrackerPtr_.reset(
                new UnAckedMessageTrackerEnabled(conf.getUnAckedMessagesTimeoutMs(), client, *this));
        }
    } else {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerDisabled());
    }
    unAckedMessageTrackerPtr_->start();

    // Setup stats reporter.
    unsigned int statsIntervalInSeconds = client->getClientConfig().getStatsIntervalInSeconds();
    if (statsIntervalInSeconds) {
        consumerStatsBasePtr_ = std::make_shared<ConsumerStatsImpl>(
            consumerStr_, client->getIOExecutorProvider()->get(), statsIntervalInSeconds);
    } else {
        consumerStatsBasePtr_ = std::make_shared<ConsumerStatsDisabled>();
    }
    consumerStatsBasePtr_->start();

    // Create msgCrypto
    if (conf.isEncryptionEnabled()) {
        msgCrypto_ = std::make_shared<MessageCrypto>(consumerStr_, false);
    }

    // Config dlq
    auto deadLetterPolicy = conf.getDeadLetterPolicy();
    if (deadLetterPolicy.getMaxRedeliverCount() > 0) {
        auto deadLetterPolicyBuilder =
            DeadLetterPolicyBuilder()
                .maxRedeliverCount(deadLetterPolicy.getMaxRedeliverCount())
                .initialSubscriptionName(deadLetterPolicy.getInitialSubscriptionName());
        if (deadLetterPolicy.getDeadLetterTopic().empty()) {
            deadLetterPolicyBuilder.deadLetterTopic(topic + "-" + subscriptionName + DLQ_GROUP_TOPIC_SUFFIX);
        } else {
            deadLetterPolicyBuilder.deadLetterTopic(deadLetterPolicy.getDeadLetterTopic());
        }
        deadLetterPolicy_ = deadLetterPolicyBuilder.build();
    }

    checkExpiredChunkedTimer_ = executor_->createDeadlineTimer();
}

ConsumerImpl::~ConsumerImpl() {
    LOG_DEBUG(getName() << "~ConsumerImpl");
    if (state_ == Ready) {
        // this could happen at least in this condition:
        //      consumer seek, caused reconnection, if consumer close happened before connection ready,
        //      then consumer will not send closeConsumer to Broker side, and caused a leak of consumer in
        //      broker.
        LOG_WARN(getName() << "Destroyed consumer which was not properly closed");

        ClientConnectionPtr cnx = getCnx().lock();
        ClientImplPtr client = client_.lock();
        if (client && cnx) {
            int requestId = client->newRequestId();
            cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId), requestId);
            cnx->removeConsumer(consumerId_);
            LOG_INFO(getName() << "Closed consumer for race condition: " << consumerId_);
        } else {
            LOG_WARN(getName() << "Client is destroyed and cannot send the CloseConsumer command");
        }
    }
    shutdown();
}

void ConsumerImpl::setPartitionIndex(int partitionIndex) { partitionIndex_ = partitionIndex; }

int ConsumerImpl::getPartitionIndex() { return partitionIndex_; }

uint64_t ConsumerImpl::getConsumerId() { return consumerId_; }

Future<Result, ConsumerImplBaseWeakPtr> ConsumerImpl::getConsumerCreatedFuture() {
    return consumerCreatedPromise_.getFuture();
}

const std::string& ConsumerImpl::getSubscriptionName() const { return originalSubscriptionName_; }

const std::string& ConsumerImpl::getTopic() const { return topic(); }

void ConsumerImpl::start() {
    HandlerBase::start();

    std::weak_ptr<ConsumerImpl> weakSelf{get_shared_this_ptr()};
    auto connectionSupplier = [weakSelf]() -> ClientConnectionPtr {
        auto self = weakSelf.lock();
        if (!self) {
            return nullptr;
        }
        return self->getCnx().lock();
    };

    // NOTE: start() is always called in `ClientImpl`'s method, so lock() returns not null
    const auto requestIdGenerator = client_.lock()->getRequestIdGenerator();
    const auto requestIdSupplier = [requestIdGenerator] { return (*requestIdGenerator)++; };

    // Initialize ackGroupingTrackerPtr_ here because the get_shared_this_ptr() was not initialized until the
    // constructor completed.
    if (TopicName::get(topic())->isPersistent()) {
        if (config_.getAckGroupingTimeMs() > 0) {
            ackGroupingTrackerPtr_.reset(new AckGroupingTrackerEnabled(
                connectionSupplier, requestIdSupplier, consumerId_, config_.isAckReceiptEnabled(),
                config_.getAckGroupingTimeMs(), config_.getAckGroupingMaxSize(),
                client_.lock()->getIOExecutorProvider()->get()));
        } else {
            ackGroupingTrackerPtr_.reset(new AckGroupingTrackerDisabled(
                connectionSupplier, requestIdSupplier, consumerId_, config_.isAckReceiptEnabled()));
        }
    } else {
        LOG_INFO(getName() << "ACK will NOT be sent to broker for this non-persistent topic.");
        ackGroupingTrackerPtr_.reset(new AckGroupingTracker(connectionSupplier, requestIdSupplier,
                                                            consumerId_, config_.isAckReceiptEnabled()));
    }
    ackGroupingTrackerPtr_->start();
}

void ConsumerImpl::beforeConnectionChange(ClientConnection& cnx) { cnx.removeConsumer(consumerId_); }

void ConsumerImpl::onNegativeAcksSend(const std::set<MessageId>& messageIds) {
    interceptors_->onNegativeAcksSend(Consumer(shared_from_this()), messageIds);
}

Future<Result, bool> ConsumerImpl::connectionOpened(const ClientConnectionPtr& cnx) {
    // Do not use bool, only Result.
    Promise<Result, bool> promise;

    if (state_ == Closed) {
        LOG_DEBUG(getName() << "connectionOpened : Consumer is already closed");
        promise.setFailed(ResultAlreadyClosed);
        return promise.getFuture();
    }

    // Register consumer so that we can handle other incomming commands (e.g. ACTIVE_CONSUMER_CHANGE) after
    // sending the subscribe request.
    cnx->registerConsumer(consumerId_, get_shared_this_ptr());

    if (duringSeek()) {
        ackGroupingTrackerPtr_->flushAndClean();
    }

    Lock lockForMessageId(mutexForMessageId_);
    clearReceiveQueue();
    const auto subscribeMessageId =
        (subscriptionMode_ == Commands::SubscriptionModeNonDurable) ? startMessageId_.get() : boost::none;
    lockForMessageId.unlock();

    unAckedMessageTrackerPtr_->clear();

    ClientImplPtr client = client_.lock();
    long requestId = client->newRequestId();
    SharedBuffer cmd = Commands::newSubscribe(
        topic(), subscription_, consumerId_, requestId, getSubType(), getConsumerName(), subscriptionMode_,
        subscribeMessageId, readCompacted_, config_.getProperties(), config_.getSubscriptionProperties(),
        config_.getSchema(), getInitialPosition(), config_.isReplicateSubscriptionStateEnabled(),
        config_.getKeySharedPolicy(), config_.getPriorityLevel());

    // Keep a reference to ensure object is kept alive.
    auto self = get_shared_this_ptr();
    setFirstRequestIdAfterConnect(requestId);
    cnx->sendRequestWithId(cmd, requestId)
        .addListener([this, self, cnx, promise](Result result, const ResponseData& responseData) {
            Result handleResult = handleCreateConsumer(cnx, result);
            if (handleResult == ResultOk) {
                promise.setSuccess();
            } else {
                promise.setFailed(handleResult);
            }
        });

    return promise.getFuture();
}

void ConsumerImpl::connectionFailed(Result result) {
    // Keep a reference to ensure object is kept alive
    auto ptr = get_shared_this_ptr();

    if (!isResultRetryable(result) && consumerCreatedPromise_.setFailed(result)) {
        state_ = Failed;
    }
}

void ConsumerImpl::sendFlowPermitsToBroker(const ClientConnectionPtr& cnx, int numMessages) {
    if (cnx && numMessages > 0) {
        LOG_DEBUG(getName() << "Send more permits: " << numMessages);
        SharedBuffer cmd = Commands::newFlow(consumerId_, static_cast<unsigned int>(numMessages));
        cnx->sendCommand(cmd);
    }
}

Result ConsumerImpl::handleCreateConsumer(const ClientConnectionPtr& cnx, Result result) {
    Result handleResult = ResultOk;

    if (result == ResultOk) {
        LOG_INFO(getName() << "Created consumer on broker " << cnx->cnxString());
        {
            Lock mutexLock(mutex_);
            setCnx(cnx);
            incomingMessages_.clear();
            possibleSendToDeadLetterTopicMessages_.clear();
            state_ = Ready;
            backoff_.reset();
            if (!messageListener_ && config_.getReceiverQueueSize() == 0) {
                // Complicated logic since we don't have a isLocked() function for mutex
                if (waitingForZeroQueueSizeMessage) {
                    sendFlowPermitsToBroker(cnx, 1);
                }
                // Note that the order of lock acquisition must be mutex_ -> pendingReceiveMutex_,
                // otherwise a deadlock will occur.
                Lock pendingReceiveMutexLock(pendingReceiveMutex_);
                if (!pendingReceives_.empty()) {
                    sendFlowPermitsToBroker(cnx, pendingReceives_.size());
                }
            }
            availablePermits_ = 0;
        }

        LOG_DEBUG(getName() << "Send initial flow permits: " << config_.getReceiverQueueSize());
        if (config_.getReceiverQueueSize() != 0) {
            sendFlowPermitsToBroker(cnx, config_.getReceiverQueueSize());
        } else if (messageListener_) {
            sendFlowPermitsToBroker(cnx, 1);
        }
        consumerCreatedPromise_.setValue(get_shared_this_ptr());
    } else {
        if (result == ResultTimeout) {
            // Creating the consumer has timed out. We need to ensure the broker closes the consumer
            // in case it was indeed created, otherwise it might prevent new subscribe operation,
            // since we are not closing the connection
            int requestId = client_.lock()->newRequestId();
            cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId), requestId);
        }

        if (consumerCreatedPromise_.isComplete()) {
            // Consumer had already been initially created, we need to retry connecting in any case
            LOG_WARN(getName() << "Failed to reconnect consumer: " << strResult(result));
            handleResult = ResultRetryable;
        } else {
            // Consumer was not yet created, retry to connect to broker if it's possible
            handleResult = convertToTimeoutIfNecessary(result, creationTimestamp_);
            if (isResultRetryable(handleResult)) {
                LOG_WARN(getName() << "Temporary error in creating consumer: " << strResult(handleResult));
            } else {
                LOG_ERROR(getName() << "Failed to create consumer: " << strResult(handleResult));
                consumerCreatedPromise_.setFailed(handleResult);
                state_ = Failed;
            }
        }
    }

    return handleResult;
}

void ConsumerImpl::unsubscribeAsync(ResultCallback originalCallback) {
    LOG_INFO(getName() << "Unsubscribing");

    auto callback = [this, originalCallback](Result result) {
        if (result == ResultOk) {
            shutdown();
            LOG_INFO(getName() << "Unsubscribed successfully");
        } else {
            state_ = Ready;
            LOG_WARN(getName() << "Failed to unsubscribe: " << result);
        }
        if (originalCallback) {
            originalCallback(result);
        }
    };

    if (state_ != Ready) {
        callback(ResultAlreadyClosed);
        return;
    }

    Lock lock(mutex_);

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        LOG_DEBUG(getName() << "Unsubscribe request sent for consumer - " << consumerId_);
        ClientImplPtr client = client_.lock();
        lock.unlock();
        int requestId = client->newRequestId();
        SharedBuffer cmd = Commands::newUnsubscribe(consumerId_, requestId);
        auto self = get_shared_this_ptr();
        cnx->sendRequestWithId(cmd, requestId)
            .addListener([self, callback](Result result, const ResponseData&) { callback(result); });
    } else {
        Result result = ResultNotConnected;
        lock.unlock();
        LOG_WARN(getName() << "Failed to unsubscribe: " << strResult(result));
        callback(result);
    }
}

void ConsumerImpl::discardChunkMessages(std::string uuid, MessageId messageId, bool autoAck) {
    if (autoAck) {
        acknowledgeAsync(messageId, [uuid, messageId](Result result) {
            if (result != ResultOk) {
                LOG_WARN("Failed to acknowledge discarded chunk, uuid: " << uuid
                                                                         << ", messageId: " << messageId);
            }
        });
    } else {
        trackMessage(messageId);
    }
}

void ConsumerImpl::triggerCheckExpiredChunkedTimer() {
    checkExpiredChunkedTimer_->expires_from_now(milliseconds(expireTimeOfIncompleteChunkedMessageMs_));
    std::weak_ptr<ConsumerImplBase> weakSelf{shared_from_this()};
    checkExpiredChunkedTimer_->async_wait([this, weakSelf](const ASIO_ERROR& ec) -> void {
        auto self = weakSelf.lock();
        if (!self) {
            return;
        }
        if (ec) {
            LOG_DEBUG(getName() << " Check expired chunked messages was failed or cancelled, code[" << ec
                                << "].");
            return;
        }
        Lock lock(chunkProcessMutex_);
        long currentTimeMs = TimeUtils::currentTimeMillis();
        chunkedMessageCache_.removeOldestValuesIf(
            [this, currentTimeMs](const std::string& uuid, const ChunkedMessageCtx& ctx) -> bool {
                bool expired =
                    currentTimeMs > ctx.getReceivedTimeMs() + expireTimeOfIncompleteChunkedMessageMs_;
                if (!expired) {
                    return false;
                }
                for (const MessageId& msgId : ctx.getChunkedMessageIds()) {
                    LOG_INFO("Removing expired chunk messages: uuid: " << uuid << ", messageId: " << msgId);
                    discardChunkMessages(uuid, msgId, true);
                }
                return true;
            });
        triggerCheckExpiredChunkedTimer();
        return;
    });
}

boost::optional<SharedBuffer> ConsumerImpl::processMessageChunk(const SharedBuffer& payload,
                                                                const proto::MessageMetadata& metadata,
                                                                const proto::MessageIdData& messageIdData,
                                                                const ClientConnectionPtr& cnx,
                                                                MessageId& messageId) {
    const auto chunkId = metadata.chunk_id();
    const auto uuid = metadata.uuid();
    LOG_DEBUG("Process message chunk (chunkId: " << chunkId << ", uuid: " << uuid
                                                 << ", messageId: " << messageId << ") of "
                                                 << payload.readableBytes() << " bytes");

    Lock lock(chunkProcessMutex_);

    // Lazy task scheduling to expire incomplete chunk message
    bool expected = false;
    if (expireTimeOfIncompleteChunkedMessageMs_ > 0 &&
        expireChunkMessageTaskScheduled_.compare_exchange_strong(expected, true)) {
        triggerCheckExpiredChunkedTimer();
    }

    auto it = chunkedMessageCache_.find(uuid);

    if (chunkId == 0 && it == chunkedMessageCache_.end()) {
        if (maxPendingChunkedMessage_ > 0 && chunkedMessageCache_.size() >= maxPendingChunkedMessage_) {
            chunkedMessageCache_.removeOldestValues(
                chunkedMessageCache_.size() - maxPendingChunkedMessage_ + 1,
                [this](const std::string& uuid, const ChunkedMessageCtx& ctx) {
                    for (const MessageId& msgId : ctx.getChunkedMessageIds()) {
                        discardChunkMessages(uuid, msgId, autoAckOldestChunkedMessageOnQueueFull_);
                    }
                });
        }
        it = chunkedMessageCache_.putIfAbsent(
            uuid, ChunkedMessageCtx{metadata.num_chunks_from_msg(), metadata.total_chunk_msg_size()});
    }

    auto& chunkedMsgCtx = it->second;
    if (it == chunkedMessageCache_.end() || !chunkedMsgCtx.validateChunkId(chunkId)) {
        auto startMessageId = startMessageId_.get().value_or(MessageId::earliest());
        if (!config_.isStartMessageIdInclusive() && startMessageId.ledgerId() == messageId.ledgerId() &&
            startMessageId.entryId() == messageId.entryId()) {
            // When the start message id is not inclusive, the last chunk of the previous chunked message will
            // be delivered, which is expected and we only need to filter it out.
            chunkedMessageCache_.remove(uuid);
            LOG_INFO("Filtered the chunked message before the start message id (uuid: "
                     << uuid << " chunkId: " << chunkId << ", messageId: " << messageId << ")");
        } else if (it == chunkedMessageCache_.end()) {
            LOG_ERROR("Received an uncached chunk (uuid: " << uuid << " chunkId: " << chunkId
                                                           << ", messageId: " << messageId << ")");
        } else {
            LOG_ERROR("Received a chunk whose chunk id is invalid (uuid: "
                      << uuid << " chunkId: " << chunkId << ", messageId: " << messageId << ")");
            chunkedMessageCache_.remove(uuid);
        }
        lock.unlock();
        increaseAvailablePermits(cnx);
        trackMessage(messageId);
        return boost::none;
    }

    chunkedMsgCtx.appendChunk(messageId, payload);
    if (!chunkedMsgCtx.isCompleted()) {
        lock.unlock();
        increaseAvailablePermits(cnx);
        return boost::none;
    }

    messageId = std::make_shared<ChunkMessageIdImpl>(chunkedMsgCtx.moveChunkedMessageIds())->build();

    LOG_DEBUG("Chunked message completed chunkId: " << chunkId << ", ChunkedMessageCtx: " << chunkedMsgCtx
                                                    << ", sequenceId: " << metadata.sequence_id());

    auto wholePayload = chunkedMsgCtx.getBuffer();
    chunkedMessageCache_.remove(uuid);
    if (uncompressMessageIfNeeded(cnx, messageIdData, metadata, wholePayload, false)) {
        return wholePayload;
    } else {
        return boost::none;
    }
}

void ConsumerImpl::messageReceived(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                   bool& isChecksumValid, proto::BrokerEntryMetadata& brokerEntryMetadata,
                                   proto::MessageMetadata& metadata, SharedBuffer& payload) {
    LOG_DEBUG(getName() << "Received Message -- Size: " << payload.readableBytes());

    if (!decryptMessageIfNeeded(cnx, msg, metadata, payload)) {
        // Message was discarded or not consumed due to decryption failure
        return;
    }

    if (!isChecksumValid) {
        // Message discarded for checksum error
        discardCorruptedMessage(cnx, msg.message_id(), CommandAck_ValidationError_ChecksumMismatch);
        return;
    }

    auto redeliveryCount = msg.redelivery_count();
    const bool isMessageUndecryptable =
        metadata.encryption_keys_size() > 0 && !config_.getCryptoKeyReader().get() &&
        config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::CONSUME;

    const bool isChunkedMessage = metadata.num_chunks_from_msg() > 1;
    if (!isMessageUndecryptable && !isChunkedMessage) {
        if (!uncompressMessageIfNeeded(cnx, msg.message_id(), metadata, payload, true)) {
            // Message was discarded on decompression error
            return;
        }
    }

    const auto& messageIdData = msg.message_id();
    auto messageId = MessageIdBuilder::from(messageIdData).batchIndex(-1).build();

    // Only a non-batched messages can be a chunk
    if (!metadata.has_num_messages_in_batch() && isChunkedMessage) {
        auto optionalPayload = processMessageChunk(payload, metadata, messageIdData, cnx, messageId);
        if (optionalPayload) {
            payload = optionalPayload.value();
        } else {
            return;
        }
    }

    Message m(messageId, brokerEntryMetadata, metadata, payload);
    m.impl_->cnx_ = cnx.get();
    m.impl_->setTopicName(getTopicPtr());
    m.impl_->setRedeliveryCount(msg.redelivery_count());

    if (metadata.has_schema_version()) {
        m.impl_->setSchemaVersion(metadata.schema_version());
    }

    LOG_DEBUG(getName() << " metadata.num_messages_in_batch() = " << metadata.num_messages_in_batch());
    LOG_DEBUG(getName() << " metadata.has_num_messages_in_batch() = "
                        << metadata.has_num_messages_in_batch());

    uint32_t numOfMessageReceived = m.impl_->metadata.num_messages_in_batch();
    if (this->ackGroupingTrackerPtr_->isDuplicate(m.getMessageId())) {
        LOG_DEBUG(getName() << " Ignoring message as it was ACKed earlier by same consumer.");
        increaseAvailablePermits(cnx, numOfMessageReceived);
        return;
    }

    if (metadata.has_num_messages_in_batch()) {
        BitSet::Data words(msg.ack_set_size());
        for (int i = 0; i < words.size(); i++) {
            words[i] = msg.ack_set(i);
        }
        BitSet ackSet{std::move(words)};
        Lock lock(mutex_);
        numOfMessageReceived = receiveIndividualMessagesFromBatch(cnx, m, ackSet, msg.redelivery_count());
    } else {
        // try convert key value data.
        m.impl_->convertPayloadToKeyValue(config_.getSchema());

        const auto startMessageId = startMessageId_.get();
        if (isPersistent_ && startMessageId &&
            m.getMessageId().ledgerId() == startMessageId.value().ledgerId() &&
            m.getMessageId().entryId() == startMessageId.value().entryId() &&
            isPriorEntryIndex(m.getMessageId().entryId())) {
            LOG_DEBUG(getName() << " Ignoring message from before the startMessageId: "
                                << startMessageId.value());
            return;
        }
        if (redeliveryCount >= deadLetterPolicy_.getMaxRedeliverCount()) {
            possibleSendToDeadLetterTopicMessages_.emplace(m.getMessageId(), std::vector<Message>{m});
            if (redeliveryCount > deadLetterPolicy_.getMaxRedeliverCount()) {
                redeliverUnacknowledgedMessages({m.getMessageId()});
                increaseAvailablePermits(cnx);
                return;
            }
        }
        executeNotifyCallback(m);
    }

    if (messageListener_) {
        if (!messageListenerRunning_) {
            return;
        }
        // Trigger message listener callback in a separate thread
        while (numOfMessageReceived--) {
            listenerExecutor_->postWork(std::bind(&ConsumerImpl::internalListener, get_shared_this_ptr()));
        }
    }
}

void ConsumerImpl::activeConsumerChanged(bool isActive) {
    if (eventListener_) {
        listenerExecutor_->postWork(
            std::bind(&ConsumerImpl::internalConsumerChangeListener, get_shared_this_ptr(), isActive));
    }
}

void ConsumerImpl::internalConsumerChangeListener(bool isActive) {
    try {
        if (isActive) {
            eventListener_->becameActive(Consumer(get_shared_this_ptr()), partitionIndex_);
        } else {
            eventListener_->becameInactive(Consumer(get_shared_this_ptr()), partitionIndex_);
        }
    } catch (const std::exception& e) {
        LOG_ERROR(getName() << "Exception thrown from event listener " << e.what());
    }
}

void ConsumerImpl::failPendingReceiveCallback() {
    Message msg;
    Lock lock(pendingReceiveMutex_);
    while (!pendingReceives_.empty()) {
        ReceiveCallback callback = pendingReceives_.front();
        pendingReceives_.pop();
        listenerExecutor_->postWork(std::bind(&ConsumerImpl::notifyPendingReceivedCallback,
                                              get_shared_this_ptr(), ResultAlreadyClosed, msg, callback));
    }
    lock.unlock();
}

void ConsumerImpl::executeNotifyCallback(Message& msg) {
    Lock lock(pendingReceiveMutex_);
    // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
    bool asyncReceivedWaiting = !pendingReceives_.empty();
    ReceiveCallback callback;
    if (asyncReceivedWaiting) {
        callback = pendingReceives_.front();
        pendingReceives_.pop();
    }
    lock.unlock();

    // has pending receive, direct callback.
    if (asyncReceivedWaiting) {
        listenerExecutor_->postWork(std::bind(&ConsumerImpl::notifyPendingReceivedCallback,
                                              get_shared_this_ptr(), ResultOk, msg, callback));
        return;
    }

    // try to add incoming messages.
    // config_.getReceiverQueueSize() != 0 or waiting For ZeroQueueSize Message`
    if (messageListener_ || config_.getReceiverQueueSize() != 0 || waitingForZeroQueueSizeMessage) {
        incomingMessages_.push(msg);
        incomingMessagesSize_.fetch_add(msg.getLength());
    }

    // try trigger pending batch messages
    Lock batchOptionLock(batchReceiveOptionMutex_);
    if (hasEnoughMessagesForBatchReceive()) {
        ConsumerImplBase::notifyBatchPendingReceivedCallback();
    }
}

void ConsumerImpl::notifyBatchPendingReceivedCallback(const BatchReceiveCallback& callback) {
    auto messages = std::make_shared<MessagesImpl>(batchReceivePolicy_.getMaxNumMessages(),
                                                   batchReceivePolicy_.getMaxNumBytes());
    Message msg;
    while (incomingMessages_.popIf(
        msg, [&messages](const Message& peekMsg) { return messages->canAdd(peekMsg); })) {
        messageProcessed(msg);
        Message interceptMsg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
        messages->add(interceptMsg);
    }
    auto self = get_shared_this_ptr();
    listenerExecutor_->postWork(
        [callback, messages, self]() { callback(ResultOk, messages->getMessageList()); });
}

void ConsumerImpl::notifyPendingReceivedCallback(Result result, Message& msg,
                                                 const ReceiveCallback& callback) {
    if (result == ResultOk && config_.getReceiverQueueSize() != 0) {
        messageProcessed(msg);
        msg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
    }
    callback(result, msg);
}

// Zero Queue size is not supported with Batch Messages
uint32_t ConsumerImpl::receiveIndividualMessagesFromBatch(const ClientConnectionPtr& cnx,
                                                          Message& batchedMessage, const BitSet& ackSet,
                                                          int redeliveryCount) {
    auto batchSize = batchedMessage.impl_->metadata.num_messages_in_batch();
    LOG_DEBUG("Received Batch messages of size - " << batchSize
                                                   << " -- msgId: " << batchedMessage.getMessageId());
    const auto startMessageId = startMessageId_.get();

    int skippedMessages = 0;

    auto acker = BatchMessageAckerImpl::create(batchSize);
    std::vector<Message> possibleToDeadLetter;
    for (int i = 0; i < batchSize; i++) {
        // This is a cheap copy since message contains only one shared pointer (impl_)
        Message msg = Commands::deSerializeSingleMessageInBatch(batchedMessage, i, batchSize, acker);
        msg.impl_->setRedeliveryCount(redeliveryCount);
        msg.impl_->setTopicName(batchedMessage.impl_->topicName_);
        msg.impl_->convertPayloadToKeyValue(config_.getSchema());
        if (msg.impl_->brokerEntryMetadata.has_index()) {
            msg.impl_->brokerEntryMetadata.set_index(msg.impl_->brokerEntryMetadata.index() - batchSize + i +
                                                     1);
        }

        if (redeliveryCount >= deadLetterPolicy_.getMaxRedeliverCount()) {
            possibleToDeadLetter.emplace_back(msg);
            if (redeliveryCount > deadLetterPolicy_.getMaxRedeliverCount()) {
                skippedMessages++;
                continue;
            }
        }

        if (startMessageId) {
            const MessageId& msgId = msg.getMessageId();

            // If we are receiving a batch message, we need to discard messages that were prior
            // to the startMessageId
            if (isPersistent_ && msgId.ledgerId() == startMessageId.value().ledgerId() &&
                msgId.entryId() == startMessageId.value().entryId() &&
                isPriorBatchIndex(msgId.batchIndex())) {
                LOG_DEBUG(getName() << "Ignoring message from before the startMessageId"
                                    << msg.getMessageId());
                ++skippedMessages;
                continue;
            }
        }

        if (!ackSet.isEmpty() && !ackSet.get(i)) {
            LOG_DEBUG(getName() << "Ignoring message from " << i
                                << "th message, which has been acknowledged");
            ++skippedMessages;
            continue;
        }

        executeNotifyCallback(msg);
    }

    if (!possibleToDeadLetter.empty()) {
        possibleSendToDeadLetterTopicMessages_.emplace(batchedMessage.getMessageId(), possibleToDeadLetter);
        if (redeliveryCount > deadLetterPolicy_.getMaxRedeliverCount()) {
            redeliverUnacknowledgedMessages({batchedMessage.getMessageId()});
        }
    }

    if (skippedMessages > 0) {
        increaseAvailablePermits(cnx, skippedMessages);
    }

    return batchSize - skippedMessages;
}

bool ConsumerImpl::decryptMessageIfNeeded(const ClientConnectionPtr& cnx, const proto::CommandMessage& msg,
                                          const proto::MessageMetadata& metadata, SharedBuffer& payload) {
    if (!metadata.encryption_keys_size()) {
        return true;
    }

    // If KeyReader is not configured throw exception based on config param
    if (!config_.isEncryptionEnabled()) {
        if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::CONSUME) {
            LOG_WARN(getName() << "CryptoKeyReader is not implemented. Consuming encrypted message.");
            return true;
        } else if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::DISCARD) {
            LOG_WARN(getName() << "Skipping decryption since CryptoKeyReader is not implemented and config "
                                  "is set to discard");
            discardCorruptedMessage(cnx, msg.message_id(), CommandAck_ValidationError_DecryptionError);
        } else {
            LOG_ERROR(getName() << "Message delivery failed since CryptoKeyReader is not implemented to "
                                   "consume encrypted message");
            auto messageId = MessageIdBuilder::from(msg.message_id()).build();
            unAckedMessageTrackerPtr_->add(messageId);
        }
        return false;
    }

    SharedBuffer decryptedPayload;
    if (msgCrypto_->decrypt(metadata, payload, config_.getCryptoKeyReader(), decryptedPayload)) {
        payload = decryptedPayload;
        return true;
    }

    if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::CONSUME) {
        // Note, batch message will fail to consume even if config is set to consume
        LOG_WARN(
            getName() << "Decryption failed. Consuming encrypted message since config is set to consume.");
        return true;
    } else if (config_.getCryptoFailureAction() == ConsumerCryptoFailureAction::DISCARD) {
        LOG_WARN(getName() << "Discarding message since decryption failed and config is set to discard");
        discardCorruptedMessage(cnx, msg.message_id(), CommandAck_ValidationError_DecryptionError);
    } else {
        LOG_ERROR(getName() << "Message delivery failed since unable to decrypt incoming message");
        auto messageId = MessageIdBuilder::from(msg.message_id()).build();
        unAckedMessageTrackerPtr_->add(messageId);
    }
    return false;
}

bool ConsumerImpl::uncompressMessageIfNeeded(const ClientConnectionPtr& cnx,
                                             const proto::MessageIdData& messageIdData,
                                             const proto::MessageMetadata& metadata, SharedBuffer& payload,
                                             bool checkMaxMessageSize) {
    if (!metadata.has_compression()) {
        return true;
    }

    CompressionType compressionType = static_cast<CompressionType>(metadata.compression());

    uint32_t uncompressedSize = metadata.uncompressed_size();
    uint32_t payloadSize = payload.readableBytes();
    if (cnx) {
        if (checkMaxMessageSize && payloadSize > ClientConnection::getMaxMessageSize()) {
            // Uncompressed size is itself corrupted since it cannot be bigger than the MaxMessageSize
            LOG_ERROR(getName() << "Got corrupted payload message size " << payloadSize  //
                                << " at  " << messageIdData.ledgerid() << ":" << messageIdData.entryid());
            discardCorruptedMessage(cnx, messageIdData,
                                    CommandAck_ValidationError_UncompressedSizeCorruption);
            return false;
        }
    } else {
        LOG_ERROR("Connection not ready for Consumer - " << getConsumerId());
        return false;
    }

    if (!CompressionCodecProvider::getCodec(compressionType).decode(payload, uncompressedSize, payload)) {
        LOG_ERROR(getName() << "Failed to decompress message with " << uncompressedSize  //
                            << " at  " << messageIdData.ledgerid() << ":" << messageIdData.entryid());
        discardCorruptedMessage(cnx, messageIdData, CommandAck_ValidationError_DecompressionError);
        return false;
    }

    return true;
}

void ConsumerImpl::discardCorruptedMessage(const ClientConnectionPtr& cnx,
                                           const proto::MessageIdData& messageId,
                                           CommandAck_ValidationError validationError) {
    LOG_ERROR(getName() << "Discarding corrupted message at " << messageId.ledgerid() << ":"
                        << messageId.entryid());

    SharedBuffer cmd = Commands::newAck(consumerId_, messageId.ledgerid(), messageId.entryid(), {},
                                        CommandAck_AckType_Individual, validationError);

    cnx->sendCommand(cmd);
    increaseAvailablePermits(cnx);
}

void ConsumerImpl::internalListener() {
    if (!messageListenerRunning_) {
        return;
    }
    Message msg;
    if (!incomingMessages_.pop(msg, std::chrono::milliseconds(0))) {
        // This will only happen when the connection got reset and we cleared the queue
        return;
    }
    trackMessage(msg.getMessageId());
    try {
        consumerStatsBasePtr_->receivedMessage(msg, ResultOk);
        lastDequedMessageId_ = msg.getMessageId();
        Consumer consumer{get_shared_this_ptr()};
        Message interceptMsg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
        messageListener_(consumer, interceptMsg);
    } catch (const std::exception& e) {
        LOG_ERROR(getName() << "Exception thrown from listener" << e.what());
    }
    messageProcessed(msg, false);
}

Result ConsumerImpl::fetchSingleMessageFromBroker(Message& msg) {
    if (config_.getReceiverQueueSize() != 0) {
        LOG_ERROR(getName() << " Can't use receiveForZeroQueueSize if the queue size is not 0");
        return ResultInvalidConfiguration;
    }

    // Using RAII for locking
    Lock lock(mutexForReceiveWithZeroQueueSize);

    // Just being cautious
    if (incomingMessages_.size() != 0) {
        LOG_ERROR(
            getName() << "The incoming message queue should never be greater than 0 when Queue size is 0");
        incomingMessages_.clear();
    }

    {
        // Lock mutex_ to prevent a race condition with handleCreateConsumer.
        // If handleCreateConsumer is executed after setting waitingForZeroQueueSizeMessage to true and
        // before calling sendFlowPermitsToBroker, the result may be that a flow permit is sent twice.
        Lock lock(mutex_);
        waitingForZeroQueueSizeMessage = true;
        // If connection_ is nullptr, sendFlowPermitsToBroker does nothing.
        // In other words, a flow permit will not be sent until setCnx(cnx) is executed in
        // handleCreateConsumer.
        sendFlowPermitsToBroker(getCnx().lock(), 1);
    }

    while (true) {
        if (!incomingMessages_.pop(msg)) {
            return ResultInterrupted;
        }

        {
            // Lock needed to prevent race between connectionOpened and the check "msg.impl_->cnx_ ==
            // currentCnx.get())"
            Lock localLock(mutex_);
            // if message received due to an old flow - discard it and wait for the message from the
            // latest flow command
            ClientConnectionPtr currentCnx = getCnx().lock();
            if (msg.impl_->cnx_ == currentCnx.get()) {
                waitingForZeroQueueSizeMessage = false;
                // Can't use break here else it may trigger a race with connection opened.

                localLock.unlock();
                msg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
                return ResultOk;
            }
        }
    }
}

Result ConsumerImpl::receive(Message& msg) {
    Result res = receiveHelper(msg);
    consumerStatsBasePtr_->receivedMessage(msg, res);
    return res;
}

void ConsumerImpl::receiveAsync(ReceiveCallback callback) {
    Message msg;

    // fail the callback if consumer is closing or closed
    if (state_ != Ready) {
        callback(ResultAlreadyClosed, msg);
        return;
    }

    if (messageListener_) {
        LOG_ERROR(getName() << "Can not receive when a listener has been set");
        callback(ResultInvalidConfiguration, msg);
        return;
    }

    Lock mutexlock(mutex_, std::defer_lock);
    if (config_.getReceiverQueueSize() == 0) {
        // Lock mutex_ to prevent a race condition with handleCreateConsumer.
        // If handleCreateConsumer is executed after pushing the callback to pendingReceives_ and
        // before calling sendFlowPermitsToBroker, the result may be that a flow permit is sent twice.
        // Note that the order of lock acquisition must be mutex_ -> pendingReceiveMutex_,
        // otherwise a deadlock will occur.
        mutexlock.lock();
    }

    Lock pendingReceiveMutexLock(pendingReceiveMutex_);
    if (incomingMessages_.pop(msg, std::chrono::milliseconds(0))) {
        pendingReceiveMutexLock.unlock();
        if (config_.getReceiverQueueSize() == 0) {
            mutexlock.unlock();
        }
        messageProcessed(msg);
        msg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
        callback(ResultOk, msg);
    } else if (config_.getReceiverQueueSize() == 0) {
        pendingReceives_.push(callback);
        // If connection_ is nullptr, sendFlowPermitsToBroker does nothing.
        // In other words, a flow permit will not be sent until setCnx(cnx) is executed in
        // handleCreateConsumer.
        sendFlowPermitsToBroker(getCnx().lock(), 1);
        pendingReceiveMutexLock.unlock();
        mutexlock.unlock();
    } else {
        pendingReceives_.push(callback);
        pendingReceiveMutexLock.unlock();
    }
}

Result ConsumerImpl::receiveHelper(Message& msg) {
    if (state_ != Ready) {
        return ResultAlreadyClosed;
    }

    if (messageListener_) {
        LOG_ERROR(getName() << "Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }

    if (config_.getReceiverQueueSize() == 0) {
        return fetchSingleMessageFromBroker(msg);
    }

    if (!incomingMessages_.pop(msg)) {
        return ResultInterrupted;
    }

    messageProcessed(msg);
    msg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
    return ResultOk;
}

Result ConsumerImpl::receive(Message& msg, int timeout) {
    Result res = receiveHelper(msg, timeout);
    consumerStatsBasePtr_->receivedMessage(msg, res);
    return res;
}

Result ConsumerImpl::receiveHelper(Message& msg, int timeout) {
    if (config_.getReceiverQueueSize() == 0) {
        LOG_WARN(getName() << "Can't use this function if the queue size is 0");
        return ResultInvalidConfiguration;
    }

    if (state_ != Ready) {
        return ResultAlreadyClosed;
    }

    if (messageListener_) {
        LOG_ERROR(getName() << "Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }

    if (incomingMessages_.pop(msg, std::chrono::milliseconds(timeout))) {
        messageProcessed(msg);
        msg = interceptors_->beforeConsume(Consumer(shared_from_this()), msg);
        return ResultOk;
    } else {
        if (state_ != Ready) {
            return ResultAlreadyClosed;
        }
        return ResultTimeout;
    }
}

void ConsumerImpl::messageProcessed(Message& msg, bool track) {
    Lock lock(mutexForMessageId_);
    lastDequedMessageId_ = msg.getMessageId();
    lock.unlock();

    incomingMessagesSize_.fetch_sub(msg.getLength());

    ClientConnectionPtr currentCnx = getCnx().lock();
    if (currentCnx && msg.impl_->cnx_ != currentCnx.get()) {
        LOG_DEBUG(getName() << "Not adding permit since connection is different.");
        return;
    }

    if (!hasParent_) {
        increaseAvailablePermits(currentCnx);
    }
    if (track) {
        trackMessage(msg.getMessageId());
    }
}

/**
 * Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that
 * was
 * not seen by the application
 * `startMessageId_` is updated so that we can discard messages after delivery restarts.
 */
void ConsumerImpl::clearReceiveQueue() {
    if (duringSeek()) {
        if (!hasSoughtByTimestamp_.load(std::memory_order_acquire)) {
            startMessageId_ = seekMessageId_.get();
        }
        SeekStatus expected = SeekStatus::COMPLETED;
        if (seekStatus_.compare_exchange_strong(expected, SeekStatus::NOT_STARTED)) {
            auto seekCallback = seekCallback_.release();
            executor_->postWork([seekCallback] { seekCallback(ResultOk); });
        }
        return;
    } else if (subscriptionMode_ == Commands::SubscriptionModeDurable) {
        return;
    }

    Message nextMessageInQueue;
    if (incomingMessages_.peekAndClear(nextMessageInQueue)) {
        // There was at least one message pending in the queue
        const MessageId& nextMessageId = nextMessageInQueue.getMessageId();
        auto previousMessageId = (nextMessageId.batchIndex() >= 0)
                                     ? MessageIdBuilder()
                                           .ledgerId(nextMessageId.ledgerId())
                                           .entryId(nextMessageId.entryId())
                                           .batchIndex(nextMessageId.batchIndex() - 1)
                                           .batchSize(nextMessageId.batchSize())
                                           .build()
                                     : MessageIdBuilder()
                                           .ledgerId(nextMessageId.ledgerId())
                                           .entryId(nextMessageId.entryId() - 1)
                                           .build();
        startMessageId_ = previousMessageId;
    } else if (lastDequedMessageId_ != MessageId::earliest()) {
        // If the queue was empty we need to restart from the message just after the last one that has been
        // dequeued
        // in the past
        startMessageId_ = lastDequedMessageId_;
    }
}

void ConsumerImpl::increaseAvailablePermits(const ClientConnectionPtr& currentCnx, int delta) {
    int newAvailablePermits = availablePermits_.fetch_add(delta) + delta;

    while (newAvailablePermits >= receiverQueueRefillThreshold_ && messageListenerRunning_) {
        if (availablePermits_.compare_exchange_weak(newAvailablePermits, 0)) {
            sendFlowPermitsToBroker(currentCnx, newAvailablePermits);
            break;
        }
    }
}

void ConsumerImpl::increaseAvailablePermits(const Message& msg) {
    ClientConnectionPtr currentCnx = getCnx().lock();
    if (currentCnx && msg.impl_->cnx_ != currentCnx.get()) {
        LOG_DEBUG(getName() << "Not adding permit since connection is different.");
        return;
    }

    increaseAvailablePermits(currentCnx);
}

inline CommandSubscribe_SubType ConsumerImpl::getSubType() {
    ConsumerType type = config_.getConsumerType();
    switch (type) {
        case ConsumerExclusive:
            return CommandSubscribe_SubType_Exclusive;

        case ConsumerShared:
            return CommandSubscribe_SubType_Shared;

        case ConsumerFailover:
            return CommandSubscribe_SubType_Failover;

        case ConsumerKeyShared:
            return CommandSubscribe_SubType_Key_Shared;
    }
    BOOST_THROW_EXCEPTION(std::logic_error("Invalid ConsumerType enumeration value"));
}

inline CommandSubscribe_InitialPosition ConsumerImpl::getInitialPosition() {
    InitialPosition initialPosition = config_.getSubscriptionInitialPosition();
    switch (initialPosition) {
        case InitialPositionLatest:
            return CommandSubscribe_InitialPosition_Latest;

        case InitialPositionEarliest:
            return CommandSubscribe_InitialPosition_Earliest;
    }
    BOOST_THROW_EXCEPTION(std::logic_error("Invalid InitialPosition enumeration value"));
}

void ConsumerImpl::acknowledgeAsync(const MessageId& msgId, ResultCallback callback) {
    auto pair = prepareIndividualAck(msgId);
    const auto& msgIdToAck = pair.first;
    const bool readyToAck = pair.second;
    if (readyToAck) {
        ackGroupingTrackerPtr_->addAcknowledge(msgIdToAck, callback);
    } else {
        if (callback) {
            callback(ResultOk);
        }
    }
    interceptors_->onAcknowledge(Consumer(shared_from_this()), ResultOk, msgId);
}

void ConsumerImpl::acknowledgeAsync(const MessageIdList& messageIdList, ResultCallback callback) {
    MessageIdList messageIdListToAck;
    // TODO: Need to check if the consumer is ready. Same to all other public methods
    for (auto&& msgId : messageIdList) {
        auto pair = prepareIndividualAck(msgId);
        const auto& msgIdToAck = pair.first;
        const bool readyToAck = pair.second;
        if (readyToAck) {
            messageIdListToAck.emplace_back(msgIdToAck);
        }
        // Invoking `onAcknowledge` for all message ids no matter if it's ready to ack. This is consistent
        // with the Java client.
        interceptors_->onAcknowledge(Consumer(shared_from_this()), ResultOk, msgId);
    }
    this->ackGroupingTrackerPtr_->addAcknowledgeList(messageIdListToAck, callback);
}

void ConsumerImpl::acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) {
    if (!isCumulativeAcknowledgementAllowed(config_.getConsumerType())) {
        interceptors_->onAcknowledgeCumulative(Consumer(shared_from_this()),
                                               ResultCumulativeAcknowledgementNotAllowedError, msgId);
        if (callback) {
            callback(ResultCumulativeAcknowledgementNotAllowedError);
        }
        return;
    }
    auto pair = prepareCumulativeAck(msgId);
    const auto& msgIdToAck = pair.first;
    const auto& readyToAck = pair.second;
    if (readyToAck) {
        consumerStatsBasePtr_->messageAcknowledged(ResultOk, CommandAck_AckType_Cumulative, 1);
        unAckedMessageTrackerPtr_->removeMessagesTill(msgIdToAck);
        ackGroupingTrackerPtr_->addAcknowledgeCumulative(msgIdToAck, callback);
    } else if (callback) {
        callback(ResultOk);
    }
    interceptors_->onAcknowledgeCumulative(Consumer(shared_from_this()), ResultOk, msgId);
}

bool ConsumerImpl::isCumulativeAcknowledgementAllowed(ConsumerType consumerType) {
    return consumerType != ConsumerKeyShared && consumerType != ConsumerShared;
}

std::pair<MessageId, bool> ConsumerImpl::prepareIndividualAck(const MessageId& messageId) {
    auto messageIdImpl = Commands::getMessageIdImpl(messageId);
    auto batchedMessageIdImpl = std::dynamic_pointer_cast<BatchedMessageIdImpl>(messageIdImpl);

    auto batchSize = messageId.batchSize();
    if (!batchedMessageIdImpl || batchedMessageIdImpl->ackIndividual(messageId.batchIndex())) {
        consumerStatsBasePtr_->messageAcknowledged(ResultOk, CommandAck_AckType_Individual,
                                                   (batchSize > 0) ? batchSize : 1);
        unAckedMessageTrackerPtr_->remove(messageId);
        possibleSendToDeadLetterTopicMessages_.remove(messageId);
        if (std::dynamic_pointer_cast<ChunkMessageIdImpl>(messageIdImpl)) {
            return std::make_pair(messageId, true);
        }
        return std::make_pair(discardBatch(messageId), true);
    } else if (config_.isBatchIndexAckEnabled()) {
        return std::make_pair(messageId, true);
    } else {
        return std::make_pair(MessageId{}, false);
    }
}

std::pair<MessageId, bool> ConsumerImpl::prepareCumulativeAck(const MessageId& messageId) {
    auto messageIdImpl = Commands::getMessageIdImpl(messageId);
    auto batchedMessageIdImpl = std::dynamic_pointer_cast<BatchedMessageIdImpl>(messageIdImpl);

    if (!batchedMessageIdImpl || batchedMessageIdImpl->ackCumulative(messageId.batchIndex())) {
        return std::make_pair(discardBatch(messageId), true);
    } else if (config_.isBatchIndexAckEnabled()) {
        return std::make_pair(messageId, true);
    } else {
        if (batchedMessageIdImpl->shouldAckPreviousMessageId()) {
            return std::make_pair(batchedMessageIdImpl->getPreviousMessageId(), true);
        } else {
            return std::make_pair(MessageId{}, false);
        }
    }
}

void ConsumerImpl::negativeAcknowledge(const MessageId& messageId) {
    unAckedMessageTrackerPtr_->remove(messageId);
    negativeAcksTracker_->add(messageId);
}

void ConsumerImpl::disconnectConsumer() { disconnectConsumer(boost::none); }

void ConsumerImpl::disconnectConsumer(const boost::optional<std::string>& assignedBrokerUrl) {
    LOG_INFO("Broker notification of Closed consumer: "
             << consumerId_ << (assignedBrokerUrl ? (" assignedBrokerUrl: " + assignedBrokerUrl.get()) : ""));
    resetCnx();
    scheduleReconnection(assignedBrokerUrl);
}

void ConsumerImpl::closeAsync(ResultCallback originalCallback) {
    auto callback = [this, originalCallback](Result result, bool alreadyClosed = false) {
        shutdown();
        if (result == ResultOk) {
            if (!alreadyClosed) {
                LOG_INFO(getName() << "Closed consumer " << consumerId_);
            }
        } else {
            LOG_WARN(getName() << "Failed to close consumer: " << result);
        }
        if (originalCallback) {
            originalCallback(result);
        }
    };

    auto state = state_.load();
    if (state == Closing || state == Closed) {
        callback(ResultOk, true);
        return;
    }

    LOG_INFO(getName() << "Closing consumer for topic " << topic());
    state_ = Closing;
    incomingMessages_.close();

    // Flush pending grouped ACK requests.
    if (ackGroupingTrackerPtr_) {
        ackGroupingTrackerPtr_->close();
    }
    negativeAcksTracker_->close();

    ClientConnectionPtr cnx = getCnx().lock();
    if (!cnx) {
        // If connection is gone, also the consumer is closed on the broker side
        callback(ResultOk);
        return;
    }

    ClientImplPtr client = client_.lock();
    if (!client) {
        // Client was already destroyed
        callback(ResultOk);
        return;
    }

    cancelTimers();

    int requestId = client->newRequestId();
    auto self = get_shared_this_ptr();
    cnx->sendRequestWithId(Commands::newCloseConsumer(consumerId_, requestId), requestId)
        .addListener([self, callback](Result result, const ResponseData&) { callback(result); });
}

const std::string& ConsumerImpl::getName() const { return consumerStr_; }

void ConsumerImpl::shutdown() {
    if (ackGroupingTrackerPtr_) {
        ackGroupingTrackerPtr_->close();
    }
    incomingMessages_.clear();
    possibleSendToDeadLetterTopicMessages_.clear();
    resetCnx();
    interceptors_->close();
    auto client = client_.lock();
    if (client) {
        client->cleanupConsumer(this);
    }
    negativeAcksTracker_->close();
    cancelTimers();
    consumerCreatedPromise_.setFailed(ResultAlreadyClosed);
    failPendingReceiveCallback();
    failPendingBatchReceiveCallback();
    state_ = Closed;
}

bool ConsumerImpl::isClosed() { return state_ == Closed; }

bool ConsumerImpl::isOpen() { return state_ == Ready; }

Result ConsumerImpl::pauseMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    messageListenerRunning_ = false;
    return ResultOk;
}

Result ConsumerImpl::resumeMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }

    if (messageListenerRunning_) {
        // Not paused
        return ResultOk;
    }
    messageListenerRunning_ = true;
    const size_t count = incomingMessages_.size();

    for (size_t i = 0; i < count; i++) {
        // Trigger message listener callback in a separate thread
        listenerExecutor_->postWork(std::bind(&ConsumerImpl::internalListener, get_shared_this_ptr()));
    }
    // Check current permits and determine whether to send FLOW command
    this->increaseAvailablePermits(getCnx().lock(), 0);
    return ResultOk;
}

void ConsumerImpl::redeliverUnacknowledgedMessages() {
    static std::set<MessageId> emptySet;
    redeliverMessages(emptySet);
    unAckedMessageTrackerPtr_->clear();
}

void ConsumerImpl::redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) {
    if (messageIds.empty()) {
        return;
    }
    if (config_.getConsumerType() != ConsumerShared && config_.getConsumerType() != ConsumerKeyShared) {
        redeliverUnacknowledgedMessages();
        return;
    }

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v2) {
            auto needRedeliverMsgs = std::make_shared<std::set<MessageId>>();
            auto needCallBack = std::make_shared<std::atomic<int>>(messageIds.size());
            auto self = get_shared_this_ptr();
            // TODO Support MAX_REDELIVER_UNACKNOWLEDGED Avoid redelivering too many messages
            for (const auto& msgId : messageIds) {
                processPossibleToDLQ(msgId,
                                     [self, needRedeliverMsgs, &msgId, needCallBack](bool processSuccess) {
                                         if (!processSuccess) {
                                             needRedeliverMsgs->emplace(msgId);
                                         }
                                         if (--(*needCallBack) == 0 && !needRedeliverMsgs->empty()) {
                                             self->redeliverMessages(*needRedeliverMsgs);
                                         }
                                     });
            }
        }
    } else {
        LOG_WARN("Connection not ready for Consumer - " << getConsumerId());
    }
}

void ConsumerImpl::redeliverMessages(const std::set<MessageId>& messageIds) {
    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v2) {
            cnx->sendCommand(Commands::newRedeliverUnacknowledgedMessages(consumerId_, messageIds));
            LOG_DEBUG("Sending RedeliverUnacknowledgedMessages command for Consumer - " << getConsumerId());
        }
    } else {
        LOG_DEBUG("Connection not ready for Consumer - " << getConsumerId());
    }
}

int ConsumerImpl::getNumOfPrefetchedMessages() const { return incomingMessages_.size(); }

void ConsumerImpl::getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) {
    if (state_ != Ready) {
        LOG_ERROR(getName() << "Client connection is not open, please try again later.")
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }

    Lock lock(mutex_);
    if (brokerConsumerStats_.isValid()) {
        LOG_DEBUG(getName() << "Serving data from cache");
        BrokerConsumerStatsImpl brokerConsumerStats = brokerConsumerStats_;
        lock.unlock();
        callback(ResultOk,
                 BrokerConsumerStats(std::make_shared<BrokerConsumerStatsImpl>(brokerConsumerStats_)));
        return;
    }
    lock.unlock();

    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v8) {
            ClientImplPtr client = client_.lock();
            uint64_t requestId = client->newRequestId();
            LOG_DEBUG(getName() << " Sending ConsumerStats Command for Consumer - " << getConsumerId()
                                << ", requestId - " << requestId);

            cnx->newConsumerStats(consumerId_, requestId)
                .addListener(std::bind(&ConsumerImpl::brokerConsumerStatsListener, get_shared_this_ptr(),
                                       std::placeholders::_1, std::placeholders::_2, callback));
            return;
        } else {
            LOG_ERROR(getName() << " Operation not supported since server protobuf version "
                                << cnx->getServerProtocolVersion() << " is older than proto::v7");
            callback(ResultUnsupportedVersionError, BrokerConsumerStats());
            return;
        }
    }
    LOG_ERROR(getName() << " Client Connection not ready for Consumer");
    callback(ResultNotConnected, BrokerConsumerStats());
}

void ConsumerImpl::brokerConsumerStatsListener(Result res, BrokerConsumerStatsImpl brokerConsumerStats,
                                               BrokerConsumerStatsCallback callback) {
    if (res == ResultOk) {
        Lock lock(mutex_);
        brokerConsumerStats.setCacheTime(config_.getBrokerConsumerStatsCacheTimeInMs());
        brokerConsumerStats_ = brokerConsumerStats;
    }

    if (callback) {
        callback(res, BrokerConsumerStats(std::make_shared<BrokerConsumerStatsImpl>(brokerConsumerStats)));
    }
}

void ConsumerImpl::seekAsync(const MessageId& msgId, ResultCallback callback) {
    const auto state = state_.load();
    if (state == Closed || state == Closing) {
        LOG_ERROR(getName() << "Client connection already closed.");
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    ClientImplPtr client = client_.lock();
    if (!client) {
        LOG_ERROR(getName() << "Client is expired when seekAsync " << msgId);
        return;
    }
    const auto requestId = client->newRequestId();
    seekAsyncInternal(requestId, Commands::newSeek(consumerId_, requestId, msgId), SeekArg{msgId}, callback);
}

void ConsumerImpl::seekAsync(uint64_t timestamp, ResultCallback callback) {
    const auto state = state_.load();
    if (state == Closed || state == Closing) {
        LOG_ERROR(getName() << "Client connection already closed.");
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    ClientImplPtr client = client_.lock();
    if (!client) {
        LOG_ERROR(getName() << "Client is expired when seekAsync " << timestamp);
        return;
    }
    const auto requestId = client->newRequestId();
    seekAsyncInternal(requestId, Commands::newSeek(consumerId_, requestId, timestamp), SeekArg{timestamp},
                      callback);
}

bool ConsumerImpl::isReadCompacted() { return readCompacted_; }

void ConsumerImpl::hasMessageAvailableAsync(HasMessageAvailableCallback callback) {
    bool compareMarkDeletePosition;
    {
        std::lock_guard<std::mutex> lock{mutexForMessageId_};
        compareMarkDeletePosition =
            (lastDequedMessageId_ == MessageId::earliest()) &&
            (startMessageId_.get().value_or(MessageId::earliest()) == MessageId::latest());
    }
    if (compareMarkDeletePosition || hasSoughtByTimestamp_.load(std::memory_order_acquire)) {
        auto self = get_shared_this_ptr();
        getLastMessageIdAsync([self, callback](Result result, const GetLastMessageIdResponse& response) {
            if (result != ResultOk) {
                callback(result, {});
                return;
            }
            auto handleResponse = [self, response, callback] {
                if (response.hasMarkDeletePosition() && response.getLastMessageId().entryId() >= 0) {
                    // We only care about comparing ledger ids and entry ids as mark delete position
                    // doesn't have other ids such as batch index
                    auto compareResult = compareLedgerAndEntryId(response.getMarkDeletePosition(),
                                                                 response.getLastMessageId());
                    callback(ResultOk, self->config_.isStartMessageIdInclusive() ? compareResult <= 0
                                                                                 : compareResult < 0);
                } else {
                    callback(ResultOk, false);
                }
            };
            if (self->config_.isStartMessageIdInclusive() &&
                !self->hasSoughtByTimestamp_.load(std::memory_order_acquire)) {
                self->seekAsync(response.getLastMessageId(), [callback, handleResponse](Result result) {
                    if (result != ResultOk) {
                        callback(result, {});
                        return;
                    }
                    handleResponse();
                });
            } else {
                handleResponse();
            }
        });
    } else {
        if (hasMoreMessages()) {
            callback(ResultOk, true);
            return;
        }
        auto self = get_shared_this_ptr();
        getLastMessageIdAsync(
            [this, self, callback](Result result, const GetLastMessageIdResponse& response) {
                callback(result, (result == ResultOk) && hasMoreMessages());
            });
    }
}

void ConsumerImpl::getLastMessageIdAsync(BrokerGetLastMessageIdCallback callback) {
    const auto state = state_.load();
    if (state == Closed || state == Closing) {
        LOG_ERROR(getName() << "Client connection already closed.");
        if (callback) {
            callback(ResultAlreadyClosed, GetLastMessageIdResponse());
        }
        return;
    }

    TimeDuration operationTimeout = seconds(client_.lock()->conf().getOperationTimeoutSeconds());
    BackoffPtr backoff = std::make_shared<Backoff>(milliseconds(100), operationTimeout * 2, milliseconds(0));
    DeadlineTimerPtr timer = executor_->createDeadlineTimer();

    internalGetLastMessageIdAsync(backoff, operationTimeout, timer, callback);
}

void ConsumerImpl::internalGetLastMessageIdAsync(const BackoffPtr& backoff, TimeDuration remainTime,
                                                 const DeadlineTimerPtr& timer,
                                                 BrokerGetLastMessageIdCallback callback) {
    ClientConnectionPtr cnx = getCnx().lock();
    if (cnx) {
        if (cnx->getServerProtocolVersion() >= proto::v12) {
            ClientImplPtr client = client_.lock();
            uint64_t requestId = client->newRequestId();
            LOG_DEBUG(getName() << " Sending getLastMessageId Command for Consumer - " << getConsumerId()
                                << ", requestId - " << requestId);

            auto self = get_shared_this_ptr();
            cnx->newGetLastMessageId(consumerId_, requestId)
                .addListener([this, self, callback](Result result, const GetLastMessageIdResponse& response) {
                    if (result == ResultOk) {
                        LOG_DEBUG(getName() << "getLastMessageId: " << response);
                        Lock lock(mutexForMessageId_);
                        lastMessageIdInBroker_ = response.getLastMessageId();
                        lock.unlock();
                    } else {
                        LOG_ERROR(getName() << "Failed to getLastMessageId: " << result);
                    }
                    callback(result, response);
                });
        } else {
            LOG_ERROR(getName() << " Operation not supported since server protobuf version "
                                << cnx->getServerProtocolVersion() << " is older than proto::v12");
            callback(ResultUnsupportedVersionError, MessageId());
        }
    } else {
        TimeDuration next = std::min(remainTime, backoff->next());
        if (toMillis(next) <= 0) {
            LOG_ERROR(getName() << " Client Connection not ready for Consumer");
            callback(ResultNotConnected, MessageId());
            return;
        }
        remainTime -= next;

        timer->expires_from_now(next);

        auto self = shared_from_this();
        timer->async_wait([this, backoff, remainTime, timer, next, callback,
                           self](const ASIO_ERROR& ec) -> void {
            if (ec == ASIO::error::operation_aborted) {
                LOG_DEBUG(getName() << " Get last message id operation was cancelled, code[" << ec << "].");
                return;
            }
            if (ec) {
                LOG_ERROR(getName() << " Failed to get last message id, code[" << ec << "].");
                return;
            }
            LOG_WARN(getName() << " Could not get connection while getLastMessageId -- Will try again in "
                               << toMillis(next) << " ms")
            this->internalGetLastMessageIdAsync(backoff, remainTime, timer, callback);
        });
    }
}

void ConsumerImpl::setNegativeAcknowledgeEnabledForTesting(bool enabled) {
    negativeAcksTracker_->setEnabledForTesting(enabled);
}

void ConsumerImpl::trackMessage(const MessageId& messageId) {
    if (hasParent_) {
        unAckedMessageTrackerPtr_->remove(messageId);
    } else {
        unAckedMessageTrackerPtr_->add(messageId);
    }
}

bool ConsumerImpl::isConnected() const { return !getCnx().expired() && state_ == Ready; }

uint64_t ConsumerImpl::getNumberOfConnectedConsumer() { return isConnected() ? 1 : 0; }

void ConsumerImpl::seekAsyncInternal(long requestId, SharedBuffer seek, const SeekArg& seekArg,
                                     ResultCallback callback) {
    ClientConnectionPtr cnx = getCnx().lock();
    if (!cnx) {
        LOG_ERROR(getName() << " Client Connection not ready for Consumer");
        callback(ResultNotConnected);
        return;
    }

    auto expected = SeekStatus::NOT_STARTED;
    if (!seekStatus_.compare_exchange_strong(expected, SeekStatus::IN_PROGRESS)) {
        LOG_ERROR(getName() << " attempted to seek " << seekArg << " when the status is "
                            << static_cast<int>(expected));
        callback(ResultNotAllowedError);
        return;
    }

    const auto originalSeekMessageId = seekMessageId_.get();
    if (boost::get<uint64_t>(&seekArg)) {
        hasSoughtByTimestamp_.store(true, std::memory_order_release);
    } else {
        seekMessageId_ = *boost::get<MessageId>(&seekArg);
    }
    seekStatus_ = SeekStatus::IN_PROGRESS;
    seekCallback_ = std::move(callback);
    LOG_INFO(getName() << " Seeking subscription to " << seekArg);

    std::weak_ptr<ConsumerImpl> weakSelf{get_shared_this_ptr()};

    cnx->sendRequestWithId(seek, requestId)
        .addListener([this, weakSelf, callback, originalSeekMessageId](Result result,
                                                                       const ResponseData& responseData) {
            auto self = weakSelf.lock();
            if (!self) {
                callback(result);
                return;
            }
            if (result == ResultOk) {
                LOG_INFO(getName() << "Seek successfully");
                ackGroupingTrackerPtr_->flushAndClean();
                incomingMessages_.clear();
                Lock lock(mutexForMessageId_);
                lastDequedMessageId_ = MessageId::earliest();
                lock.unlock();
                if (getCnx().expired()) {
                    // It's during reconnection, complete the seek future after connection is established
                    seekStatus_ = SeekStatus::COMPLETED;
                } else {
                    if (!hasSoughtByTimestamp_.load(std::memory_order_acquire)) {
                        startMessageId_ = seekMessageId_.get();
                    }
                    seekCallback_.release()(result);
                }
            } else {
                LOG_ERROR(getName() << "Failed to seek: " << result);
                seekMessageId_ = originalSeekMessageId;
                seekStatus_ = SeekStatus::NOT_STARTED;
                seekCallback_.release()(result);
            }
        });
}

bool ConsumerImpl::isPriorBatchIndex(int32_t idx) {
    return config_.isStartMessageIdInclusive() ? idx < startMessageId_.get().value().batchIndex()
                                               : idx <= startMessageId_.get().value().batchIndex();
}

bool ConsumerImpl::isPriorEntryIndex(int64_t idx) {
    return config_.isStartMessageIdInclusive() ? idx < startMessageId_.get().value().entryId()
                                               : idx <= startMessageId_.get().value().entryId();
}

bool ConsumerImpl::hasEnoughMessagesForBatchReceive() const {
    if (batchReceivePolicy_.getMaxNumMessages() <= 0 && batchReceivePolicy_.getMaxNumBytes() <= 0) {
        return false;
    }
    return (batchReceivePolicy_.getMaxNumMessages() > 0 &&
            incomingMessages_.size() >= batchReceivePolicy_.getMaxNumMessages()) ||
           (batchReceivePolicy_.getMaxNumBytes() > 0 &&
            incomingMessagesSize_ >= batchReceivePolicy_.getMaxNumBytes());
}

std::shared_ptr<ConsumerImpl> ConsumerImpl::get_shared_this_ptr() {
    return std::dynamic_pointer_cast<ConsumerImpl>(shared_from_this());
}

void ConsumerImpl::cancelTimers() noexcept {
    ASIO_ERROR ec;
    batchReceiveTimer_->cancel(ec);
    checkExpiredChunkedTimer_->cancel(ec);
    unAckedMessageTrackerPtr_->stop();
    consumerStatsBasePtr_->stop();
}

void ConsumerImpl::processPossibleToDLQ(const MessageId& messageId, ProcessDLQCallBack cb) {
    auto messages = possibleSendToDeadLetterTopicMessages_.find(messageId);
    if (!messages) {
        cb(false);
        return;
    }

    // Initialize deadLetterProducer_
    if (!deadLetterProducer_) {
        std::lock_guard<std::mutex> createLock(createProducerLock_);
        if (!deadLetterProducer_) {
            deadLetterProducer_ = std::make_shared<Promise<Result, Producer>>();
            ProducerConfiguration producerConfiguration;
            producerConfiguration.setSchema(config_.getSchema());
            producerConfiguration.setBlockIfQueueFull(false);
            producerConfiguration.setBatchingEnabled(false);
            producerConfiguration.impl_->initialSubscriptionName =
                deadLetterPolicy_.getInitialSubscriptionName();
            ClientImplPtr client = client_.lock();
            if (client) {
                auto self = get_shared_this_ptr();
                client->createProducerAsync(
                    deadLetterPolicy_.getDeadLetterTopic(), producerConfiguration,
                    [self](Result res, Producer producer) {
                        if (res == ResultOk) {
                            self->deadLetterProducer_->setValue(producer);
                        } else {
                            LOG_ERROR("Dead letter producer create exception with topic: "
                                      << self->deadLetterPolicy_.getDeadLetterTopic() << " ex: " << res);
                            self->deadLetterProducer_.reset();
                        }
                    });
            } else {
                LOG_WARN(getName() << "Client is destroyed and cannot create dead letter producer.");
                return;
            }
        }
    }

    for (const auto& message : messages.value()) {
        std::weak_ptr<ConsumerImpl> weakSelf{get_shared_this_ptr()};
        deadLetterProducer_->getFuture().addListener([weakSelf, message, messageId, cb](Result res,
                                                                                        Producer producer) {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            auto originMessageId = message.getMessageId();
            std::stringstream originMessageIdStr;
            originMessageIdStr << originMessageId;
            MessageBuilder msgBuilder;
            msgBuilder.setAllocatedContent(const_cast<void*>(message.getData()), message.getLength())
                .setProperties(message.getProperties())
                .setProperty(PROPERTY_ORIGIN_MESSAGE_ID, originMessageIdStr.str())
                .setProperty(SYSTEM_PROPERTY_REAL_TOPIC, message.getTopicName());
            if (message.hasPartitionKey()) {
                msgBuilder.setPartitionKey(message.getPartitionKey());
            }
            if (message.hasOrderingKey()) {
                msgBuilder.setOrderingKey(message.getOrderingKey());
            }
            producer.sendAsync(msgBuilder.build(), [weakSelf, originMessageId, messageId, cb](
                                                       Result res, const MessageId& messageIdInDLQ) {
                auto self = weakSelf.lock();
                if (!self) {
                    return;
                }
                if (res == ResultOk) {
                    if (self->state_ != Ready) {
                        LOG_WARN(
                            "Send to the DLQ successfully, but consumer is not ready. ignore acknowledge : "
                            << self->state_);
                        cb(false);
                        return;
                    }
                    self->possibleSendToDeadLetterTopicMessages_.remove(messageId);
                    self->acknowledgeAsync(originMessageId, [weakSelf, originMessageId, cb](Result result) {
                        auto self = weakSelf.lock();
                        if (!self) {
                            return;
                        }
                        if (result != ResultOk) {
                            LOG_WARN("{" << self->topic() << "} {" << self->subscription_ << "} {"
                                         << self->getConsumerName() << "} Failed to acknowledge the message {"
                                         << originMessageId
                                         << "} of the original topic but send to the DLQ successfully : "
                                         << result);
                            cb(false);
                        } else {
                            LOG_DEBUG("Send msg:" << originMessageId
                                                  << "to DLQ success and acknowledge success.");
                            cb(true);
                        }
                    });
                } else {
                    LOG_WARN("{" << self->topic() << "} {" << self->subscription_ << "} {"
                                 << self->getConsumerName() << "} Failed to send DLQ message to {"
                                 << self->deadLetterPolicy_.getDeadLetterTopic() << "} for message id "
                                 << "{" << originMessageId << "} : " << res);
                    cb(false);
                }
            });
        });
    }
}

} /* namespace pulsar */

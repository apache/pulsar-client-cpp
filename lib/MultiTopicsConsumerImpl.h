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
#ifndef PULSAR_MULTI_TOPICS_CONSUMER_HEADER
#define PULSAR_MULTI_TOPICS_CONSUMER_HEADER

#include <pulsar/Client.h>

#include <memory>
#include <vector>

#include "Commands.h"
#include "ConsumerImpl.h"
#include "ConsumerInterceptors.h"
#include "Future.h"
#include "Latch.h"
#include "LookupDataResult.h"
#include "SynchronizedHashMap.h"
#include "TestUtil.h"
#include "TimeUtils.h"
#include "UnboundedBlockingQueue.h"

namespace pulsar {
typedef std::shared_ptr<Promise<Result, Consumer>> ConsumerSubResultPromisePtr;

using ConsumerImplPtr = std::shared_ptr<ConsumerImpl>;
class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
class TopicName;
using TopicNamePtr = std::shared_ptr<TopicName>;
class MultiTopicsBrokerConsumerStatsImpl;
using MultiTopicsBrokerConsumerStatsPtr = std::shared_ptr<MultiTopicsBrokerConsumerStatsImpl>;
class UnAckedMessageTrackerInterface;
using UnAckedMessageTrackerPtr = std::shared_ptr<UnAckedMessageTrackerInterface>;
class LookupService;
using LookupServicePtr = std::shared_ptr<LookupService>;

class MultiTopicsConsumerImpl;
class MultiTopicsConsumerImpl : public ConsumerImplBase {
   public:
    MultiTopicsConsumerImpl(const ClientImplPtr& client, const TopicNamePtr& topicName, int numPartitions,
                            const std::string& subscriptionName, const ConsumerConfiguration& conf,
                            const LookupServicePtr& lookupServicePtr,
                            const ConsumerInterceptorsPtr& interceptors,
                            Commands::SubscriptionMode = Commands::SubscriptionModeDurable,
                            const optional<MessageId>& startMessageId = optional<MessageId>{});

    MultiTopicsConsumerImpl(const ClientImplPtr& client, const std::vector<std::string>& topics,
                            const std::string& subscriptionName, const TopicNamePtr& topicName,
                            const ConsumerConfiguration& conf, const LookupServicePtr& lookupServicePtr_,
                            const ConsumerInterceptorsPtr& interceptors,
                            Commands::SubscriptionMode = Commands::SubscriptionModeDurable,
                            const optional<MessageId>& startMessageId = optional<MessageId>{});

    ~MultiTopicsConsumerImpl();
    // overrided methods from ConsumerImplBase
    Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture() override;
    const std::string& getSubscriptionName() const override;
    const std::string& getTopic() const override;
    Result receive(Message& msg) override;
    Result receive(Message& msg, int timeout) override;
    void receiveAsync(const ReceiveCallback& callback) override;
    void unsubscribeAsync(const ResultCallback& callback) override;
    void acknowledgeAsync(const MessageId& msgId, const ResultCallback& callback) override;
    void acknowledgeAsync(const MessageIdList& messageIdList, const ResultCallback& callback) override;
    void acknowledgeCumulativeAsync(const MessageId& msgId, const ResultCallback& callback) override;
    void closeAsync(const ResultCallback& callback) override;
    void start() override;
    void shutdown() override;
    void internalShutdown();
    bool isClosed() override;
    bool isOpen() override;
    Result pauseMessageListener() override;
    Result resumeMessageListener() override;
    void redeliverUnacknowledgedMessages() override;
    void redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) override;
    const std::string& getName() const override;
    int getNumOfPrefetchedMessages() const override;
    void getBrokerConsumerStatsAsync(const BrokerConsumerStatsCallback& callback) override;
    void getLastMessageIdAsync(const BrokerGetLastMessageIdCallback& callback) override;
    void seekAsync(const MessageId& msgId, const ResultCallback& callback) override;
    void seekAsync(uint64_t timestamp, const ResultCallback& callback) override;
    void negativeAcknowledge(const MessageId& msgId) override;
    bool isConnected() const override;
    uint64_t getNumberOfConnectedConsumer() override;
    void hasMessageAvailableAsync(const HasMessageAvailableCallback& callback) override;

    void handleGetConsumerStats(Result, const BrokerConsumerStats&, const LatchPtr&,
                                const MultiTopicsBrokerConsumerStatsPtr&, size_t,
                                const BrokerConsumerStatsCallback&);
    // return first topic name when all topics name valid, or return null pointer
    static std::shared_ptr<TopicName> topicNamesValid(const std::vector<std::string>& topics);
    void unsubscribeOneTopicAsync(const std::string& topic, const ResultCallback& callback);
    Future<Result, Consumer> subscribeOneTopicAsync(const std::string& topic);

   protected:
    const ClientImplWeakPtr client_;
    const std::string subscriptionName_;
    std::string consumerStr_;
    const ConsumerConfiguration conf_;
    typedef SynchronizedHashMap<std::string, ConsumerImplPtr> ConsumerMap;
    ConsumerMap consumers_;
    std::map<std::string, int> topicsPartitions_;
    mutable std::mutex mutex_;
    std::mutex pendingReceiveMutex_;
    UnboundedBlockingQueue<Message> incomingMessages_;
    std::atomic_int incomingMessagesSize_ = {0};
    MessageListener messageListener_;
    DeadlineTimerPtr partitionsUpdateTimer_;
    TimeDuration partitionsUpdateInterval_;
    LookupServicePtr lookupServicePtr_;
    std::shared_ptr<std::atomic<int>> numberTopicPartitions_;
    std::atomic<Result> failedResult{ResultOk};
    Promise<Result, ConsumerImplBaseWeakPtr> multiTopicsConsumerCreatedPromise_;
    UnAckedMessageTrackerPtr unAckedMessageTrackerPtr_;
    const std::vector<std::string> topics_;
    std::queue<ReceiveCallback> pendingReceives_;
    const Commands::SubscriptionMode subscriptionMode_;
    optional<MessageId> startMessageId_;
    ConsumerInterceptorsPtr interceptors_;
    std::atomic_bool duringSeek_{false};

    /* methods */
    void handleSinglePartitionConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                              unsigned int partitionIndex);
    void notifyResult(const CloseCallback& closeCallback);
    void messageReceived(const Consumer& consumer, const Message& msg);
    void messageProcessed(Message& msg);
    void internalListener(const Consumer& consumer);
    void receiveMessages();
    void failPendingReceiveCallback();
    void notifyPendingReceivedCallback(Result result, const Message& message,
                                       const ReceiveCallback& callback);

    void handleOneTopicSubscribed(Result result, const Consumer& consumer, const std::string& topic,
                                  const std::shared_ptr<std::atomic<int>>& topicsNeedCreate);
    void subscribeTopicPartitions(int numPartitions, const TopicNamePtr& topicName,
                                  const std::string& consumerName,
                                  const ConsumerSubResultPromisePtr& topicSubResultPromise);
    void handleSingleConsumerCreated(Result result, const ConsumerImplBaseWeakPtr& consumerImplBaseWeakPtr,
                                     const std::shared_ptr<std::atomic<int>>& partitionsNeedCreate,
                                     const ConsumerSubResultPromisePtr& topicSubResultPromise);
    void handleOneTopicUnsubscribedAsync(Result result,
                                         const std::shared_ptr<std::atomic<int>>& consumerUnsubed,
                                         int numberPartitions, const TopicNamePtr& topicNamePtr,
                                         const std::string& topicPartitionName,
                                         const ResultCallback& callback);
    void runPartitionUpdateTask();
    void topicPartitionUpdate();
    void handleGetPartitions(const TopicNamePtr& topicName, Result result,
                             const LookupDataResultPtr& lookupDataResult, int currentNumPartitions);
    void subscribeSingleNewConsumer(int numPartitions, const TopicNamePtr& topicName, int partitionIndex,
                                    const ConsumerSubResultPromisePtr& topicSubResultPromise,
                                    const std::shared_ptr<std::atomic<int>>& partitionsNeedCreate);
    // impl consumer base virtual method
    bool hasEnoughMessagesForBatchReceive() const override;
    void notifyBatchPendingReceivedCallback(const BatchReceiveCallback& callback) override;
    void beforeConnectionChange(ClientConnection& cnx) override;
    friend class PulsarFriend;

   private:
    std::shared_ptr<MultiTopicsConsumerImpl> get_shared_this_ptr();
    void setNegativeAcknowledgeEnabledForTesting(bool enabled) override;
    void cancelTimers() noexcept;

    std::weak_ptr<MultiTopicsConsumerImpl> weak_from_this() noexcept {
        return std::static_pointer_cast<MultiTopicsConsumerImpl>(shared_from_this());
    }

    template <typename SeekArg>
#if __cplusplus >= 202002L
        requires std::convertible_to<SeekArg, uint64_t> ||
        std::same_as<std::remove_cv_t<std::remove_reference_t<SeekArg>>, MessageId>
#endif
        void seekAllAsync(const SeekArg& seekArg, const ResultCallback& callback);

    void beforeSeek();
    void afterSeek();

    FRIEND_TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testAcknowledgeCumulativeWithPartition);
    FRIEND_TEST(ConsumerTest, testPatternSubscribeTopic);
    FRIEND_TEST(ConsumerTest, testMultiConsumerListenerAndAck);
};

typedef std::shared_ptr<MultiTopicsConsumerImpl> MultiTopicsConsumerImplPtr;

template <typename SeekArg>
#if __cplusplus >= 202002L
    requires std::convertible_to<SeekArg, uint64_t> ||
    std::same_as<std::remove_cv_t<std::remove_reference_t<SeekArg>>, MessageId>
#endif
    inline void MultiTopicsConsumerImpl::seekAllAsync(const SeekArg& seekArg,
                                                      const ResultCallback& callback) {
    if (state_ != Ready) {
        callback(ResultAlreadyClosed);
        return;
    }
    beforeSeek();
    auto weakSelf = weak_from_this();
    auto failed = std::make_shared<std::atomic_bool>(false);
    consumers_.forEachValue(
        [this, weakSelf, &seekArg, callback, failed](const ConsumerImplPtr& consumer,
                                                     const SharedFuture& future) {
            consumer->seekAsync(seekArg, [this, weakSelf, callback, failed, future](Result result) {
                auto self = weakSelf.lock();
                if (!self || failed->load(std::memory_order_acquire)) {
                    callback(result);
                    return;
                }
                if (result != ResultOk) {
                    failed->store(true, std::memory_order_release);  // skip the following callbacks
                    afterSeek();
                    callback(result);
                    return;
                }
                if (future.tryComplete()) {
                    afterSeek();
                    callback(ResultOk);
                }
            });
        },
        [callback] { callback(ResultOk); });
}

}  // namespace pulsar
#endif  // PULSAR_MULTI_TOPICS_CONSUMER_HEADER

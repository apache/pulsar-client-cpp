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

#include "BlockingQueue.h"
#include "Commands.h"
#include "ConsumerImplBase.h"
#include "ConsumerInterceptors.h"
#include "Future.h"
#include "Latch.h"
#include "LookupDataResult.h"
#include "SynchronizedHashMap.h"
#include "TestUtil.h"

namespace pulsar {
typedef std::shared_ptr<Promise<Result, Consumer>> ConsumerSubResultPromisePtr;

class ConsumerImpl;
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
    MultiTopicsConsumerImpl(ClientImplPtr client, TopicNamePtr topicName, int numPartitions,
                            const std::string& subscriptionName, const ConsumerConfiguration& conf,
                            LookupServicePtr lookupServicePtr, const ConsumerInterceptorsPtr& interceptors,
                            Commands::SubscriptionMode = Commands::SubscriptionModeDurable,
                            boost::optional<MessageId> startMessageId = boost::none);

    MultiTopicsConsumerImpl(ClientImplPtr client, const std::vector<std::string>& topics,
                            const std::string& subscriptionName, TopicNamePtr topicName,
                            const ConsumerConfiguration& conf, LookupServicePtr lookupServicePtr_,
                            const ConsumerInterceptorsPtr& interceptors,
                            Commands::SubscriptionMode = Commands::SubscriptionModeDurable,
                            boost::optional<MessageId> startMessageId = boost::none);

    ~MultiTopicsConsumerImpl();
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

    void handleGetConsumerStats(Result, BrokerConsumerStats, LatchPtr, MultiTopicsBrokerConsumerStatsPtr,
                                size_t, BrokerConsumerStatsCallback);
    // return first topic name when all topics name valid, or return null pointer
    static std::shared_ptr<TopicName> topicNamesValid(const std::vector<std::string>& topics);
    void unsubscribeOneTopicAsync(const std::string& topic, ResultCallback callback);
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
    BlockingQueue<Message> incomingMessages_;
    std::atomic_int incomingMessagesSize_ = {0};
    MessageListener messageListener_;
    DeadlineTimerPtr partitionsUpdateTimer_;
    boost::posix_time::time_duration partitionsUpdateInterval_;
    LookupServicePtr lookupServicePtr_;
    std::shared_ptr<std::atomic<int>> numberTopicPartitions_;
    std::atomic<Result> failedResult{ResultOk};
    Promise<Result, ConsumerImplBaseWeakPtr> multiTopicsConsumerCreatedPromise_;
    UnAckedMessageTrackerPtr unAckedMessageTrackerPtr_;
    const std::vector<std::string> topics_;
    std::queue<ReceiveCallback> pendingReceives_;
    const Commands::SubscriptionMode subscriptionMode_;
    boost::optional<MessageId> startMessageId_;
    ConsumerInterceptorsPtr interceptors_;

    /* methods */
    void handleSinglePartitionConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                              unsigned int partitionIndex);
    void notifyResult(CloseCallback closeCallback);
    void messageReceived(Consumer consumer, const Message& msg);
    void messageProcessed(Message& msg);
    void internalListener(Consumer consumer);
    void receiveMessages();
    void failPendingReceiveCallback();
    void notifyPendingReceivedCallback(Result result, const Message& message,
                                       const ReceiveCallback& callback);

    void handleOneTopicSubscribed(Result result, Consumer consumer, const std::string& topic,
                                  std::shared_ptr<std::atomic<int>> topicsNeedCreate);
    void subscribeTopicPartitions(int numPartitions, TopicNamePtr topicName, const std::string& consumerName,
                                  ConsumerSubResultPromisePtr topicSubResultPromise);
    void handleSingleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                     std::shared_ptr<std::atomic<int>> partitionsNeedCreate,
                                     ConsumerSubResultPromisePtr topicSubResultPromise);
    void handleUnsubscribedAsync(Result result, std::shared_ptr<std::atomic<int>> consumerUnsubed,
                                 ResultCallback callback);
    void handleOneTopicUnsubscribedAsync(Result result, std::shared_ptr<std::atomic<int>> consumerUnsubed,
                                         int numberPartitions, TopicNamePtr topicNamePtr,
                                         std::string& topicPartitionName, ResultCallback callback);
    void runPartitionUpdateTask();
    void topicPartitionUpdate();
    void handleGetPartitions(TopicNamePtr topicName, Result result,
                             const LookupDataResultPtr& lookupDataResult, int currentNumPartitions);
    void subscribeSingleNewConsumer(int numPartitions, TopicNamePtr topicName, int partitionIndex,
                                    ConsumerSubResultPromisePtr topicSubResultPromise,
                                    std::shared_ptr<std::atomic<int>> partitionsNeedCreate);
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

    FRIEND_TEST(ConsumerTest, testMultiTopicsConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testPartitionedConsumerUnAckedMessageRedelivery);
    FRIEND_TEST(ConsumerTest, testAcknowledgeCumulativeWithPartition);
    FRIEND_TEST(ConsumerTest, testPatternSubscribeTopic);
};

typedef std::shared_ptr<MultiTopicsConsumerImpl> MultiTopicsConsumerImplPtr;
}  // namespace pulsar
#endif  // PULSAR_MULTI_TOPICS_CONSUMER_HEADER

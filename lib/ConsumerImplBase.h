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
#ifndef PULSAR_CONSUMER_IMPL_BASE_HEADER
#define PULSAR_CONSUMER_IMPL_BASE_HEADER
#include <pulsar/Consumer.h>
#include <pulsar/Message.h>

#include <queue>
#include <set>

#include "Future.h"
#include "GetLastMessageIdResponse.h"
#include "HandlerBase.h"

namespace pulsar {
typedef std::function<void(Result result, bool hasMessageAvailable)> HasMessageAvailableCallback;
class ConsumerImplBase;
using ConsumerImplBaseWeakPtr = std::weak_ptr<ConsumerImplBase>;
class OpBatchReceive {
   public:
    OpBatchReceive();
    explicit OpBatchReceive(const BatchReceiveCallback& batchReceiveCallback);
    BatchReceiveCallback batchReceiveCallback_;
    const int64_t createAt_;
};

class ConsumerImplBase : public HandlerBase {
   public:
    virtual ~ConsumerImplBase(){};
    ConsumerImplBase(const ClientImplPtr& client, const std::string& topic, Backoff backoff,
                     const ConsumerConfiguration& conf, const ExecutorServicePtr& listenerExecutor);
    std::shared_ptr<ConsumerImplBase> shared_from_this() noexcept {
        return std::dynamic_pointer_cast<ConsumerImplBase>(HandlerBase::shared_from_this());
    }

    // interface by consumer
    virtual Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture() = 0;
    virtual const std::string& getTopic() const = 0;
    virtual const std::string& getSubscriptionName() const = 0;
    virtual Result receive(Message& msg) = 0;
    virtual Result receive(Message& msg, int timeout) = 0;
    virtual void receiveAsync(const ReceiveCallback& callback) = 0;
    void batchReceiveAsync(const BatchReceiveCallback& callback);
    virtual void unsubscribeAsync(const ResultCallback& callback) = 0;
    virtual void acknowledgeAsync(const MessageId& msgId, const ResultCallback& callback) = 0;
    virtual void acknowledgeAsync(const MessageIdList& messageIdList, const ResultCallback& callback) = 0;
    virtual void acknowledgeCumulativeAsync(const MessageId& msgId, const ResultCallback& callback) = 0;
    virtual void closeAsync(const ResultCallback& callback) = 0;
    virtual void start() = 0;
    virtual void shutdown() = 0;
    virtual bool isClosed() = 0;
    virtual bool isOpen() = 0;
    virtual Result pauseMessageListener() = 0;
    virtual Result resumeMessageListener() = 0;
    virtual void redeliverUnacknowledgedMessages() = 0;
    virtual void redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) = 0;
    virtual int getNumOfPrefetchedMessages() const = 0;
    virtual void getBrokerConsumerStatsAsync(const BrokerConsumerStatsCallback& callback) = 0;
    virtual void getLastMessageIdAsync(const BrokerGetLastMessageIdCallback& callback) = 0;
    virtual void seekAsync(const MessageId& msgId, const ResultCallback& callback) = 0;
    virtual void seekAsync(uint64_t timestamp, const ResultCallback& callback) = 0;
    virtual void negativeAcknowledge(const MessageId& msgId) = 0;
    virtual bool isConnected() const = 0;
    virtual uint64_t getNumberOfConnectedConsumer() = 0;
    // overrided methods from HandlerBase
    virtual const std::string& getName() const override = 0;
    virtual void hasMessageAvailableAsync(const HasMessageAvailableCallback& callback) = 0;

    const std::string& getConsumerName() const noexcept { return consumerName_; }

   protected:
    // overrided methods from HandlerBase
    Future<Result, bool> connectionOpened(const ClientConnectionPtr& cnx) override {
        // Do not use bool, only Result.
        Promise<Result, bool> promise;
        promise.setSuccess();
        return promise.getFuture();
    }
    void connectionFailed(Result result) override {}

    // consumer impl generic method.
    ExecutorServicePtr listenerExecutor_;
    std::queue<OpBatchReceive> batchPendingReceives_;
    BatchReceivePolicy batchReceivePolicy_;
    DeadlineTimerPtr batchReceiveTimer_;
    std::mutex batchReceiveOptionMutex_;
    void triggerBatchReceiveTimerTask(long timeoutMs);
    void doBatchReceiveTimeTask();
    void failPendingBatchReceiveCallback();
    void notifyBatchPendingReceivedCallback();
    virtual void notifyBatchPendingReceivedCallback(const BatchReceiveCallback& callback) = 0;
    virtual bool hasEnoughMessagesForBatchReceive() const = 0;

   private:
    const std::string consumerName_;

    virtual void setNegativeAcknowledgeEnabledForTesting(bool enabled) = 0;

    // Note: it should be protected by batchPendingReceiveMutex_ and called when `batchPendingReceives_` is
    // not empty
    BatchReceiveCallback popBatchReceiveCallback() {
        auto callback = std::move(batchPendingReceives_.front().batchReceiveCallback_);
        batchPendingReceives_.pop();
        return callback;
    }

    friend class MultiTopicsConsumerImpl;
    friend class PulsarFriend;
};
}  // namespace pulsar
#endif  // PULSAR_CONSUMER_IMPL_BASE_HEADER

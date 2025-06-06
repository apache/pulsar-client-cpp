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

#include "ReaderImpl.h"

#include "ClientImpl.h"
#include "ConsumerImpl.h"
#include "ExecutorService.h"
#include "GetLastMessageIdResponse.h"
#include "MultiTopicsConsumerImpl.h"
#include "TopicName.h"

namespace pulsar {

namespace test {
std::mutex readerConfigTestMutex;
std::atomic_bool readerConfigTestEnabled{false};
ConsumerConfiguration consumerConfigOfReader;
}  // namespace test

static ResultCallback emptyCallback;

ReaderImpl::ReaderImpl(const ClientImplPtr& client, const std::string& topic, int partitions,
                       const ReaderConfiguration& conf, const ExecutorServicePtr& listenerExecutor,
                       const ReaderCallback& readerCreatedCallback)
    : topic_(topic),
      partitions_(partitions),
      client_(client),
      readerConf_(conf),
      readerCreatedCallback_(readerCreatedCallback) {}

void ReaderImpl::start(const MessageId& startMessageId,
                       const std::function<void(const ConsumerImplBaseWeakPtr&)>& callback) {
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerExclusive);
    consumerConf.setReceiverQueueSize(readerConf_.getReceiverQueueSize());
    consumerConf.setReadCompacted(readerConf_.isReadCompacted());
    consumerConf.setSchema(readerConf_.getSchema());
    consumerConf.setUnAckedMessagesTimeoutMs(readerConf_.getUnAckedMessagesTimeoutMs());
    consumerConf.setTickDurationInMs(readerConf_.getTickDurationInMs());
    consumerConf.setAckGroupingTimeMs(readerConf_.getAckGroupingTimeMs());
    consumerConf.setAckGroupingMaxSize(readerConf_.getAckGroupingMaxSize());
    consumerConf.setCryptoKeyReader(readerConf_.getCryptoKeyReader());
    consumerConf.setCryptoFailureAction(readerConf_.getCryptoFailureAction());
    consumerConf.setProperties(readerConf_.getProperties());
    consumerConf.setStartMessageIdInclusive(readerConf_.isStartMessageIdInclusive());

    if (readerConf_.getReaderName().length() > 0) {
        consumerConf.setConsumerName(readerConf_.getReaderName());
    }

    if (readerConf_.hasReaderListener()) {
        // Adapt the message listener to be a reader-listener
        readerListener_ = readerConf_.getReaderListener();
        consumerConf.setMessageListener(std::bind(&ReaderImpl::messageListener, shared_from_this(),
                                                  std::placeholders::_1, std::placeholders::_2));
    }

    std::string subscription;
    if (!readerConf_.getInternalSubscriptionName().empty()) {
        subscription = readerConf_.getInternalSubscriptionName();
    } else {
        subscription = "reader-" + generateRandomName();
        if (!readerConf_.getSubscriptionRolePrefix().empty()) {
            subscription = readerConf_.getSubscriptionRolePrefix() + "-" + subscription;
        }
    }

    // get the consumer's configuration before created
    if (test::readerConfigTestEnabled) {
        test::consumerConfigOfReader = consumerConf.clone();
    }

    if (partitions_ > 0) {
        auto consumerImpl = std::make_shared<MultiTopicsConsumerImpl>(
            client_.lock(), TopicName::get(topic_), partitions_, subscription, consumerConf,
            client_.lock()->getLookup(),
            std::make_shared<ConsumerInterceptors>(std::vector<ConsumerInterceptorPtr>()),
            Commands::SubscriptionModeNonDurable, startMessageId);
        consumer_ = consumerImpl;
    } else {
        auto consumerImpl = std::make_shared<ConsumerImpl>(
            client_.lock(), topic_, subscription, consumerConf, TopicName::get(topic_)->isPersistent(),
            std::make_shared<ConsumerInterceptors>(std::vector<ConsumerInterceptorPtr>()),
            ExecutorServicePtr(), false, NonPartitioned, Commands::SubscriptionModeNonDurable,
            startMessageId);
        consumerImpl->setPartitionIndex(TopicName::getPartitionIndex(topic_));
        consumer_ = consumerImpl;
    }
    auto self = shared_from_this();
    consumer_->getConsumerCreatedFuture().addListener(
        [this, self, callback](Result result, const ConsumerImplBaseWeakPtr& weakConsumerPtr) {
            if (result == ResultOk) {
                callback(weakConsumerPtr);
                readerCreatedCallback_(result, Reader(self));
            } else {
                readerCreatedCallback_(result, {});
            }
        });
    consumer_->start();
}

const std::string& ReaderImpl::getTopic() const { return consumer_->getTopic(); }

Result ReaderImpl::readNext(Message& msg) {
    Result res = consumer_->receive(msg);
    acknowledgeIfNecessary(res, msg);
    return res;
}

Result ReaderImpl::readNext(Message& msg, int timeoutMs) {
    Result res = consumer_->receive(msg, timeoutMs);
    acknowledgeIfNecessary(res, msg);
    return res;
}

void ReaderImpl::readNextAsync(const ReceiveCallback& callback) {
    auto self = shared_from_this();
    consumer_->receiveAsync([self, callback](Result result, const Message& message) {
        self->acknowledgeIfNecessary(result, message);
        callback(result, message);
    });
}

void ReaderImpl::messageListener(const Consumer& consumer, const Message& msg) {
    readerListener_(Reader(shared_from_this()), msg);
    acknowledgeIfNecessary(ResultOk, msg);
}

void ReaderImpl::acknowledgeIfNecessary(Result result, const Message& msg) {
    if (result != ResultOk) {
        return;
    }

    // Only acknowledge on the first message in the batch
    if (msg.getMessageId().batchIndex() <= 0) {
        // Acknowledge message immediately because the reader is based on non-durable
        // subscription. When it reconnects, it will specify the subscription position anyway
        consumer_->acknowledgeCumulativeAsync(msg.getMessageId(), emptyCallback);
    }
}

void ReaderImpl::closeAsync(const ResultCallback& callback) { consumer_->closeAsync(callback); }

void ReaderImpl::hasMessageAvailableAsync(const HasMessageAvailableCallback& callback) {
    consumer_->hasMessageAvailableAsync(callback);
}

void ReaderImpl::seekAsync(const MessageId& msgId, const ResultCallback& callback) {
    consumer_->seekAsync(msgId, callback);
}
void ReaderImpl::seekAsync(uint64_t timestamp, const ResultCallback& callback) {
    consumer_->seekAsync(timestamp, callback);
}

void ReaderImpl::getLastMessageIdAsync(const GetLastMessageIdCallback& callback) {
    consumer_->getLastMessageIdAsync([callback](Result result, const GetLastMessageIdResponse& response) {
        callback(result, response.getLastMessageId());
    });
}

bool ReaderImpl::isConnected() const { return consumer_->isConnected(); }

}  // namespace pulsar

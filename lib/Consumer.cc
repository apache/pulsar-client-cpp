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
#include <pulsar/BrokerConsumerStats.h>
#include <pulsar/Consumer.h>
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/MessageBuilder.h>

#include "ConsumerImpl.h"
#include "GetLastMessageIdResponse.h"
#include "Utils.h"

namespace pulsar {

static const std::string EMPTY_STRING;

Consumer::Consumer() : impl_() {}

Consumer::Consumer(ConsumerImplBasePtr impl) : impl_(std::move(impl)) {}

const std::string& Consumer::getTopic() const { return impl_ != NULL ? impl_->getTopic() : EMPTY_STRING; }

const std::string& Consumer::getSubscriptionName() const {
    return impl_ != NULL ? impl_->getSubscriptionName() : EMPTY_STRING;
}

const std::string& Consumer::getConsumerName() const {
    return impl_ ? impl_->getConsumerName() : EMPTY_STRING;
}

Result Consumer::unsubscribe() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<bool, Result> promise;
    impl_->unsubscribeAsync(WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::unsubscribeAsync(const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->unsubscribeAsync(callback);
}

Result Consumer::receive(Message& msg) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->receive(msg);
}

Result Consumer::receive(Message& msg, int timeoutMs) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->receive(msg, timeoutMs);
}

void Consumer::receiveAsync(const ReceiveCallback& callback) {
    if (!impl_) {
        Message msg;
        callback(ResultConsumerNotInitialized, msg);
        return;
    }
    impl_->receiveAsync(callback);
}

Result Consumer::batchReceive(Messages& msgs) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<Result, Messages> promise;
    impl_->batchReceiveAsync(WaitForCallbackValue<Messages>(promise));
    return promise.getFuture().get(msgs);
}

void Consumer::batchReceiveAsync(const BatchReceiveCallback& callback) {
    if (!impl_) {
        Messages msgs;
        callback(ResultConsumerNotInitialized, msgs);
        return;
    }
    impl_->batchReceiveAsync(callback);
}

Result Consumer::acknowledge(const Message& message) { return acknowledge(message.getMessageId()); }

Result Consumer::acknowledge(const MessageId& messageId) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<bool, Result> promise;
    impl_->acknowledgeAsync(messageId, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

Result Consumer::acknowledge(const MessageIdList& messageIdList) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<bool, Result> promise;
    impl_->acknowledgeAsync(messageIdList, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::acknowledgeAsync(const Message& message, const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeAsync(message.getMessageId(), callback);
}

void Consumer::acknowledgeAsync(const MessageId& messageId, const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeAsync(messageId, callback);
}

void Consumer::acknowledgeAsync(const MessageIdList& messageIdList, const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeAsync(messageIdList, callback);
}

Result Consumer::acknowledgeCumulative(const Message& message) {
    return acknowledgeCumulative(message.getMessageId());
}

Result Consumer::acknowledgeCumulative(const MessageId& messageId) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    Promise<bool, Result> promise;
    impl_->acknowledgeCumulativeAsync(messageId, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::acknowledgeCumulativeAsync(const Message& message, const ResultCallback& callback) {
    acknowledgeCumulativeAsync(message.getMessageId(), callback);
}

void Consumer::acknowledgeCumulativeAsync(const MessageId& messageId, const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->acknowledgeCumulativeAsync(messageId, callback);
}

void Consumer::negativeAcknowledge(const Message& message) { negativeAcknowledge(message.getMessageId()); }

void Consumer::negativeAcknowledge(const MessageId& messageId) {
    if (impl_) {
        impl_->negativeAcknowledge(messageId);
        ;
    }
}

Result Consumer::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Consumer::closeAsync(const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->closeAsync(callback);
}

Result Consumer::pauseMessageListener() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->pauseMessageListener();
}

Result Consumer::resumeMessageListener() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    return impl_->resumeMessageListener();
}

void Consumer::redeliverUnacknowledgedMessages() {
    if (impl_) {
        impl_->redeliverUnacknowledgedMessages();
    }
}

Result Consumer::getBrokerConsumerStats(BrokerConsumerStats& brokerConsumerStats) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<Result, BrokerConsumerStats> promise;
    getBrokerConsumerStatsAsync(WaitForCallbackValue<BrokerConsumerStats>(promise));
    return promise.getFuture().get(brokerConsumerStats);
}

void Consumer::getBrokerConsumerStatsAsync(const BrokerConsumerStatsCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }
    impl_->getBrokerConsumerStatsAsync(callback);
}

void Consumer::seekAsync(const MessageId& msgId, const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }
    impl_->seekAsync(msgId, callback);
}

void Consumer::seekAsync(uint64_t timestamp, const ResultCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }
    impl_->seekAsync(timestamp, callback);
}

Result Consumer::seek(const MessageId& msgId) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    Promise<bool, Result> promise;
    impl_->seekAsync(msgId, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

Result Consumer::seek(uint64_t timestamp) {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }

    Promise<bool, Result> promise;
    impl_->seekAsync(timestamp, WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

bool Consumer::isConnected() const { return impl_ && impl_->isConnected(); }

void Consumer::getLastMessageIdAsync(const GetLastMessageIdCallback& callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized, MessageId());
        return;
    }

    impl_->getLastMessageIdAsync([callback](Result result, const GetLastMessageIdResponse& response) {
        callback(result, response.getLastMessageId());
    });
}

Result Consumer::getLastMessageId(MessageId& messageId) {
    Promise<Result, MessageId> promise;

    getLastMessageIdAsync(WaitForCallbackValue<MessageId>(promise));
    return promise.getFuture().get(messageId);
}

}  // namespace pulsar

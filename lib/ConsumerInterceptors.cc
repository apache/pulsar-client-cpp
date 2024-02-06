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

#include "ConsumerInterceptors.h"

#include <pulsar/Consumer.h>

#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

Message ConsumerInterceptors::beforeConsume(const Consumer &consumer, const Message &message) const {
    Message interceptorMessage = message;
    for (const ConsumerInterceptorPtr &interceptor : interceptors_) {
        try {
            interceptorMessage = interceptor->beforeConsume(consumer, interceptorMessage);
        } catch (const std::exception &e) {
            LOG_WARN("Error executing interceptor beforeConsume callback for topic: "
                     << consumer.getTopic() << ", exception: " << e.what());
        }
    }
    return interceptorMessage;
}

void ConsumerInterceptors::onAcknowledge(const Consumer &consumer, Result result,
                                         const MessageId &messageID) const {
    for (const ConsumerInterceptorPtr &interceptor : interceptors_) {
        try {
            interceptor->onAcknowledge(consumer, result, messageID);
        } catch (const std::exception &e) {
            LOG_WARN("Error executing interceptor onAcknowledge callback for topic: "
                     << consumer.getTopic() << ", exception: " << e.what());
        }
    }
}

void ConsumerInterceptors::onAcknowledgeCumulative(const Consumer &consumer, Result result,
                                                   const MessageId &messageID) const {
    for (const ConsumerInterceptorPtr &interceptor : interceptors_) {
        try {
            interceptor->onAcknowledgeCumulative(consumer, result, messageID);
        } catch (const std::exception &e) {
            LOG_WARN("Error executing interceptor onAcknowledge callback for topic: "
                     << consumer.getTopic() << ", exception: " << e.what());
        }
    }
}

void ConsumerInterceptors::onNegativeAcksSend(const Consumer &consumer,
                                              const std::set<MessageId> &messageIds) const {
    for (const ConsumerInterceptorPtr &interceptor : interceptors_) {
        try {
            interceptor->onNegativeAcksSend(consumer, messageIds);
        } catch (const std::exception &e) {
            LOG_WARN("Error executing interceptor onNegativeAcksSend callback for topic: "
                     << consumer.getTopic() << ", exception: " << e.what());
        }
    }
}

void ConsumerInterceptors::close() {
    State state = Ready;
    if (!state_.compare_exchange_strong(state, Closing)) {
        return;
    }
    for (const ConsumerInterceptorPtr &interceptor : interceptors_) {
        try {
            interceptor->close();
        } catch (const std::exception &e) {
            LOG_WARN("Failed to close consumer interceptor: " << e.what());
        }
    }
    state_ = Closed;
}

}  // namespace pulsar

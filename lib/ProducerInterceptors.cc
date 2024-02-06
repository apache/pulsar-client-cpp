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

#include "ProducerInterceptors.h"

#include <pulsar/Producer.h>

#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {
void ProducerInterceptors::onPartitionsChange(const std::string& topicName, int partitions) const {
    for (const ProducerInterceptorPtr& interceptor : interceptors_) {
        try {
            interceptor->onPartitionsChange(topicName, partitions);
        } catch (const std::exception& e) {
            LOG_WARN("Error executing interceptor onPartitionsChange callback for topicName: "
                     << topicName << ", exception: " << e.what());
        }
    }
}

Message ProducerInterceptors::beforeSend(const Producer& producer, const Message& message) {
    if (interceptors_.empty()) {
        return message;
    }

    Message interceptorMessage = message;
    for (const ProducerInterceptorPtr& interceptor : interceptors_) {
        try {
            interceptorMessage = interceptor->beforeSend(producer, interceptorMessage);
        } catch (const std::exception& e) {
            LOG_WARN("Error executing interceptor beforeSend callback for topicName: "
                     << producer.getTopic() << ", exception: " << e.what());
        }
    }
    return interceptorMessage;
}

void ProducerInterceptors::onSendAcknowledgement(const Producer& producer, Result result,
                                                 const Message& message, const MessageId& messageID) {
    for (const ProducerInterceptorPtr& interceptor : interceptors_) {
        try {
            interceptor->onSendAcknowledgement(producer, result, message, messageID);
        } catch (const std::exception& e) {
            LOG_WARN("Error executing interceptor onSendAcknowledgement callback for topicName: "
                     << producer.getTopic() << ", exception: " << e.what());
        }
    }
}

void ProducerInterceptors::close() {
    State state = Ready;
    if (!state_.compare_exchange_strong(state, Closing)) {
        return;
    }
    for (const ProducerInterceptorPtr& interceptor : interceptors_) {
        try {
            interceptor->close();
        } catch (const std::exception& e) {
            LOG_WARN("Failed to close producer interceptor: " << e.what());
        }
    }
    state_ = Closed;
}

}  // namespace pulsar

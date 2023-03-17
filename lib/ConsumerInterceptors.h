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

#pragma once

#include <pulsar/ConsumerInterceptor.h>

#include <atomic>
#include <set>
#include <utility>
#include <vector>

namespace pulsar {
class ConsumerInterceptors {
   public:
    explicit ConsumerInterceptors(std::vector<ConsumerInterceptorPtr> interceptors)
        : interceptors_(std::move(interceptors)) {}

    void close();

    Message beforeConsume(const Consumer& consumer, const Message& message) const;

    void onAcknowledge(const Consumer& consumer, Result result, const MessageId& messageID) const;

    void onAcknowledgeCumulative(const Consumer& consumer, Result result, const MessageId& messageID) const;

    void onNegativeAcksSend(const Consumer& consumer, const std::set<MessageId>& messageIds) const;

   private:
    enum State
    {
        Ready,
        Closing,
        Closed
    };
    std::vector<ConsumerInterceptorPtr> interceptors_;
    std::atomic<State> state_{Ready};
};

typedef std::shared_ptr<ConsumerInterceptors> ConsumerInterceptorsPtr;
}  // namespace pulsar

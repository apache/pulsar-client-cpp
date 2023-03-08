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

#ifndef PULSAR_CPP_CONSUMERINTERCEPPTOR_H
#define PULSAR_CPP_CONSUMERINTERCEPPTOR_H

#include <pulsar/Message.h>
#include <pulsar/Result.h>
#include <pulsar/defines.h>

namespace pulsar {

class Consumer;

class PULSAR_PUBLIC ConsumerInterceptor {
   public:
    virtual ~ConsumerInterceptor() {}
    /**
     * Close the interceptor
     */
    virtual void close() {}

    virtual Message beforeConsume(const Consumer& consumer, const Message& message) = 0;

    virtual void onAcknowledge(const Consumer& consumer, Result result, const MessageId& messageID) = 0;

    virtual void onAcknowledgeCumulative(const Consumer& consumer, Result result,
                                         const MessageId& messageID) = 0;
};

typedef std::shared_ptr<ConsumerInterceptor> ConsumerInterceptorPtr;
}  // namespace pulsar

#endif  // PULSAR_CPP_CONSUMERINTERCEPPTOR_H

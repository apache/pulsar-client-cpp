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
#ifndef DEAD_LETTER_POLICY_BUILD_HPP_
#define DEAD_LETTER_POLICY_BUILD_HPP_

#include <pulsar/DeadLetterPolicy.h>
#include <pulsar/defines.h>

#include <memory>

namespace pulsar {

struct DeadLetterPolicyImpl;

/**
 * The builder to build a DeadLetterPolicyBuilder
 *
 * Example of building DeadLetterPolicy:
 *
 * ```c++
 * DeadLetterPolicy dlqPolicy = DeadLetterPolicyBuilder()
 *                       .deadLetterTopic("dlq-topic")
 *                       .maxRedeliverCount(10)
 *                       .initialSubscriptionName("init-sub-name")
 *                       .build();
 * ```
 */
class PULSAR_PUBLIC DeadLetterPolicyBuilder {
   public:
    DeadLetterPolicyBuilder();

    /**
     * Set dead letter topic.
     *
     * @param deadLetterTopic Name of the dead topic where the failing messages are sent.
     * The default value is: sourceTopicName + "-" + subscriptionName + "-DLQ"
     *
     * @return
     */
    DeadLetterPolicyBuilder& deadLetterTopic(const std::string& deadLetterTopic);

    /**
     * Set max redeliver count
     *
     * @param maxRedeliverCount Maximum number of times that a message is redelivered before being sent
     * to the dead letter queue.
     * - The maxRedeliverCount must be greater than 0.
     * - The default value is {INT_MAX} (DLQ is not enabled)
     *
     * @return
     */
    DeadLetterPolicyBuilder& maxRedeliverCount(int maxRedeliverCount);

    /**
     * Set initial subscription name
     *
     * @param initialSubscriptionName  Name of the initial subscription name of the dead letter topic.
     * If this field is not set, the initial subscription for the dead letter topic is not created.
     * If this field is set but the broker's `allowAutoSubscriptionCreation` is disabled, the DLQ producer
     * fails to be created.
     *
     * @return
     */
    DeadLetterPolicyBuilder& initialSubscriptionName(const std::string& initialSubscriptionName);

    /**
     * Build DeadLetterPolicy.
     *
     * @return
     */
    DeadLetterPolicy build();

   private:
    std::shared_ptr<DeadLetterPolicyImpl> impl_;
};
}  // namespace pulsar

#endif /* DEAD_LETTER_POLICY_BUILD_HPP_ */

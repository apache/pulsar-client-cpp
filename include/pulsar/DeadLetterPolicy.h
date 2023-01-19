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
#ifndef DEAD_LETTER_POLICY_HPP_
#define DEAD_LETTER_POLICY_HPP_

#include <pulsar/defines.h>

#include <memory>
#include <string>

namespace pulsar {

struct DeadLetterPolicyImpl;

/**
 * Configuration for the "dead letter queue" feature in consumer.
 *
 * see @DeadLetterPolicyBuilder
 */
class PULSAR_PUBLIC DeadLetterPolicy {
   public:
    DeadLetterPolicy();

    /**
     * Get dead letter topic
     *
     * @return
     */
    const std::string& getDeadLetterTopic() const;

    /**
     * Get max redeliver count
     *
     * @return
     */
    int getMaxRedeliverCount() const;

    /**
     * Get initial subscription name
     *
     * @return
     */
    const std::string& getInitialSubscriptionName() const;

   private:
    friend class DeadLetterPolicyBuilder;

    typedef std::shared_ptr<DeadLetterPolicyImpl> DeadLetterPolicyImplPtr;
    DeadLetterPolicyImplPtr impl_;

    explicit DeadLetterPolicy(const DeadLetterPolicyImplPtr& impl);
};
}  // namespace pulsar

#endif /* DEAD_LETTER_POLICY_HPP_ */

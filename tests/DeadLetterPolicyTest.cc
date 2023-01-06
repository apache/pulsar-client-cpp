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
#include <gtest/gtest.h>
#include <pulsar/DeadLetterPolicyBuilder.h>

#include <climits>

using namespace pulsar;

TEST(DeadLetterPolicy, testDeadLetterPolicy) {
    // test default value.
    DeadLetterPolicy deadLetterPolicy;
    ASSERT_EQ(deadLetterPolicy.getMaxRedeliverCount(), INT_MAX);
    ASSERT_TRUE(deadLetterPolicy.getDeadLetterTopic().empty());
    ASSERT_TRUE(deadLetterPolicy.getInitialSubscriptionName().empty());

    // test don't allowed max redeliver count less than 0.
    ASSERT_THROW(DeadLetterPolicyBuilder().maxRedeliverCount(-1).build(), std::invalid_argument);

    // test create DeadLetterPolicy by builder.
    deadLetterPolicy = DeadLetterPolicyBuilder()
                           .maxRedeliverCount(10)
                           .deadLetterTopic("topic-subscription-DLQ")
                           .initialSubscriptionName("init-DLQ-subscription")
                           .build();
    ASSERT_EQ(deadLetterPolicy.getMaxRedeliverCount(), 10);
    ASSERT_EQ(deadLetterPolicy.getDeadLetterTopic(), "topic-subscription-DLQ");
    ASSERT_EQ(deadLetterPolicy.getInitialSubscriptionName(), "init-DLQ-subscription");
}

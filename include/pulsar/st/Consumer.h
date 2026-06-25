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

#include <pulsar/st/detail/Cxx20.h>

#include <chrono>
#include <optional>
#include <string>

/**
 * @file
 * Enumerations and policy types shared across the consumer modes (spec §7).
 */

namespace pulsar::st {

/**
 * Where a brand-new subscription starts reading.
 *
 * Determines the initial position of the cursor the first time a subscription is
 * created. It is ignored once the subscription exists and has a durable cursor:
 * an already-established subscription always resumes from its stored position.
 */
enum class SubscriptionInitialPosition
{
    Earliest,  ///< Start from the oldest available message on the topic.
    Latest     ///< Start from the newest message, skipping anything published before subscribing.
};

/**
 * Acknowledgment tuning (mirrors the Java v5 config package).
 *
 * Controls how the consumer batches acknowledgments and, for a QueueConsumer, how
 * long redelivery is delayed after a negative acknowledgment. Both fields are
 * optional and fall back to the client default when unset.
 */
struct AckPolicy {
    /** Time window over which acknowledgments are batched before being sent, in milliseconds; 0 acks
     * immediately. Unset uses the client default. */
    std::optional<std::chrono::milliseconds> groupTime = std::nullopt;
    /** Delay before a negatively-acknowledged message is redelivered, in milliseconds. QueueConsumer only.
     * Unset uses the client default. */
    std::optional<std::chrono::milliseconds> negativeAckRedeliveryDelay = std::nullopt;
};

/**
 * Dead-letter handling for a QueueConsumer (spec §7.2).
 *
 * When a message is redelivered more than #maxRedeliverCount times, it is moved
 * to a dead-letter topic instead of being redelivered indefinitely. Dead-lettering
 * applies to a QueueConsumer only and is disabled unless #maxRedeliverCount is set
 * to a positive value.
 */
struct DeadLetterPolicy {
    /** Maximum number of redeliveries before a message is routed to the dead-letter topic. Defaults to 0,
     * which disables dead-lettering. */
    int maxRedeliverCount = 0;
    /** Name of the dead-letter topic. Unset defaults to "&lt;topic&gt;-&lt;subscription&gt;-DLQ". */
    std::optional<std::string> deadLetterTopic = std::nullopt;
    /** If set, creates this subscription on the dead-letter topic up front so no messages are missed before a
     * consumer attaches. Unset creates no initial subscription. */
    std::optional<std::string> initialSubscriptionName = std::nullopt;
};

}  // namespace pulsar::st

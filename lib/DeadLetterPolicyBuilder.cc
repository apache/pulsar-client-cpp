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

#include <pulsar/DeadLetterPolicy.h>
#include <pulsar/DeadLetterPolicyBuilder.h>

#include "DeadLetterPolicyImpl.h"
#include "stdexcept"

using namespace pulsar;

namespace pulsar {

DeadLetterPolicyBuilder::DeadLetterPolicyBuilder() : impl_(std::make_shared<DeadLetterPolicyImpl>()) {}

DeadLetterPolicyBuilder& DeadLetterPolicyBuilder::deadLetterTopic(const std::string& deadLetterTopic) {
    impl_->deadLetterTopic = deadLetterTopic;
    return *this;
}

DeadLetterPolicyBuilder& DeadLetterPolicyBuilder::maxRedeliverCount(int maxRedeliverCount) {
    impl_->maxRedeliverCount = maxRedeliverCount;
    return *this;
}

DeadLetterPolicyBuilder& DeadLetterPolicyBuilder::initialSubscriptionName(
    const std::string& initialSubscriptionName) {
    impl_->initialSubscriptionName = initialSubscriptionName;
    return *this;
}

DeadLetterPolicy DeadLetterPolicyBuilder::build() {
    if (impl_->maxRedeliverCount <= 0) {
        throw std::invalid_argument("maxRedeliverCount must be > 0.");
    }
    return DeadLetterPolicy(impl_);
}

}  // namespace pulsar

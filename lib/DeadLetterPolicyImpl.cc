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

#include "DeadLetterPolicyImpl.h"

#include <pulsar/DeadLetterPolicy.h>

using namespace pulsar;

namespace pulsar {

DeadLetterPolicy::DeadLetterPolicy() : impl_(std::make_shared<DeadLetterPolicyImpl>()) {}

const std::string& DeadLetterPolicy::getDeadLetterTopic() const { return impl_->deadLetterTopic; }

int DeadLetterPolicy::getMaxRedeliverCount() const { return impl_->maxRedeliverCount; }

const std::string& DeadLetterPolicy::getInitialSubscriptionName() const {
    return impl_->initialSubscriptionName;
}

DeadLetterPolicy::DeadLetterPolicy(const DeadLetterPolicy::DeadLetterPolicyImplPtr& impl) : impl_(impl) {}

}  // namespace pulsar

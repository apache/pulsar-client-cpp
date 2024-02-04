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

#include <assert.h>
#include <pulsar/Result.h>

#include <unordered_set>

namespace pulsar {

inline bool isResultRetryable(Result result) {
    assert(result != ResultOk);
    if (result == ResultRetryable || result == ResultDisconnected) {
        return true;
    }

    static const std::unordered_set<Result> fatalResults{ResultConnectError,
                                                         ResultTimeout,
                                                         ResultAuthenticationError,
                                                         ResultAuthorizationError,
                                                         ResultInvalidUrl,
                                                         ResultInvalidConfiguration,
                                                         ResultIncompatibleSchema,
                                                         ResultTopicNotFound,
                                                         ResultOperationNotSupported,
                                                         ResultNotAllowedError,
                                                         ResultChecksumError,
                                                         ResultCryptoError,
                                                         ResultConsumerAssignError,
                                                         ResultProducerBusy,
                                                         ResultConsumerBusy,
                                                         ResultLookupError,
                                                         ResultTooManyLookupRequestException,
                                                         ResultProducerBlockedQuotaExceededException,
                                                         ResultProducerBlockedQuotaExceededError};
    return fatalResults.find(result) == fatalResults.cend();
}

}  // namespace pulsar

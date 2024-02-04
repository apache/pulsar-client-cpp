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
// This file is used to mock ClientConnection's methods
#pragma once

#include <pulsar/Result.h>

#include "ProtoApiEnums.h"
#include "PulsarApi.pb.h"

namespace pulsar {

namespace adaptor {

template <typename Connection>
inline void checkServerError(Connection& connection, ServerError error, const std::string& message) {
    switch (error) {
        case proto::ServerError::ServiceNotReady:
            // There are some typical error messages that should not trigger the
            // close() of the connection.
            //   "Namespace bundle ... is being unloaded"
            //   "KeeperException$..."
            //   "Failed to acquire ownership for namespace bundle ..."
            //   "the broker do not have test listener"
            // Before https://github.com/apache/pulsar/pull/21211, the error of the 1st and 2nd messages
            // is ServiceNotReady. Before https://github.com/apache/pulsar/pull/21993, the error of the 3rd
            // message is ServiceNotReady.
            if (message.find("Failed to acquire ownership") == std::string::npos &&
                message.find("KeeperException") == std::string::npos &&
                message.find("is being unloaded") == std::string::npos &&
                message.find("the broker do not have test listener") == std::string::npos) {
                connection.close(ResultDisconnected);
            }
            break;
        case proto::ServerError::TooManyRequests:
            // TODO: Implement maxNumberOfRejectedRequestPerConnection like
            // https://github.com/apache/pulsar/pull/274
            connection.close(ResultDisconnected);
            break;
        default:
            break;
    }
}

}  // namespace adaptor

}  // namespace pulsar

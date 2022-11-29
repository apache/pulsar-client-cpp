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
#ifndef LIB_CLIENTCONFIGURATIONIMPL_H_
#define LIB_CLIENTCONFIGURATIONIMPL_H_

#include <pulsar/ClientConfiguration.h>

namespace pulsar {

struct ClientConfigurationImpl {
    AuthenticationPtr authenticationPtr{AuthFactory::Disabled()};
    uint64_t memoryLimit{0ull};
    int32_t ioThreads{1};
    int32_t operationTimeoutSeconds{30};
    int32_t messageListenerThreads{1};
    int32_t concurrentLookupRequest{50000};
    int32_t maxLookupRedirects{20};
    int32_t initialBackoffIntervalMs{100};
    int32_t maxBackoffIntervalMs{60000};
    std::string logConfFilePath;
    bool useTls{false};
    std::string tlsPrivateKeyFilePath;
    std::string tlsCertificateFilePath;
    std::string tlsTrustCertsFilePath;
    bool tlsAllowInsecureConnection{false};
    uint32_t statsIntervalInSeconds{600};  // 10 minutes
    std::unique_ptr<LoggerFactory> loggerFactory;
    bool validateHostName{false};
    uint32_t partitionsUpdateInterval{60};  // 1 minute
    std::string listenerName;
    int32_t connectionTimeoutMs{10000};  // 10 seconds

    std::unique_ptr<LoggerFactory> takeLogger() { return std::move(loggerFactory); }
};
}  // namespace pulsar

#endif /* LIB_CLIENTCONFIGURATIONIMPL_H_ */

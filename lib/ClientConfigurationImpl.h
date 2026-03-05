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
#include <pulsar/ServiceInfo.h>

#include <chrono>
#include <shared_mutex>

#include "ServiceNameResolver.h"
#include "ServiceURI.h"

namespace pulsar {

// Use struct rather than class here just for ABI compatibility
struct ClientConfigurationImpl {
   private:
    mutable std::shared_mutex mutex;
    AuthenticationPtr authenticationPtr{AuthFactory::Disabled()};
    std::string tlsTrustCertsFilePath;
    bool useTls{false};

   public:
    void updateServiceInfo(const ServiceInfo& serviceInfo) {
        std::unique_lock lock(mutex);
        if (serviceInfo.authentication.has_value() && *serviceInfo.authentication) {
            authenticationPtr = *serviceInfo.authentication;
        } else {
            authenticationPtr = AuthFactory::Disabled();
        }
        if (serviceInfo.tlsTrustCertsFilePath.has_value()) {
            tlsTrustCertsFilePath = *serviceInfo.tlsTrustCertsFilePath;
        } else {
            tlsTrustCertsFilePath = "";
        }
        useTls = ServiceNameResolver::useTls(ServiceURI(serviceInfo.serviceUrl));
    }

    auto& getAuthentication() const {
        std::shared_lock lock(mutex);
        return authenticationPtr;
    }

    auto setAuthentication(const AuthenticationPtr& authentication) {
        std::unique_lock lock(mutex);
        authenticationPtr = authentication;
    }

    auto& getTlsTrustCertsFilePath() const {
        std::shared_lock lock(mutex);
        return tlsTrustCertsFilePath;
    }

    auto setTlsTrustCertsFilePath(const std::string& path) {
        std::unique_lock lock(mutex);
        tlsTrustCertsFilePath = path;
    }

    auto isUseTls() const {
        std::shared_lock lock(mutex);
        return useTls;
    }

    auto setUseTls(bool useTls_) {
        std::unique_lock lock(mutex);
        useTls = useTls_;
    }

    uint64_t memoryLimit{0ull};
    int ioThreads{1};
    int connectionsPerBroker{1};
    std::chrono::nanoseconds operationTimeout{30LL * 1000 * 1000 * 1000};
    int messageListenerThreads{1};
    int concurrentLookupRequest{50000};
    int maxLookupRedirects{20};
    int initialBackoffIntervalMs{100};
    int maxBackoffIntervalMs{60000};
    std::string tlsPrivateKeyFilePath;
    std::string tlsCertificateFilePath;
    bool tlsAllowInsecureConnection{false};
    unsigned int statsIntervalInSeconds{600};  // 10 minutes
    std::unique_ptr<LoggerFactory> loggerFactory;
    bool validateHostName{false};
    unsigned int partitionsUpdateInterval{60};  // 1 minute
    std::string listenerName;
    int connectionTimeoutMs{10000};  // 10 seconds
    unsigned int keepAliveIntervalInSeconds{30};
    std::string description;
    std::string proxyServiceUrl;
    ClientConfiguration::ProxyProtocol proxyProtocol;

    std::unique_ptr<LoggerFactory> takeLogger() { return std::move(loggerFactory); }
};
}  // namespace pulsar

#endif /* LIB_CLIENTCONFIGURATIONIMPL_H_ */

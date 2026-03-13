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
#ifndef PULSAR_SERVICE_INFO_H_
#define PULSAR_SERVICE_INFO_H_

#include <pulsar/Authentication.h>

#include <optional>
#include <string>

namespace pulsar {

/**
 * ServiceInfo encapsulates the information of a Pulsar service, which is used by the client to connect to the
 * service. It includes the service URL, authentication information, and TLS configuration.
 */
class PULSAR_PUBLIC ServiceInfo final {
   public:
    ServiceInfo() = default;  // only for storing in containers like std::vector, not for public use
    ServiceInfo(std::string serviceUrl, AuthenticationPtr authentication = AuthFactory::Disabled(),
                std::optional<std::string> tlsTrustCertsFilePath = std::nullopt);

    auto& serviceUrl() const noexcept { return serviceUrl_; }
    auto useTls() const noexcept { return useTls_; }
    auto& authentication() const noexcept { return authentication_; }
    auto& tlsTrustCertsFilePath() const noexcept { return tlsTrustCertsFilePath_; }

    bool operator==(const ServiceInfo& other) const noexcept {
        return serviceUrl_ == other.serviceUrl_ && useTls_ == other.useTls_ &&
               authentication_ == other.authentication_ &&
               tlsTrustCertsFilePath_ == other.tlsTrustCertsFilePath_;
    }

   private:
    std::string serviceUrl_;
    bool useTls_;
    AuthenticationPtr authentication_;
    std::optional<std::string> tlsTrustCertsFilePath_;
};

}  // namespace pulsar
#endif

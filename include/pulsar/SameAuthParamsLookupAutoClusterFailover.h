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
#ifndef PULSAR_SAME_AUTH_PARAMS_LOOKUP_AUTO_CLUSTER_FAILOVER_H_
#define PULSAR_SAME_AUTH_PARAMS_LOOKUP_AUTO_CLUSTER_FAILOVER_H_

#include <pulsar/ClientConfiguration.h>
#include <pulsar/ServiceInfoProvider.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace pulsar {

class SameAuthParamsLookupAutoClusterFailoverImpl;

class PULSAR_PUBLIC SameAuthParamsLookupAutoClusterFailover final : public ServiceInfoProvider {
   public:
    struct Config {
        std::vector<std::string> serviceUrls;
        ClientConfiguration clientConfiguration;
        uint32_t failoverThreshold{5};
        uint32_t recoverThreshold{5};
        std::chrono::milliseconds checkHealthyInterval{1000};
        bool markTopicNotFoundAsAvailable{true};
        std::string testTopic{"public/default/tp_test"};

        Config(std::vector<std::string> serviceUrls, ClientConfiguration clientConfiguration = {});
    };

    class Builder {
       public:
        explicit Builder(std::vector<std::string> serviceUrls, ClientConfiguration clientConfiguration = {});

        Builder& withFailoverThreshold(uint32_t threshold);
        Builder& withRecoverThreshold(uint32_t threshold);
        Builder& withCheckHealthyInterval(std::chrono::milliseconds interval);
        Builder& withMarkTopicNotFoundAsAvailable(bool enabled);
        Builder& withTestTopic(std::string testTopic);

        SameAuthParamsLookupAutoClusterFailover build();

       private:
        Config config_;
    };

    explicit SameAuthParamsLookupAutoClusterFailover(Config&& config);

    ~SameAuthParamsLookupAutoClusterFailover() final;

    ServiceInfo initialServiceInfo() final;

    void initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) final;

   private:
    friend class SameAuthParamsLookupAutoClusterFailoverImpl;

    static ServiceInfo toServiceInfo(const ClientConfiguration& clientConfiguration,
                                     const std::string& serviceUrl);

    std::shared_ptr<SameAuthParamsLookupAutoClusterFailoverImpl> impl_;
};

}  // namespace pulsar

#endif

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
#ifndef PULSAR_AUTO_CLUSTER_FAILOVER_H_
#define PULSAR_AUTO_CLUSTER_FAILOVER_H_

#include <pulsar/ServiceInfoProvider.h>

#include <chrono>
#include <cstdint>

namespace pulsar {

class Client;
class AutoClusterFailoverImpl;

class PULSAR_PUBLIC AutoClusterFailover final : public ServiceInfoProvider {
   public:
    struct Config {
        const ServiceInfo primary;
        const std::vector<ServiceInfo> secondary;
        std::chrono::milliseconds checkInterval{30000};  // 30 seconds
        uint32_t failoverThreshold{1};
        uint32_t switchBackThreshold{1};

        Config(ServiceInfo primary, std::vector<ServiceInfo> secondary)
            : primary(std::move(primary)), secondary(std::move(secondary)) {}
    };

    /**
     * Builder helps create an AutoClusterFailover configuration.
     *
     * Example:
     *   ServiceInfo primary{...};
     *   std::vector<ServiceInfo> secondaries{...};
     *   AutoClusterFailover provider = AutoClusterFailover::Builder(primary, secondaries)
     *       .withCheckInterval(std::chrono::seconds(30))
     *       .withFailoverThreshold(3)
     *       .withSwitchBackThreshold(3)
     *       .build();
     *
     * Notes:
     * - primary: the preferred cluster to use when available.
     * - secondary: ordered list of fallback clusters.
     * - checkInterval: frequency of health probes.
     * - failoverThreshold: the number of consecutive failed probes required before switching away from
     *   the current cluster.
     * - switchBackThreshold: the number of consecutive successful probes to the primary required before
     *   switching back from a secondary.
     */
    class Builder {
       public:
        Builder(ServiceInfo primary, std::vector<ServiceInfo> secondary)
            : config_(std::move(primary), std::move(secondary)) {}

        // Set how frequently probes run against the active cluster(s).
        Builder& withCheckInterval(std::chrono::milliseconds interval) {
            config_.checkInterval = interval;
            return *this;
        }

        // Set the number of consecutive failed probes required before attempting failover.
        Builder& withFailoverThreshold(uint32_t threshold) {
            config_.failoverThreshold = threshold;
            return *this;
        }

        // Set the number of consecutive successful primary probes required before switching back.
        Builder& withSwitchBackThreshold(uint32_t threshold) {
            config_.switchBackThreshold = threshold;
            return *this;
        }

        AutoClusterFailover build() { return AutoClusterFailover(std::move(config_)); }

       private:
        Config config_;
    };

    explicit AutoClusterFailover(Config&& config);

    ~AutoClusterFailover() final;

    ServiceInfo initialServiceInfo() final;

    void initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) final;

   private:
    std::shared_ptr<AutoClusterFailoverImpl> impl_;
};

}  // namespace pulsar

#endif

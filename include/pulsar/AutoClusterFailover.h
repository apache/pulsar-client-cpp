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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace pulsar {

class Client;
class AutoClusterFailoverImpl;

class PULSAR_PUBLIC AutoClusterFailover final : public ServiceInfoProvider {
   public:
    struct Config {
        const ServiceInfo primary;
        const std::vector<ServiceInfo> secondary;
        std::chrono::milliseconds checkInterval{30000};    // 30 seconds
        std::chrono::milliseconds failoverDelay{30000};    // 30 seconds
        std::chrono::milliseconds switchBackDelay{60000};  // 60 seconds
        uint32_t failoverThreshold{1};
        uint32_t switchBackThreshold{2};

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
     *       .withCheckInterval(std::chrono::seconds(5))
     *       .withFailoverDelay(std::chrono::seconds(30))
     *       .withSwitchBackDelay(std::chrono::seconds(60))
     *       .build();
     *
     * Notes:
     * - primary: the preferred cluster to use when available.
     * - secondary: ordered list of fallback clusters.
     * - checkInterval: frequency of health probes.
     * - failoverDelay: how long the current cluster must remain unavailable before switching away.
     * - switchBackDelay: how long the primary must remain healthy before switching back from a secondary.
     *   If the active secondary becomes unavailable and the primary is available, the implementation may
     *   switch back to the primary immediately, regardless of this delay.
     */
    class Builder {
       public:
        Builder(ServiceInfo primary, std::vector<ServiceInfo> secondary)
            : config_(std::move(primary), std::move(secondary)) {}

        // Set how frequently probes run against the active cluster(s). Default: 30 seconds.
        Builder& withCheckInterval(std::chrono::milliseconds interval) {
            config_.checkInterval = interval;
            updateThresholdsFromDelays();
            return *this;
        }

        // Set how long the current cluster must remain unavailable before switching away. Default: 30
        // seconds.
        Builder& withFailoverDelay(std::chrono::milliseconds delay) {
            failoverThresholdConfigured_ = false;
            config_.failoverDelay = delay;
            config_.failoverThreshold = calculateThreshold(config_.failoverDelay, config_.checkInterval);
            return *this;
        }

        // Set how long the primary must remain healthy before switching back from a secondary. Default: 60
        // seconds.
        Builder& withSwitchBackDelay(std::chrono::milliseconds delay) {
            switchBackThresholdConfigured_ = false;
            config_.switchBackDelay = delay;
            config_.switchBackThreshold = calculateThreshold(config_.switchBackDelay, config_.checkInterval);
            return *this;
        }

        // Set the number of consecutive failed probes required before attempting failover.
        Builder& withFailoverThreshold(uint32_t threshold) {
            failoverThresholdConfigured_ = true;
            config_.failoverThreshold = threshold;
            config_.failoverDelay = config_.checkInterval * threshold;
            return *this;
        }

        // Set the number of consecutive successful primary probes required before switching back from a
        // healthy secondary. If the active secondary becomes unavailable and the primary is available,
        // the implementation may switch back immediately regardless of this threshold.
        Builder& withSwitchBackThreshold(uint32_t threshold) {
            switchBackThresholdConfigured_ = true;
            config_.switchBackThreshold = threshold;
            config_.switchBackDelay = config_.checkInterval * threshold;
            return *this;
        }

        AutoClusterFailover build() { return AutoClusterFailover(std::move(config_)); }

       private:
        static uint32_t calculateThreshold(std::chrono::milliseconds delay,
                                           std::chrono::milliseconds interval) {
            if (delay <= std::chrono::milliseconds::zero() || interval <= std::chrono::milliseconds::zero()) {
                return 1;
            }
            return static_cast<uint32_t>(
                std::max<int64_t>(1, (delay.count() + interval.count() - 1) / interval.count()));
        }

        void updateThresholdsFromDelays() {
            if (failoverThresholdConfigured_) {
                config_.failoverDelay = config_.checkInterval * config_.failoverThreshold;
            } else {
                config_.failoverThreshold = calculateThreshold(config_.failoverDelay, config_.checkInterval);
            }
            if (switchBackThresholdConfigured_) {
                config_.switchBackDelay = config_.checkInterval * config_.switchBackThreshold;
            } else {
                config_.switchBackThreshold =
                    calculateThreshold(config_.switchBackDelay, config_.checkInterval);
            }
        }

        Config config_;
        bool failoverThresholdConfigured_{false};
        bool switchBackThresholdConfigured_{false};
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

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
#include <pulsar/SameAuthParamsLookupAutoClusterFailover.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "AsioDefines.h"
#include "AsioTimer.h"
#include "AtomicSharedPtr.h"
#include "BinaryProtoLookupService.h"
#include "ClientConfigurationImpl.h"
#include "ConnectionPool.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "ServiceURI.h"
#include "TopicName.h"

#ifdef USE_ASIO
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#else
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#endif

DECLARE_LOG_OBJECT()

namespace pulsar {

namespace {

bool isBlank(const std::string& value) {
    return std::all_of(value.begin(), value.end(), [](unsigned char ch) { return std::isspace(ch) != 0; });
}

bool startsWithHttp(const std::string& value) {
    return value.size() >= 4 && (value.compare(0, 4, "http") == 0 || value.compare(0, 4, "HTTP") == 0);
}

}  // namespace

class SameAuthParamsLookupAutoClusterFailoverImpl
    : public std::enable_shared_from_this<SameAuthParamsLookupAutoClusterFailoverImpl> {
   public:
    enum class PulsarServiceState
    {
        Healthy,
        PreFail,
        Failed,
        PreRecover
    };

    explicit SameAuthParamsLookupAutoClusterFailoverImpl(
        SameAuthParamsLookupAutoClusterFailover::Config&& config)
        : config_(std::move(config)),
          states_(config_.serviceUrls.size(), PulsarServiceState::Healthy),
          counters_(config_.serviceUrls.size(), 0) {
        for (const auto& serviceUrl : config_.serviceUrls) {
            serviceInfos_.emplace_back(SameAuthParamsLookupAutoClusterFailover::toServiceInfo(
                config_.clientConfiguration, serviceUrl));
        }
    }

    ~SameAuthParamsLookupAutoClusterFailoverImpl() {
        if (!thread_.joinable()) {
            return;
        }

        cancelTimer(*timer_);
        workGuard_.reset();
        ioContext_.stop();
        thread_.join();
    }

    ServiceInfo initialServiceInfo() const { return serviceInfos_.front(); }

    void initialize(std::function<void(ServiceInfo)>&& onServiceInfoUpdate) {
        if (thread_.joinable()) {
            throw std::logic_error("ServiceInfoProvider has already been initialized");
        }
        onServiceInfoUpdate_ = std::move(onServiceInfoUpdate);
        workGuard_.emplace(ASIO::make_work_guard(ioContext_));
        timer_.emplace(ioContext_);

        auto weakSelf = weak_from_this();
        ASIO::post(ioContext_, [weakSelf] {
            if (auto self = weakSelf.lock()) {
                self->scheduleCheck();
            }
        });

        thread_ = std::thread([this] { ioContext_.run(); });
    }

   private:
    SameAuthParamsLookupAutoClusterFailover::Config config_;
    std::vector<ServiceInfo> serviceInfos_;
    std::vector<PulsarServiceState> states_;
    std::vector<uint32_t> counters_;
    size_t currentServiceIndex_{0};

    ASIO::io_context ioContext_;
    std::optional<ASIO::executor_work_guard<ASIO::io_context::executor_type>> workGuard_;
    std::optional<ASIO::steady_timer> timer_;
    std::thread thread_;
    std::function<void(ServiceInfo)> onServiceInfoUpdate_;

    void scheduleCheck() {
        timer_->expires_after(config_.checkHealthyInterval);
        auto weakSelf = weak_from_this();
        timer_->async_wait([weakSelf](ASIO_ERROR error) {
            if (error) {
                return;
            }
            if (auto self = weakSelf.lock()) {
                self->executeCheck();
                self->scheduleCheck();
            }
        });
    }

    int firstHealthyPulsarService() const {
        for (size_t i = 0; i <= currentServiceIndex_; i++) {
            if (states_[i] == PulsarServiceState::Healthy || states_[i] == PulsarServiceState::PreFail) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    int findFailoverTo() {
        for (size_t i = currentServiceIndex_ + 1; i < serviceInfos_.size(); i++) {
            if (probeAvailable(i)) {
                return static_cast<int>(i);
            }

            states_[i] = PulsarServiceState::Failed;
            counters_[i] = 0;
        }
        return -1;
    }

    void checkPulsarServices() {
        for (size_t i = 0; i <= currentServiceIndex_; i++) {
            if (probeAvailable(i)) {
                handleProbeSuccess(i);
            } else {
                handleProbeFailure(i);
            }
        }
    }

    void handleProbeSuccess(size_t index) {
        switch (states_[index]) {
            case PulsarServiceState::Healthy:
                break;
            case PulsarServiceState::PreFail:
                states_[index] = PulsarServiceState::Healthy;
                counters_[index] = 0;
                break;
            case PulsarServiceState::Failed:
                states_[index] = PulsarServiceState::PreRecover;
                counters_[index] = 1;
                break;
            case PulsarServiceState::PreRecover:
                if (++counters_[index] >= config_.recoverThreshold) {
                    states_[index] = PulsarServiceState::Healthy;
                    counters_[index] = 0;
                }
                break;
        }
    }

    void handleProbeFailure(size_t index) {
        switch (states_[index]) {
            case PulsarServiceState::Healthy:
                states_[index] = PulsarServiceState::PreFail;
                counters_[index] = 1;
                break;
            case PulsarServiceState::PreFail:
                if (++counters_[index] >= config_.failoverThreshold) {
                    states_[index] = PulsarServiceState::Failed;
                    counters_[index] = 0;
                }
                break;
            case PulsarServiceState::Failed:
                break;
            case PulsarServiceState::PreRecover:
                states_[index] = PulsarServiceState::Failed;
                counters_[index] = 0;
                break;
        }
    }

    bool probeAvailable(size_t index) {
        ClientConfiguration probeConfiguration = config_.clientConfiguration;
        probeConfiguration.setOperationTimeoutSeconds(3);
        AtomicSharedPtr<ServiceInfo> serviceInfo;
        serviceInfo.store(std::make_shared<const ServiceInfo>(serviceInfos_[index]));
        auto executorProvider = std::make_shared<ExecutorServiceProvider>(probeConfiguration.getIOThreads());
        ConnectionPool pool(serviceInfo, probeConfiguration, executorProvider, "Pulsar-CPP-lookup-failover");
        BinaryProtoLookupService lookupService(serviceInfos_[index], pool, probeConfiguration);

        auto topicName = TopicName::get(config_.testTopic);
        if (!topicName) {
            LOG_WARN("Invalid lookup probe topic: " << config_.testTopic);
            executorProvider->close();
            pool.close();
            return false;
        }

        LookupService::LookupResult lookupResult;
        const auto result = lookupService.getBroker(*topicName).get(lookupResult);
        pool.close();
        executorProvider->close();

        if (result == ResultOk) {
            LOG_DEBUG("Successfully probed service availability: " << serviceInfos_[index].serviceUrl());
            return true;
        }
        if (result == ResultTopicNotFound && config_.markTopicNotFoundAsAvailable) {
            LOG_DEBUG("Successfully probed service availability with topic not found: "
                      << serviceInfos_[index].serviceUrl());
            return true;
        }
        LOG_WARN("Failed to probe service availability: " << serviceInfos_[index].serviceUrl() << " - "
                                                          << result);
        return false;
    }

    void executeCheck() {
        checkPulsarServices();
        const int firstHealthy = firstHealthyPulsarService();
        if (firstHealthy == static_cast<int>(currentServiceIndex_)) {
            return;
        }
        if (firstHealthy < 0) {
            const int failoverTo = findFailoverTo();
            if (failoverTo >= 0) {
                updateServiceInfo(static_cast<size_t>(failoverTo));
            }
            return;
        }
        updateServiceInfo(static_cast<size_t>(firstHealthy));
    }

    void updateServiceInfo(size_t targetIndex) {
        LOG_INFO("Switch service URL from " << serviceInfos_[currentServiceIndex_].serviceUrl() << " to "
                                            << serviceInfos_[targetIndex].serviceUrl());
        if (targetIndex < currentServiceIndex_) {
            for (size_t i = targetIndex + 1; i < states_.size(); i++) {
                states_[i] = PulsarServiceState::Healthy;
                counters_[i] = 0;
            }
        }
        currentServiceIndex_ = targetIndex;
        onServiceInfoUpdate_(serviceInfos_[currentServiceIndex_]);
    }
};

SameAuthParamsLookupAutoClusterFailover::Config::Config(std::vector<std::string> serviceUrls,
                                                        ClientConfiguration clientConfiguration)
    : serviceUrls(std::move(serviceUrls)), clientConfiguration(std::move(clientConfiguration)) {
    if (this->serviceUrls.empty()) {
        throw std::invalid_argument("serviceUrls cannot be empty");
    }

    std::unordered_set<std::string> uniqueUrls;
    for (const auto& serviceUrl : this->serviceUrls) {
        if (isBlank(serviceUrl)) {
            throw std::invalid_argument("serviceUrls contains a blank value");
        }
        if (startsWithHttp(serviceUrl)) {
            throw std::invalid_argument(
                "SameAuthParamsLookupAutoClusterFailover does not support HTTP service URLs");
        }
        if (!uniqueUrls.insert(serviceUrl).second) {
            throw std::invalid_argument("serviceUrls contains duplicated value " + serviceUrl);
        }
        ServiceURI serviceUri(serviceUrl);
    }
}

SameAuthParamsLookupAutoClusterFailover::Builder::Builder(std::vector<std::string> serviceUrls,
                                                          ClientConfiguration clientConfiguration)
    : config_(std::move(serviceUrls), std::move(clientConfiguration)) {}

SameAuthParamsLookupAutoClusterFailover::Builder&
SameAuthParamsLookupAutoClusterFailover::Builder::withFailoverThreshold(uint32_t threshold) {
    if (threshold < 1) {
        throw std::invalid_argument("failoverThreshold must be larger than 0");
    }
    config_.failoverThreshold = threshold;
    return *this;
}

SameAuthParamsLookupAutoClusterFailover::Builder&
SameAuthParamsLookupAutoClusterFailover::Builder::withRecoverThreshold(uint32_t threshold) {
    if (threshold < 1) {
        throw std::invalid_argument("recoverThreshold must be larger than 0");
    }
    config_.recoverThreshold = threshold;
    return *this;
}

SameAuthParamsLookupAutoClusterFailover::Builder&
SameAuthParamsLookupAutoClusterFailover::Builder::withCheckHealthyInterval(
    std::chrono::milliseconds interval) {
    if (interval < std::chrono::milliseconds{1}) {
        throw std::invalid_argument("checkHealthyInterval must be larger than 0");
    }
    config_.checkHealthyInterval = interval;
    return *this;
}

SameAuthParamsLookupAutoClusterFailover::Builder&
SameAuthParamsLookupAutoClusterFailover::Builder::withMarkTopicNotFoundAsAvailable(bool enabled) {
    config_.markTopicNotFoundAsAvailable = enabled;
    return *this;
}

SameAuthParamsLookupAutoClusterFailover::Builder&
SameAuthParamsLookupAutoClusterFailover::Builder::withTestTopic(std::string testTopic) {
    if (isBlank(testTopic) || !TopicName::get(testTopic)) {
        throw std::invalid_argument("testTopic cannot be blank or invalid");
    }
    config_.testTopic = std::move(testTopic);
    return *this;
}

SameAuthParamsLookupAutoClusterFailover SameAuthParamsLookupAutoClusterFailover::Builder::build() {
    return SameAuthParamsLookupAutoClusterFailover(std::move(config_));
}

SameAuthParamsLookupAutoClusterFailover::SameAuthParamsLookupAutoClusterFailover(Config&& config)
    : impl_(std::make_shared<SameAuthParamsLookupAutoClusterFailoverImpl>(std::move(config))) {}

SameAuthParamsLookupAutoClusterFailover::~SameAuthParamsLookupAutoClusterFailover() {}

ServiceInfo SameAuthParamsLookupAutoClusterFailover::initialServiceInfo() {
    return impl_->initialServiceInfo();
}

void SameAuthParamsLookupAutoClusterFailover::initialize(
    std::function<void(ServiceInfo)> onServiceInfoUpdate) {
    impl_->initialize(std::move(onServiceInfoUpdate));
}

ServiceInfo SameAuthParamsLookupAutoClusterFailover::toServiceInfo(
    const ClientConfiguration& clientConfiguration, const std::string& serviceUrl) {
    return clientConfiguration.impl_->toServiceInfo(serviceUrl);
}

}  // namespace pulsar

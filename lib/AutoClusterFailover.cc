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
#include <pulsar/AutoClusterFailover.h>
#include <pulsar/Client.h>

#include <atomic>
#include <memory>

#include "ClientImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

class AutoClusterFailoverImpl : public std::enable_shared_from_this<AutoClusterFailoverImpl> {
   public:
    explicit AutoClusterFailoverImpl(AutoClusterFailover::Config&& config) : config_(std::move(config)) {}

    ~AutoClusterFailoverImpl() { running_->store(false, std::memory_order_release); }

    void initialize(const ClientImplPtr& client) {
        client_ = client;
        executor_ = client->getIOExecutorProvider()->get();
        scheduleProbeAndUpdateServiceUrl();
    }

    void scheduleProbeAndUpdateServiceUrl() {
        auto timer = executor_->createDeadlineTimer();
        timer->expires_after(config_.checkInterval);
        timer->async_wait([this, weakSelf{weak_from_this()}, timer](auto error) {
            auto self = weakSelf.lock();
            if (!self) {
                LOG_INFO("AutoClusterFailoverImpl has been destroyed, exiting timer callback");
                return;
            }
            auto closed = !running_->load(std::memory_order_acquire);
            if (!error || closed) {
                LOG_INFO("AutoClusterFailover exited, timer error: " << error.message()
                                                                     << ", closed: " << closed);
                return;
            }

            probeAndUpdateServiceUrl();
        });
    }

    void probeAndUpdateServiceUrl() {
        auto currentServiceInfo = client_->getServiceInfo();
        if (currentServiceInfo == config_.primary) {
            // TODO: probe whether primary is down
        } else {
            // TODO:
            //   1. probe whether current (one secondary) is down
            //   2. if not, check whether primary is up and switch back if it is
        }
        scheduleProbeAndUpdateServiceUrl();
    }

   private:
    const AutoClusterFailover::Config config_;
    ClientImplPtr client_;
    ExecutorServicePtr executor_;
    std::shared_ptr<std::atomic_bool> running_{std::make_shared<std::atomic_bool>(true)};
};

AutoClusterFailover::AutoClusterFailover(AutoClusterFailover::Config&& config)
    : impl_(std::make_shared<AutoClusterFailoverImpl>(std::move(config))) {}

AutoClusterFailover::~AutoClusterFailover() {}

void AutoClusterFailover::initialize(Client& client) { impl_->initialize(client.impl_); }

}  // namespace pulsar

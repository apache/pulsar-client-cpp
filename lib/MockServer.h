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

#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ClientConnection.h"
#include "ConsumerImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "PulsarApi.pb.h"

namespace pulsar {

class MockServer : public std::enable_shared_from_this<MockServer> {
   public:
    using RequestDelayType = std::unordered_map<std::string, long /* delay in milliseconds */>;

    MockServer(const ClientConnectionPtr& connection) : connection_(connection) {
        requestDelays_["CLOSE_CONSUMER"] = 1;
    }

    void setRequestDelay(std::initializer_list<typename RequestDelayType::value_type> delays) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto&& delay : delays) {
            requestDelays_[delay.first] = delay.second;
        }
    }

    bool sendRequest(const std::string& request, uint64_t requestId) {
        auto connection = connection_.lock();
        if (!connection) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto iter = requestDelays_.find(request); iter != requestDelays_.end()) {
            // Mock the `CLOSE_CONSUMER` command sent by broker, for simplicity, disconnect all consumers
            if (request == "SEEK") {
                schedule(connection, "CLOSE_CONSUMER" + std::to_string(requestId),
                         requestDelays_["CLOSE_CONSUMER"], [connection] {
                             std::vector<uint64_t> consumerIds;
                             {
                                 std::lock_guard<std::mutex> lock{connection->mutex_};
                                 for (auto&& kv : connection->consumers_) {
                                     if (auto consumer = kv.second.lock()) {
                                         consumerIds.push_back(consumer->getConsumerId());
                                     }
                                 }
                             }
                             for (auto consumerId : consumerIds) {
                                 proto::CommandCloseConsumer closeConsumerCmd;
                                 closeConsumerCmd.set_consumer_id(consumerId);
                                 connection->handleCloseConsumer(closeConsumerCmd);
                             }
                         });
            }
            schedule(connection, request + std::to_string(requestId), iter->second, [connection, requestId] {
                proto::CommandSuccess success;
                success.set_request_id(requestId);
                connection->handleSuccess(success);
            });
            return true;
        } else {
            return false;
        }
    }

    // Return the number of pending timers cancelled
    auto close() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto result = pendingTimers_.size();
        for (auto&& kv : pendingTimers_) {
            try {
                LOG_INFO("Cancelling timer for " << kv.first);
                kv.second->cancel();
            } catch (...) {
                LOG_WARN("Failed to cancel timer for " << kv.first);
            }
        }
        pendingTimers_.clear();
        return result;
    }

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, long> requestDelays_;
    std::unordered_map<std::string, DeadlineTimerPtr> pendingTimers_;
    ClientConnectionWeakPtr connection_;

    void schedule(ClientConnectionPtr& connection, const std::string& key, long delayMs,
                  std::function<void()>&& task) {
        auto timer = connection->executor_->createDeadlineTimer();
        pendingTimers_[key] = timer;
        timer->expires_after(std::chrono::milliseconds(delayMs));
        LOG_INFO("Mock scheduling " << key << " with delay " << delayMs << " ms");
        auto self = shared_from_this();
        timer->async_wait([this, self, key, connection, task{std::move(task)}](const auto& ec) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                pendingTimers_.erase(key);
            }
            if (ec) {
                LOG_INFO("Timer cancelled for " << key);
                return;
            }
            if (connection->isClosed()) {
                LOG_INFO("Connection is closed, not completing request " << key);
                return;
            }
            LOG_INFO("Completing delayed request " << key);
            task();
        });
    }

    DECLARE_LOG_OBJECT()
};

}  // namespace pulsar

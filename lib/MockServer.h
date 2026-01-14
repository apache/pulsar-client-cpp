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
#include <mutex>
#include <unordered_map>
#include <vector>

#include "ClientConnection.h"
#include "ConsumerImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "PulsarApi.pb.h"

namespace pulsar {

class MockServer {
   public:
    using RequestDelayType = std::unordered_map<std::string, long /* delay in milliseconds */>;

    MockServer(const ClientConnectionPtr& connection) : connection_(connection) {}

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
                connection->executor_->postWork([connection] {
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
            long delayMs = iter->second;
            auto timer = connection->executor_->createDeadlineTimer();
            timer->expires_from_now(std::chrono::milliseconds(delayMs));
            timer->async_wait([connection, requestId, request, timer](const auto& ec) {
                if (ec) {
                    LOG_INFO("Timer cancelled for request " << request << " with id " << requestId);
                    return;
                }
                if (connection->isClosed()) {
                    LOG_INFO("Connection is closed, not completing request " << request << " with id "
                                                                             << requestId);
                    return;
                }
                LOG_INFO("Completing delayed request " << request << " with id " << requestId);
                proto::CommandSuccess success;
                success.set_request_id(requestId);
                connection->handleSuccess(success);
            });
            return true;
        } else {
            return false;
        }
    }

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, long> requestDelays_;
    ClientConnectionWeakPtr connection_;

    DECLARE_LOG_OBJECT()
};

}  // namespace pulsar

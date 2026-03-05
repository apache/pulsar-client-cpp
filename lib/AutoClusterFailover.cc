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
#include <chrono>
#include <memory>

#ifdef USE_ASIO
#include <asio/connect.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#else
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#endif

#include "AsioDefines.h"
#include "ClientImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "ServiceURI.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

// Probe whether a Pulsar service URL is reachable by attempting a TCP connection.
// Parses the first host:port from the service URL and tries to connect within timeoutMs.
static bool probeAvailable(const std::string& serviceUrl, int timeoutMs) {
    try {
        ServiceURI uri(serviceUrl);
        const auto& hosts = uri.getServiceHosts();
        if (hosts.empty()) {
            return false;
        }

        // Each entry in getServiceHosts() is a full URL like "pulsar://host:port".
        // Strip the scheme prefix to get "host:port".
        const auto& hostAddr = hosts[0];
        const auto schemeEnd = hostAddr.find("://");
        if (schemeEnd == std::string::npos) {
            return false;
        }
        const std::string hostPort = hostAddr.substr(schemeEnd + 3);
        const auto colonPos = hostPort.rfind(':');
        if (colonPos == std::string::npos) {
            return false;
        }
        const std::string host = hostPort.substr(0, colonPos);
        const std::string port = hostPort.substr(colonPos + 1);

        ASIO::io_context ioCtx;
        ASIO::ip::tcp::resolver resolver(ioCtx);
        ASIO_ERROR ec;
        const auto endpoints = resolver.resolve(host, port, ec);
        if (ec) {
            LOG_WARN("probeAvailable: failed to resolve " << host << ":" << port << " - " << ec.message());
            return false;
        }

        ASIO::ip::tcp::socket socket(ioCtx);
        bool connected = false;
        ASIO::steady_timer timer(ioCtx);
        timer.expires_after(std::chrono::milliseconds(timeoutMs));
        timer.async_wait([&](const ASIO_ERROR& timerEc) {
            if (!timerEc) {
                ASIO_ERROR closeEc;
                socket.close(closeEc);
            }
        });
        ASIO::async_connect(socket, endpoints,
                            [&](const ASIO_ERROR& connectEc, const ASIO::ip::tcp::endpoint&) {
                                if (!connectEc) {
                                    connected = true;
                                }
                                timer.cancel();
                            });
        ioCtx.run();

        if (connected) {
            ASIO_ERROR closeEc;
            socket.close(closeEc);
        }
        return connected;
    } catch (const std::exception& e) {
        LOG_WARN("probeAvailable: exception probing " << serviceUrl << ": " << e.what());
        return false;
    }
}

static int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

static constexpr int kProbeTimeoutMs = 30000;

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
            if (error || closed) {
                LOG_INFO("AutoClusterFailover exited, timer error: " << error.message()
                                                                     << ", closed: " << closed);
                return;
            }

            probeAndUpdateServiceUrl();
        });
    }

    void probeAndUpdateServiceUrl() {
        auto currentServiceInfo = client_->getServiceInfo();
        const auto now = nowMs();

        if (currentServiceInfo == config_.primary) {
            // Currently on primary: probe it and fail over to first available secondary if it's down.
            if (probeAvailable(currentServiceInfo.serviceUrl, kProbeTimeoutMs)) {
                failedTimestamp_ = -1;
            } else {
                if (failedTimestamp_ < 0) {
                    failedTimestamp_ = now;
                } else if (now - failedTimestamp_ >= config_.failoverDelay.count()) {
                    for (const auto& secondary : config_.secondary) {
                        if (probeAvailable(secondary.serviceUrl, kProbeTimeoutMs)) {
                            LOG_INFO("Primary " << currentServiceInfo.serviceUrl << " has been down for "
                                                << (now - failedTimestamp_) << "ms, switching to secondary "
                                                << secondary.serviceUrl);
                            auto info = secondary;
                            client_->updateServiceInfo(std::move(info));
                            failedTimestamp_ = -1;
                            break;
                        } else {
                            LOG_WARN("Primary " << currentServiceInfo.serviceUrl << " has been down for "
                                                << (now - failedTimestamp_) << "ms. Secondary "
                                                << secondary.serviceUrl
                                                << " is also unavailable, trying next.");
                        }
                    }
                }
            }
        } else {
            // Currently on a secondary: probe it.
            if (probeAvailable(currentServiceInfo.serviceUrl, kProbeTimeoutMs)) {
                failedTimestamp_ = -1;
                // Secondary is up; check whether primary has recovered long enough to switch back.
                if (probeAvailable(config_.primary.serviceUrl, kProbeTimeoutMs)) {
                    if (recoverTimestamp_ < 0) {
                        recoverTimestamp_ = now;
                    } else if (now - recoverTimestamp_ >= config_.switchBackDelay.count()) {
                        LOG_INFO("Primary " << config_.primary.serviceUrl << " has been recovered for "
                                            << (now - recoverTimestamp_) << "ms, switching back from "
                                            << currentServiceInfo.serviceUrl);
                        auto info = config_.primary;
                        client_->updateServiceInfo(std::move(info));
                        recoverTimestamp_ = -1;
                    }
                } else {
                    recoverTimestamp_ = -1;
                }
            } else {
                // Current secondary is down; reset recovery clock and attempt to switch back to primary.
                recoverTimestamp_ = -1;
                if (failedTimestamp_ < 0) {
                    failedTimestamp_ = now;
                } else if (now - failedTimestamp_ >= config_.failoverDelay.count()) {
                    if (probeAvailable(config_.primary.serviceUrl, kProbeTimeoutMs)) {
                        LOG_INFO("Secondary " << currentServiceInfo.serviceUrl << " has been down for "
                                              << (now - failedTimestamp_) << "ms, switching back to primary "
                                              << config_.primary.serviceUrl);
                        auto info = config_.primary;
                        client_->updateServiceInfo(std::move(info));
                        failedTimestamp_ = -1;
                    } else {
                        LOG_ERROR("Secondary " << currentServiceInfo.serviceUrl << " has been down for "
                                               << (now - failedTimestamp_) << "ms and primary "
                                               << config_.primary.serviceUrl << " is also unavailable.");
                    }
                }
            }
        }

        scheduleProbeAndUpdateServiceUrl();
    }

   private:
    const AutoClusterFailover::Config config_;
    ClientImplPtr client_;
    ExecutorServicePtr executor_;
    std::shared_ptr<std::atomic_bool> running_{std::make_shared<std::atomic_bool>(true)};
    // Timestamp (ms) when the current service first became unreachable; -1 when it is reachable.
    int64_t failedTimestamp_{-1};
    // Timestamp (ms) when the primary first became reachable again while on a secondary; -1 otherwise.
    int64_t recoverTimestamp_{-1};
};

AutoClusterFailover::AutoClusterFailover(AutoClusterFailover::Config&& config)
    : impl_(std::make_shared<AutoClusterFailoverImpl>(std::move(config))) {}

AutoClusterFailover::~AutoClusterFailover() {}

void AutoClusterFailover::initialize(Client& client) { impl_->initialize(client.impl_); }

}  // namespace pulsar

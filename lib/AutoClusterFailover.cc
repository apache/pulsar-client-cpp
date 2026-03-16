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

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "AsioTimer.h"
#include "LogUtils.h"
#include "ServiceURI.h"
#include "Url.h"

#ifdef USE_ASIO
#include <asio/connect.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#else
#include <boost/asio/connect.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#endif

#include "AsioDefines.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

class AutoClusterFailoverImpl : public std::enable_shared_from_this<AutoClusterFailoverImpl> {
   public:
    AutoClusterFailoverImpl(AutoClusterFailover::Config&& config)
        : config_(std::move(config)), currentServiceInfo_(&config_.primary) {}

    ~AutoClusterFailoverImpl() {
        using namespace std::chrono_literals;
        if (!thread_.joinable() || !future_.valid()) {
            return;
        }

        cancelTimer(*timer_);
        workGuard_.reset();
        ioContext_.stop();

        if (future_.wait_for(1s) != std::future_status::ready) {
            LOG_WARN("AutoClusterFailoverImpl is not stopped within 3 seconds, skip it");
            thread_.detach();
        } else {
            thread_.join();
        }
        thread_.join();
    }

    auto primary() const noexcept { return config_.primary; }

    void initialize(std::function<void(ServiceInfo)>&& onServiceInfoUpdate) {
        onServiceInfoUpdate_ = std::move(onServiceInfoUpdate);
        workGuard_.emplace(ASIO::make_work_guard(ioContext_));
        timer_.emplace(ioContext_);

        auto weakSelf = weak_from_this();
        ASIO::post(ioContext_, [weakSelf] {
            if (auto self = weakSelf.lock()) {
                self->scheduleFailoverCheck();
            }
        });

        // Capturing `this` is safe because the thread will be joined in the destructor
        std::promise<void> promise;
        future_ = promise.get_future();
        thread_ = std::thread([this, promise{std::move(promise)}]() mutable {
            ioContext_.run();
            promise.set_value();
        });
    }

   private:
    static constexpr std::chrono::milliseconds probeTimeout_{30000};
    using CompletionCallback = std::function<void()>;
    using ProbeCallback = std::function<void(bool)>;

    struct ProbeContext {
        ASIO::ip::tcp::resolver resolver;
        ASIO::ip::tcp::socket socket;
        ASIO::steady_timer timer;
        ProbeCallback callback;
        bool done{false};
        std::string hostUrl;

        ProbeContext(ASIO::io_context& ioContext, std::string hostUrl, ProbeCallback callback)
            : resolver(ioContext),
              socket(ioContext),
              timer(ioContext),
              callback(std::move(callback)),
              hostUrl(std::move(hostUrl)) {}
    };

    AutoClusterFailover::Config config_;
    const ServiceInfo* currentServiceInfo_;
    std::optional<std::chrono::steady_clock::time_point> failedSince_;
    std::optional<std::chrono::steady_clock::time_point> recoveredSince_;

    std::thread thread_;
    std::future<void> future_;

    ASIO::io_context ioContext_;
    std::function<void(ServiceInfo)> onServiceInfoUpdate_;

    std::optional<ASIO::executor_work_guard<ASIO::io_context::executor_type>> workGuard_;
    std::optional<ASIO::steady_timer> timer_;

    bool isUsingPrimary() const noexcept { return currentServiceInfo_ == &config_.primary; }

    const ServiceInfo& current() const noexcept { return *currentServiceInfo_; }

    void scheduleFailoverCheck() {
        timer_->expires_after(config_.checkInterval);
        auto weakSelf = weak_from_this();
        timer_->async_wait([weakSelf](ASIO_ERROR error) {
            if (error) {
                return;
            }
            if (auto self = weakSelf.lock()) {
                self->executeFailoverCheck();
            }
        });
    }

    void executeFailoverCheck() {
        auto done = [weakSelf = weak_from_this()] {
            if (auto self = weakSelf.lock()) {
                self->scheduleFailoverCheck();
            }
        };

        if (isUsingPrimary()) {
            checkAndFailoverToSecondaryAsync(std::move(done));
        } else {
            checkSecondaryAndPrimaryAsync(std::move(done));
        }
    }

    static void completeProbe(const std::shared_ptr<ProbeContext>& context, bool success,
                              const ASIO_ERROR& error = ASIO_SUCCESS) {
        if (context->done) {
            return;
        }

        context->done = true;
        ASIO_ERROR ignored;
        context->resolver.cancel();
        context->socket.close(ignored);
        context->timer.cancel(ignored);

        context->callback(success);
    }

    void probeHostAsync(const std::string& hostUrl, ProbeCallback callback) {
        Url parsedUrl;
        if (!Url::parse(hostUrl, parsedUrl)) {
            LOG_WARN("Failed to parse service URL for probing: " << hostUrl);
            callback(false);
            return;
        }

        auto context = std::make_shared<ProbeContext>(ioContext_, hostUrl, std::move(callback));
        context->timer.expires_after(probeTimeout_);
        context->timer.async_wait([context](const ASIO_ERROR& error) {
            if (!error) {
                completeProbe(context, false, ASIO::error::timed_out);
            }
        });

        context->resolver.async_resolve(
            parsedUrl.host(), std::to_string(parsedUrl.port()),
            [context](const ASIO_ERROR& error, const ASIO::ip::tcp::resolver::results_type& endpoints) {
                if (error) {
                    completeProbe(context, false, error);
                    return;
                }

                ASIO::async_connect(
                    context->socket, endpoints,
                    [context](const ASIO_ERROR& connectError, const ASIO::ip::tcp::endpoint&) {
                        completeProbe(context, !connectError, connectError);
                    });
            });
    }

    void probeHostsAsync(const std::shared_ptr<std::vector<std::string>>& hosts, size_t index,
                         ProbeCallback callback) {
        if (index >= hosts->size()) {
            callback(false);
            return;
        }

        auto hostUrl = (*hosts)[index];
        auto weakSelf = weak_from_this();
        probeHostAsync(hostUrl, [weakSelf, hosts, index, callback = std::move(callback)](bool available) mutable {
            if (available) {
                callback(true);
                return;
            }
            if (auto self = weakSelf.lock()) {
                self->probeHostsAsync(hosts, index + 1, std::move(callback));
            }
        });
    }

    void probeAvailableAsync(const ServiceInfo& serviceInfo, ProbeCallback callback) {
        try {
            ServiceURI serviceUri{serviceInfo.serviceUrl()};
            auto hosts = std::make_shared<std::vector<std::string>>(serviceUri.getServiceHosts());
            if (hosts->empty()) {
                callback(false);
                return;
            }
            probeHostsAsync(hosts, 0, std::move(callback));
        } catch (const std::exception& e) {
            LOG_WARN("Failed to probe service URL " << serviceInfo.serviceUrl() << ": " << e.what());
            callback(false);
        }
    }

    void switchTo(const ServiceInfo* serviceInfo) {
        if (currentServiceInfo_ == serviceInfo) {
            return;
        }

        LOG_INFO("Switch service URL from " << current().serviceUrl() << " to " << serviceInfo->serviceUrl());
        currentServiceInfo_ = serviceInfo;
        failedSince_.reset();
        recoveredSince_.reset();
        onServiceInfoUpdate_(current());
    }

    void probeSecondaryFrom(size_t index, CompletionCallback done) {
        if (index >= config_.secondary.size()) {
            done();
            return;
        }

        auto weakSelf = weak_from_this();
        probeAvailableAsync(config_.secondary[index],
                            [weakSelf, index, done = std::move(done)](bool available) mutable {
                                auto self = weakSelf.lock();
                                if (!self) {
                                    return;
                                }

                                if (available) {
                                    self->switchTo(&self->config_.secondary[index]);
                                    done();
                                    return;
                                }

                                self->probeSecondaryFrom(index + 1, std::move(done));
                            });
    }

    void checkAndFailoverToSecondaryAsync(CompletionCallback done) {
        auto weakSelf = weak_from_this();
        probeAvailableAsync(current(), [weakSelf, done = std::move(done)](bool primaryAvailable) mutable {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }

            const auto now = std::chrono::steady_clock::now();
            if (primaryAvailable) {
                self->failedSince_.reset();
                done();
                return;
            }

            if (!self->failedSince_) {
                self->failedSince_ = now;
                done();
                return;
            }

            if (now - *self->failedSince_ < self->config_.failoverDelay) {
                done();
                return;
            }

            self->probeSecondaryFrom(0, std::move(done));
        });
    }

    void checkSwitchBackToPrimaryAsync(CompletionCallback done, std::optional<bool> primaryAvailableHint) {
        const auto now = std::chrono::steady_clock::now();
        auto handlePrimaryAvailable = [weakSelf = weak_from_this(), now,
                                       done = std::move(done)](bool primaryAvailable) mutable {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }

            if (!primaryAvailable) {
                self->recoveredSince_.reset();
                done();
                return;
            }

            if (!self->recoveredSince_) {
                self->recoveredSince_ = now;
                done();
                return;
            }

            if (now - *self->recoveredSince_ >= self->config_.switchBackDelay) {
                self->switchTo(&self->config_.primary);
            }
            done();
        };

        if (primaryAvailableHint.has_value()) {
            handlePrimaryAvailable(*primaryAvailableHint);
            return;
        }

        probeAvailableAsync(config_.primary, std::move(handlePrimaryAvailable));
    }

    void checkSecondaryAndPrimaryAsync(CompletionCallback done) {
        auto weakSelf = weak_from_this();
        probeAvailableAsync(current(), [weakSelf, done = std::move(done)](bool secondaryAvailable) mutable {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }

            const auto now = std::chrono::steady_clock::now();
            if (secondaryAvailable) {
                self->failedSince_.reset();
                self->checkSwitchBackToPrimaryAsync(std::move(done), std::nullopt);
                return;
            }

            if (!self->failedSince_) {
                self->failedSince_ = now;
                self->checkSwitchBackToPrimaryAsync(std::move(done), std::nullopt);
                return;
            }

            if (now - *self->failedSince_ < self->config_.failoverDelay) {
                self->checkSwitchBackToPrimaryAsync(std::move(done), std::nullopt);
                return;
            }

            self->probeAvailableAsync(self->config_.primary,
                                      [weakSelf, done = std::move(done)](bool primaryAvailable) mutable {
                                          auto self = weakSelf.lock();
                                          if (!self) {
                                              return;
                                          }

                                          if (primaryAvailable) {
                                              self->switchTo(&self->config_.primary);
                                              done();
                                              return;
                                          }

                                          self->checkSwitchBackToPrimaryAsync(std::move(done), false);
                                      });
        });
    }
};

AutoClusterFailover::AutoClusterFailover(Config&& config)
    : impl_(std::make_shared<AutoClusterFailoverImpl>(std::move(config))) {}

AutoClusterFailover::~AutoClusterFailover() {}

ServiceInfo AutoClusterFailover::initialServiceInfo() { return impl_->primary(); }

void AutoClusterFailover::initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) {
    impl_->initialize(std::move(onServiceInfoUpdate));
}

}  // namespace pulsar

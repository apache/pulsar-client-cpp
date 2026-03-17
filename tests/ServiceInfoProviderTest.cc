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
#include <gtest/gtest.h>
#include <pulsar/AutoClusterFailover.h>
#include <pulsar/Client.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "PulsarFriend.h"
#include "WaitUtils.h"
#include "lib/AsioDefines.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;
using namespace std::chrono_literals;

namespace {

class ProbeTcpServer {
   public:
    ProbeTcpServer() { start(); }

    ~ProbeTcpServer() { stop(); }

    void start() {
        if (running_) {
            return;
        }

        auto ioContext = std::unique_ptr<ASIO::io_context>(new ASIO::io_context);
        auto acceptor = std::unique_ptr<ASIO::ip::tcp::acceptor>(new ASIO::ip::tcp::acceptor(*ioContext));
        ASIO::ip::tcp::endpoint endpoint{ASIO::ip::tcp::v4(), static_cast<unsigned short>(port_)};
        acceptor->open(endpoint.protocol());
        acceptor->set_option(ASIO::ip::tcp::acceptor::reuse_address(true));
        acceptor->bind(endpoint);
        acceptor->listen();

        port_ = acceptor->local_endpoint().port();
        ioContext_ = std::move(ioContext);
        acceptor_ = std::move(acceptor);
        running_ = true;

        scheduleAccept();
        serverThread_ = std::thread([this] { ioContext_->run(); });
    }

    void stop() {
        if (!running_.exchange(false)) {
            return;
        }

        ASIO::post(*ioContext_, [this] {
            ASIO_ERROR ignored;
            if (acceptor_ && acceptor_->is_open()) {
                acceptor_->close(ignored);
            }
        });

        if (serverThread_.joinable()) {
            serverThread_.join();
        }

        acceptor_.reset();
        ioContext_.reset();
    }

    std::string getServiceUrl() const { return "pulsar://127.0.0.1:" + std::to_string(port_); }

   private:
    void scheduleAccept() {
        if (!running_ || !acceptor_ || !acceptor_->is_open()) {
            return;
        }

        auto socket = std::make_shared<ASIO::ip::tcp::socket>(*ioContext_);
        acceptor_->async_accept(*socket, [this, socket](const ASIO_ERROR &error) {
            if (!error) {
                ASIO_ERROR ignored;
                socket->close(ignored);
            }

            if (running_ && acceptor_ && acceptor_->is_open()) {
                scheduleAccept();
            }
        });
    }

    int port_{0};
    std::atomic_bool running_{false};
    std::unique_ptr<ASIO::io_context> ioContext_;
    std::unique_ptr<ASIO::ip::tcp::acceptor> acceptor_;
    std::thread serverThread_;
};

class ServiceUrlObserver {
   public:
    void onUpdate(const ServiceInfo &serviceInfo) {
        std::lock_guard<std::mutex> lock(mutex_);
        serviceUrls_.emplace_back(serviceInfo.serviceUrl());
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return serviceUrls_.size();
    }

    std::string last() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return serviceUrls_.empty() ? std::string() : serviceUrls_.back();
    }

    std::vector<std::string> snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return serviceUrls_;
    }

   private:
    mutable std::mutex mutex_;
    std::vector<std::string> serviceUrls_;
};

}  // namespace

class ServiceInfoHolder {
   public:
    ServiceInfoHolder(ServiceInfo info) : serviceInfo_(std::move(info)) {}

    std::optional<ServiceInfo> getUpdatedValue() {
        std::lock_guard lock(mutex_);
        if (!owned_) {
            return std::nullopt;
        }
        owned_ = false;
        return std::move(serviceInfo_);
    }

    void updateValue(ServiceInfo info) {
        std::lock_guard lock(mutex_);
        serviceInfo_ = std::move(info);
        owned_ = true;
    }

   private:
    ServiceInfo serviceInfo_;
    bool owned_{true};

    mutable std::mutex mutex_;
};

class TestServiceInfoProvider : public ServiceInfoProvider {
   public:
    TestServiceInfoProvider(ServiceInfoHolder &serviceInfo) : serviceInfo_(serviceInfo) {}

    ServiceInfo initialServiceInfo() override { return serviceInfo_.getUpdatedValue().value(); }

    void initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) override {
        thread_ = std::thread([this, onServiceInfoUpdate] {
            while (running_) {
                auto updatedValue = serviceInfo_.getUpdatedValue();
                if (updatedValue) {
                    onServiceInfoUpdate(std::move(*updatedValue));
                }
                // Use a tight wait loop for tests
                std::this_thread::sleep_for(10ms);
            }
        });
    }

    ~TestServiceInfoProvider() override {
        running_ = false;
        if (thread_.joinable()) {
            thread_.join();
        }
    }

   private:
    std::thread thread_;
    ServiceInfoHolder &serviceInfo_;
    std::atomic_bool running_{true};
    mutable std::mutex mutex_;
};

TEST(AutoClusterFailoverTest, testFailoverToFirstAvailableSecondaryAfterDelay) {
    ProbeTcpServer availableSecondary;
    ProbeTcpServer unavailableSecondary;
    const auto primaryUrl = unavailableSecondary.getServiceUrl();
    unavailableSecondary.stop();

    ProbeTcpServer skippedSecondary;
    const auto skippedSecondaryUrl = skippedSecondary.getServiceUrl();
    skippedSecondary.stop();

    const auto availableSecondaryUrl = availableSecondary.getServiceUrl();
    ServiceUrlObserver observer;
    AutoClusterFailover provider =
        AutoClusterFailover::Builder(ServiceInfo(primaryUrl),
                                     {ServiceInfo(skippedSecondaryUrl), ServiceInfo(availableSecondaryUrl)})
            .withCheckInterval(20ms)
            .withFailoverThreshold(6)
            .withSwitchBackThreshold(6)
            .build();

    ASSERT_EQ(provider.initialServiceInfo().serviceUrl(), primaryUrl);

    observer.onUpdate(provider.initialServiceInfo());
    provider.initialize([&observer](const ServiceInfo &serviceInfo) { observer.onUpdate(serviceInfo); });

    ASSERT_FALSE(waitUntil(
        80ms, [&observer, &availableSecondaryUrl] { return observer.last() == availableSecondaryUrl; }));
    ASSERT_TRUE(waitUntil(
        2s, [&observer, &availableSecondaryUrl] { return observer.last() == availableSecondaryUrl; }));

    const auto updates = observer.snapshot();
    ASSERT_EQ(updates.size(), 2u);
    ASSERT_EQ(updates[0], primaryUrl);
    ASSERT_EQ(updates[1], availableSecondaryUrl);
}

TEST(AutoClusterFailoverTest, testSwitchBackToPrimaryAfterRecoveryDelay) {
    ProbeTcpServer primary;
    const auto primaryUrl = primary.getServiceUrl();
    primary.stop();

    ProbeTcpServer secondary;
    const auto secondaryUrl = secondary.getServiceUrl();

    ServiceUrlObserver observer;
    AutoClusterFailover provider =
        AutoClusterFailover::Builder(ServiceInfo(primaryUrl), {ServiceInfo(secondaryUrl)})
            .withCheckInterval(20ms)
            .withFailoverThreshold(4)
            .withSwitchBackThreshold(6)
            .build();

    observer.onUpdate(provider.initialServiceInfo());
    provider.initialize([&observer](const ServiceInfo &serviceInfo) { observer.onUpdate(serviceInfo); });

    ASSERT_TRUE(waitUntil(2s, [&observer, &secondaryUrl] { return observer.last() == secondaryUrl; }));

    primary.start();

    ASSERT_FALSE(waitUntil(80ms, [&observer, &primaryUrl] { return observer.last() == primaryUrl; }));
    ASSERT_TRUE(waitUntil(2s, [&observer, &primaryUrl] { return observer.last() == primaryUrl; }));

    const auto updates = observer.snapshot();
    ASSERT_EQ(updates.size(), 3u);
    ASSERT_EQ(updates[0], primaryUrl);
    ASSERT_EQ(updates[1], secondaryUrl);
    ASSERT_EQ(updates[2], primaryUrl);
}

TEST(ServiceInfoProviderTest, testSwitchCluster) {
    extern std::string getToken();  // from tests/AuthTokenTest.cc
    // Access "private/auth" namespace in cluster 1
    ServiceInfo info1{"pulsar://localhost:6650", AuthToken::createWithToken(getToken())};
    // Access "private/auth" namespace in cluster 2
    ServiceInfo info2{"pulsar+ssl://localhost:6653",
                      AuthTls::create(TEST_CONF_DIR "/client-cert.pem", TEST_CONF_DIR "/client-key.pem"),
                      TEST_CONF_DIR "/hn-verification/cacert.pem"};
    // Access "public/default" namespace in cluster 1, which doesn't require authentication
    ServiceInfo info3{"pulsar://localhost:6650"};

    ServiceInfoHolder serviceInfo{info1};
    auto client = Client::create(std::make_unique<TestServiceInfoProvider>(serviceInfo), {});

    const auto topicRequiredAuth = "private/auth/testUpdateConnectionInfo-" + std::to_string(time(nullptr));
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicRequiredAuth, producer));

    Reader reader;
    ASSERT_EQ(ResultOk, client.createReader(topicRequiredAuth, MessageId::earliest(), {}, reader));

    auto sendAndReceive = [&](const std::string &value) {
        MessageId msgId;
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(value).build(), msgId));
        LOG_INFO("Sent " << value << " to " << msgId);

        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg, 3000));
        LOG_INFO("Read " << msg.getDataAsString() << " from " << msgId);
        ASSERT_EQ(value, msg.getDataAsString());
    };

    sendAndReceive("msg-0");

    // Switch to cluster 2 (started by ./build-support/start-mim-test-service-inside-container.sh)
    ASSERT_FALSE(PulsarFriend::getConnections(client).empty());
    serviceInfo.updateValue(info2);
    ASSERT_TRUE(waitUntil(1s, [&] {
        return PulsarFriend::getConnections(client).empty() && client.getServiceInfo() == info2;
    }));

    // Now the same will access the same topic in cluster 2
    sendAndReceive("msg-1");

    // Switch back to cluster 1 without any authentication, the previous authentication info configured for
    // cluster 2 will be cleared.
    ASSERT_FALSE(PulsarFriend::getConnections(client).empty());
    serviceInfo.updateValue(info3);
    ASSERT_TRUE(waitUntil(1s, [&] {
        return PulsarFriend::getConnections(client).empty() && client.getServiceInfo() == info3;
    }));

    const auto topicNoAuth = "testUpdateConnectionInfo-" + std::to_string(time(nullptr));
    producer.close();
    ASSERT_EQ(ResultOk, client.createProducer(topicNoAuth, producer));
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("msg-2").build()));

    client.close();

    // Verify messages sent to cluster 1 and cluster 2 can be consumed successfully with correct
    // authentication info.
    auto verify = [](Client &client, const std::string &topic, const std::string &value) {
        Reader reader;
        ASSERT_EQ(ResultOk, client.createReader(topic, MessageId::earliest(), {}, reader));
        Message msg;
        ASSERT_EQ(ResultOk, reader.readNext(msg, 3000));
        ASSERT_EQ(value, msg.getDataAsString());
    };
    Client client1{info1.serviceUrl(), ClientConfiguration().setAuth(info1.authentication())};
    verify(client1, topicRequiredAuth, "msg-0");
    client1.close();

    Client client2{info2.serviceUrl(), ClientConfiguration()
                                           .setAuth(info2.authentication())
                                           .setTlsTrustCertsFilePath(*info2.tlsTrustCertsFilePath())};
    verify(client2, topicRequiredAuth, "msg-1");
    client2.close();

    Client client3{info3.serviceUrl()};
    verify(client3, topicNoAuth, "msg-2");
    client3.close();
}

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

#ifdef USE_ASIO
#include <asio.hpp>
#else
#include <boost/asio.hpp>
#endif

#include "WaitUtils.h"
#include "lib/AsioDefines.h"

using namespace pulsar;
using namespace std::chrono_literals;

// A TCP acceptor that simply listens on a random port.
// The kernel handles the TCP handshake into the backlog without calling accept(),
// so probe() (which only does a TCP connect) will succeed while the listener is open.
class SimpleListener {
   public:
    SimpleListener() : acceptor_(io_context_, ASIO::ip::tcp::endpoint(ASIO::ip::tcp::v4(), 0)) {
        port_ = static_cast<uint16_t>(acceptor_.local_endpoint().port());
    }

    uint16_t getPort() const { return port_; }

    std::string getServiceUrl() const { return "pulsar://localhost:" + std::to_string(port_); }

    // Close the listener, simulating the service going down.
    void close() {
        ASIO_ERROR ec;
        acceptor_.close(ec);
    }

    // Reopen the listener on the same port, simulating the service coming back.
    void reopen() {
        ASIO::ip::tcp::endpoint ep(ASIO::ip::tcp::v4(), port_);
        acceptor_.open(ep.protocol());
        acceptor_.set_option(ASIO::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
    }

   private:
    ASIO::io_context io_context_;
    ASIO::ip::tcp::acceptor acceptor_;
    uint16_t port_{0};
};

// Fast timings used across all unit tests.
static constexpr auto kCheckInterval = 200ms;
static constexpr auto kNoDelay = 0ms;
// Allow at least 2 check intervals to pass before failover/switch-back can trigger,
// plus a comfortable margin.
static constexpr auto kWaitTimeout = 3s;

// Wait until the client's current service URL matches the expected value.
static bool waitForServiceUrl(Client& client, const std::string& expected) {
    return waitUntil(kWaitTimeout, [&] { return client.getServiceInfo().serviceUrl == expected; });
}

// ====================== Unit tests (mock TCP listeners) ======================

TEST(AutoClusterFailoverTest, testStayOnPrimaryWhenPrimaryIsUp) {
    SimpleListener primary;
    SimpleListener secondary;

    Client client{primary.getServiceUrl()};

    auto failover = AutoClusterFailover::Builder({primary.getServiceUrl()}, {{secondary.getServiceUrl()}})
                        .withCheckInterval(kCheckInterval)
                        .withFailoverDelay(kNoDelay)
                        .withSwitchBackDelay(kNoDelay)
                        .build();
    failover.initialize(client);

    // Let a few probe cycles run; should remain on primary.
    std::this_thread::sleep_for(kCheckInterval * 4);
    EXPECT_EQ(primary.getServiceUrl(), client.getServiceInfo().serviceUrl);
}

TEST(AutoClusterFailoverTest, testFailoverWhenPrimaryIsDown) {
    SimpleListener secondary;

    // Primary port is not listening: probe will always fail.
    const std::string unreachableUrl = "pulsar://localhost:19998";

    Client client{unreachableUrl};

    auto failover = AutoClusterFailover::Builder({unreachableUrl}, {{secondary.getServiceUrl()}})
                        .withCheckInterval(kCheckInterval)
                        .withFailoverDelay(kNoDelay)
                        .withSwitchBackDelay(kNoDelay)
                        .build();
    failover.initialize(client);

    EXPECT_TRUE(waitForServiceUrl(client, secondary.getServiceUrl()));
}

TEST(AutoClusterFailoverTest, testSwitchBackWhenPrimaryRecovers) {
    SimpleListener primary;
    SimpleListener secondary;

    Client client{primary.getServiceUrl()};

    auto failover = AutoClusterFailover::Builder({primary.getServiceUrl()}, {{secondary.getServiceUrl()}})
                        .withCheckInterval(kCheckInterval)
                        .withFailoverDelay(kNoDelay)
                        .withSwitchBackDelay(kNoDelay)
                        .build();
    failover.initialize(client);

    // Verify we start on primary.
    ASSERT_EQ(primary.getServiceUrl(), client.getServiceInfo().serviceUrl);

    // Take primary down.
    primary.close();
    EXPECT_TRUE(waitForServiceUrl(client, secondary.getServiceUrl()));

    // Bring primary back.
    primary.reopen();
    EXPECT_TRUE(waitForServiceUrl(client, primary.getServiceUrl()));
}

TEST(AutoClusterFailoverTest, testSkipUnreachableSecondary) {
    SimpleListener secondary2;

    // secondary1 is not listening; secondary2 is.
    const std::string unreachablePrimary = "pulsar://localhost:19998";
    const std::string unreachableSecondary1 = "pulsar://localhost:19999";

    Client client{unreachablePrimary};

    auto failover = AutoClusterFailover::Builder({unreachablePrimary},
                                                 {{unreachableSecondary1}, {secondary2.getServiceUrl()}})
                        .withCheckInterval(kCheckInterval)
                        .withFailoverDelay(kNoDelay)
                        .withSwitchBackDelay(kNoDelay)
                        .build();
    failover.initialize(client);

    // Should skip secondary1 and land on secondary2.
    EXPECT_TRUE(waitForServiceUrl(client, secondary2.getServiceUrl()));
}

// ====================== Integration tests (real Pulsar at localhost:6650) ======================

TEST(AutoClusterFailoverTest, testFailoverToRealCluster) {
    const std::string unreachableUrl = "pulsar://localhost:19998";
    const std::string realUrl = "pulsar://localhost:6650";

    Client client{unreachableUrl};

    auto failover = AutoClusterFailover::Builder({unreachableUrl}, {{realUrl}})
                        .withCheckInterval(kCheckInterval)
                        .withFailoverDelay(kNoDelay)
                        .withSwitchBackDelay(kNoDelay)
                        .build();
    failover.initialize(client);

    // Wait until failover to the real cluster.
    ASSERT_TRUE(waitForServiceUrl(client, realUrl));

    // Verify we can create a producer on the real cluster after failover.
    Producer producer;
    const auto topic = "AutoClusterFailoverTest-" + std::to_string(time(nullptr));
    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    producer.close();
    client.close();
}

TEST(AutoClusterFailoverTest, testSwitchBackToRealCluster) {
    SimpleListener fakeSecondary;
    const std::string realUrl = "pulsar://localhost:6650";

    // Start on the real cluster (primary).
    Client client{realUrl};

    auto failover = AutoClusterFailover::Builder({realUrl}, {{fakeSecondary.getServiceUrl()}})
                        .withCheckInterval(kCheckInterval)
                        .withFailoverDelay(kNoDelay)
                        .withSwitchBackDelay(kNoDelay)
                        .build();
    failover.initialize(client);

    ASSERT_EQ(realUrl, client.getServiceInfo().serviceUrl);

    // Simulate the real cluster going down by closing the fake secondary listener and
    // pointing to an unreachable URL instead. Instead, we'll use a forward approach:
    // take primary "down" by swapping: use a listener as primary and real cluster as secondary,
    // then close the listener.
    client.close();

    // Restart: fake listener as primary, real cluster as secondary.
    SimpleListener fakePrimary;
    Client client2{fakePrimary.getServiceUrl()};

    auto failover2 = AutoClusterFailover::Builder({fakePrimary.getServiceUrl()}, {{realUrl}})
                         .withCheckInterval(kCheckInterval)
                         .withFailoverDelay(kNoDelay)
                         .withSwitchBackDelay(kNoDelay)
                         .build();
    failover2.initialize(client2);

    ASSERT_EQ(fakePrimary.getServiceUrl(), client2.getServiceInfo().serviceUrl);

    // Close the fake primary — triggers failover to real cluster.
    fakePrimary.close();
    ASSERT_TRUE(waitForServiceUrl(client2, realUrl));

    // Verify producers work on the real cluster after failover.
    Producer producer;
    const auto topic = "AutoClusterFailoverSwitchBack-" + std::to_string(time(nullptr));
    ASSERT_EQ(ResultOk, client2.createProducer(topic, producer));
    producer.close();

    // Bring fake primary back — should switch back.
    fakePrimary.reopen();
    ASSERT_TRUE(waitForServiceUrl(client2, fakePrimary.getServiceUrl()));

    client2.close();
}

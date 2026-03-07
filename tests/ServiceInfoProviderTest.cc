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
#include <pulsar/Client.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>

#include "PulsarFriend.h"
#include "WaitUtils.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;
using namespace std::chrono_literals;

class ServiceInfoQueue {
   public:
    void push(ServiceInfo info) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(info));
        }
        cond_.notify_all();
    }

    ServiceInfo pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return !queue_.empty() || !running_; });
        if (queue_.empty()) {
            throw std::runtime_error("Queue is closed");
        }

        ServiceInfo info = std::move(queue_.front());
        queue_.pop();
        return info;
    }

    void close() {
        running_ = false;
        cond_.notify_all();
    }

   private:
    mutable std::mutex mutex_;
    mutable std::condition_variable cond_;
    std::queue<ServiceInfo> queue_;
    std::atomic_bool running_{true};
};

class TestServiceInfoProvider : public ServiceInfoProvider {
   public:
    TestServiceInfoProvider(ServiceInfoQueue &queue) : queue_(queue) {}

    void initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) override {
        onServiceInfoUpdate(queue_.pop());
        thread_ = std::thread([this, onServiceInfoUpdate] {
            try {
                while (true) {
                    ServiceInfo info = queue_.pop();
                    onServiceInfoUpdate(std::move(info));
                }
            } catch (const std::runtime_error &) {
            }
        });
    }

    ~TestServiceInfoProvider() override {
        queue_.close();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

   private:
    ServiceInfoQueue &queue_;

    std::thread thread_;
    mutable std::mutex mutex_;
    std::queue<ServiceInfo> newServiceInfo_;
};

TEST(ServiceInfoProviderTest, testSwitchCluster) {
    extern std::string getToken();  // from tests/AuthToken.cc
    // Access "private/auth" namespace in cluster 1
    ServiceInfo info1{"pulsar://localhost:6650", AuthToken::createWithToken(getToken())};
    // Access "private/auth" namespace in cluster 2
    ServiceInfo info2{"pulsar+ssl://localhost:6653",
                      AuthTls::create(TEST_CONF_DIR "/client-cert.pem", TEST_CONF_DIR "/client-key.pem"),
                      TEST_CONF_DIR "/hn-verification/cacert.pem"};
    // Access "public/default" namespace in cluster 1, which doesn't require authentication
    ServiceInfo info3{"pulsar://localhost:6650"};

    ServiceInfoQueue queue;
    queue.push(info1);

    auto client = Client::create(std::make_unique<TestServiceInfoProvider>(queue), {});

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
    queue.push(info2);
    ASSERT_TRUE(waitUntil(1s, [&] {
        return PulsarFriend::getConnections(client).empty() && client.getServiceInfo() == info2;
    }));

    // Now the same will access the same topic in cluster 2
    sendAndReceive("msg-1");

    // Switch back to cluster 1 without any authentication, the previous authentication info configured for
    // cluster 2 will be cleared.
    ASSERT_FALSE(PulsarFriend::getConnections(client).empty());
    queue.push(info3);
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

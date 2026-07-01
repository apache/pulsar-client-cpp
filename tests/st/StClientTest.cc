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
#include <pulsar/st/Client.h>

#include <string>

// Broker-free tests for PulsarClientBuilder::build() and the client shell: they
// validate configuration mapping and lifecycle, never connecting anywhere.

using namespace pulsar::st;

TEST(StClientTest, testEmptyServiceUrlIsRejected) {
    auto client = PulsarClient::builder().build();
    ASSERT_FALSE(client);
    ASSERT_EQ(client.error().result, ResultInvalidUrl);
}

TEST(StClientTest, testTlsEnabledRequiresTlsScheme) {
    auto client =
        PulsarClient::builder().serviceUrl("pulsar://localhost:6650").tlsPolicy({.enabled = true}).build();
    ASSERT_FALSE(client);
    ASSERT_EQ(client.error().result, ResultInvalidConfiguration);
}

TEST(StClientTest, testTlsEnabledAcceptsTlsScheme) {
    auto client = PulsarClient::builder()
                      .serviceUrl("pulsar+ssl://localhost:6651")
                      .tlsPolicy({.enabled = true, .allowInsecureConnection = true})
                      .build();
    ASSERT_TRUE(client);
    ASSERT_TRUE(client->close());
}

TEST(StClientTest, testBuildAndCloseWithoutBroker) {
    auto client = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    ASSERT_TRUE(client);
    ASSERT_TRUE(static_cast<bool>(*client));

    auto closed = client->close();
    ASSERT_TRUE(closed);
}

TEST(StClientTest, testBuildWithPoliciesWithoutBroker) {
    using namespace std::chrono_literals;
    auto client = PulsarClient::builder()
                      .serviceUrl("pulsar://localhost:6650")
                      .connectionPolicy({.connectionsPerBroker = 2,
                                         .connectionTimeout = 5000ms,
                                         .operationTimeout = 20000ms,
                                         .keepAliveInterval = 30s})
                      .threadPolicy({.ioThreads = 2, .messageListenerThreads = 2})
                      .memoryPolicy({.limit = MemorySize::ofMiB(64)})
                      .backoffPolicy({.initialBackoff = 100ms, .maxBackoff = 10s})
                      .build();
    ASSERT_TRUE(client);
    ASSERT_TRUE(client->close());
}

TEST(StClientTest, testProducerCreationReportsNotSupportedYet) {
    auto client = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    ASSERT_TRUE(client);

    auto producer = client->newProducer().topic("st-topic").create();
    ASSERT_FALSE(producer);
    ASSERT_EQ(producer.error().result, ResultOperationNotSupported);

    ASSERT_TRUE(client->close());
}

TEST(StClientTest, testTransactionReportsNotSupportedYet) {
    auto client = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    ASSERT_TRUE(client);

    auto txn = client->newTransaction();
    ASSERT_FALSE(txn);
    ASSERT_EQ(txn.error().result, ResultOperationNotSupported);

    ASSERT_TRUE(client->close());
}

TEST(StClientTest, testShutdownWithoutBroker) {
    auto client = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    ASSERT_TRUE(client);
    client->shutdown();  // immediate teardown must not crash or hang
}

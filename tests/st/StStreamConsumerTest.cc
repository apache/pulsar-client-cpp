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

#include <chrono>
#include <string>
#include <utility>

// Broker-free tests for the stream consumer's subscribe path: the failures that must
// reach the caller without any broker. The controller protocol, the per-segment
// consumers and the position-vector acks are exercised against a real broker by
// StStreamConsumerE2ETest.

using namespace pulsar::st;
using namespace std::chrono_literals;

namespace {

// Nothing listens on port 1, so every connect is refused immediately; the short
// operation timeout bounds the lookup retries that follow.
PulsarClient unreachableClient() {
    auto clientResult = PulsarClient::builder()
                            .serviceUrl("pulsar://localhost:1")
                            .connectionPolicy({.connectionTimeout = 1s, .operationTimeout = 1s})
                            .build();
    EXPECT_TRUE(clientResult) << clientResult.error();
    return std::move(clientResult).value();
}

}  // namespace

TEST(StStreamConsumerTest, testSubscribeFailsWhenControllerIsUnreachable) {
    PulsarClient client = unreachableClient();
    // Regression: the start promise used to be completed (successfully) by the
    // assignment listener's empty replay before the controller subscribe even ran, so
    // a subscribe that could not reach any broker still handed back a "live" consumer.
    auto consumerResult = client.newStreamConsumer(Schema<std::string>{})
                              .topic("topic://public/default/orders")
                              .subscriptionName("sub")
                              .subscribe();
    ASSERT_FALSE(consumerResult);
    EXPECT_NE(consumerResult.error().result, ResultOk);
    EXPECT_TRUE(client.close());
}

TEST(StStreamConsumerTest, testNamespaceModeIsNotSupportedYet) {
    PulsarClient client = unreachableClient();
    auto consumerResult = client.newStreamConsumer(Schema<std::string>{})
                              .inNamespace("public/default")
                              .subscriptionName("sub")
                              .subscribe();
    ASSERT_FALSE(consumerResult);
    EXPECT_EQ(consumerResult.error().result, ResultOperationNotSupported);
    EXPECT_TRUE(client.close());
}

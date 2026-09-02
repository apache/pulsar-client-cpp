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
#include <pulsar/MessageBuilder.h>

#include <chrono>
#include <memory>

#include "lib/ExecutorService.h"
#include "lib/st/MessageIdImpl.h"
#include "lib/st/MessageImpl.h"
#include "lib/st/ReceiveQueue.h"

// Broker-free tests for the fan-in mux queue's batch receive: greedy drain of what
// is buffered, deadline behavior, and close. The single-message paths are covered
// end-to-end by the queue-consumer tests.

using namespace pulsar::st;

namespace {

MessageImplPtr makeMessage(int i) {
    auto classic = pulsar::MessageBuilder().setContent("m-" + std::to_string(i)).build();
    return std::make_shared<MessageImpl>(classic, MessageIdFactory::create(pulsar::MessageId::earliest(), 0));
}

pulsar::ExecutorServicePtr makeExecutor() {
    static auto provider = std::make_shared<pulsar::ExecutorServiceProvider>(1);
    return provider->get();
}

}  // namespace

TEST(StReceiveQueueTest, testReceiveMultiDrainsBufferedWithoutWaiting) {
    auto queue = std::make_shared<ReceiveQueue>(makeExecutor(), 100);
    for (int i = 0; i < 5; i++) {
        queue->offer(makeMessage(i));
    }
    auto batch = queue->receiveMultiAsync(3, std::chrono::seconds(10)).get();
    ASSERT_TRUE(batch);
    ASSERT_EQ(batch->size(), 3u);

    auto rest = queue->receiveMultiAsync(10, std::chrono::milliseconds(50)).get();
    ASSERT_TRUE(rest);
    // Fewer than asked: the deadline elapsed after draining what was buffered.
    ASSERT_EQ(rest->size(), 2u);
    queue->close();
}

TEST(StReceiveQueueTest, testReceiveMultiTimesOutEmpty) {
    auto queue = std::make_shared<ReceiveQueue>(makeExecutor(), 100);
    auto batch = queue->receiveMultiAsync(4, std::chrono::milliseconds(50)).get();
    ASSERT_TRUE(batch);
    ASSERT_TRUE(batch->empty()) << "a quiet deadline yields an empty batch, not an error";
    queue->close();
}

TEST(StReceiveQueueTest, testReceiveMultiWaitsForFirstThenDrains) {
    auto queue = std::make_shared<ReceiveQueue>(makeExecutor(), 100);
    // The batch collects until full or deadline; with two messages arriving after the
    // receive parked, the short deadline hands back a partial batch of both.
    auto future = queue->receiveMultiAsync(5, std::chrono::milliseconds(500));
    queue->offer(makeMessage(0));
    queue->offer(makeMessage(1));
    auto batch = future.get();
    ASSERT_TRUE(batch);
    ASSERT_GE(batch->size(), 1u);
    ASSERT_LE(batch->size(), 2u);
    queue->close();
}

TEST(StReceiveQueueTest, testReceiveMultiFailsWhenClosedEmpty) {
    auto queue = std::make_shared<ReceiveQueue>(makeExecutor(), 100);
    queue->close();
    auto batch = queue->receiveMultiAsync(4, std::chrono::seconds(1)).get();
    ASSERT_FALSE(batch);
    ASSERT_EQ(batch.error().result, pulsar::ResultAlreadyClosed);
}

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
#include <pulsar/ClientConfiguration.h>

#include <memory>
#include <vector>

#include "lib/ClientImpl.h"
#include "lib/st/ConsumerAssignmentSession.h"

// Broker-free tests for the consumer assignment session: the lifecycle paths that
// must be safe without any connection, and the listener replay contract. The full
// session protocol (controller lookup, subscribe, assignment updates, reconnect) is
// exercised end-to-end against a real broker by the stream-consumer integration
// tests.

using namespace pulsar::st;

TEST(ConsumerAssignmentSessionTest, testSessionLifecycleWithoutConnection) {
    auto classic =
        std::make_shared<pulsar::ClientImpl>("pulsar://localhost:6650", pulsar::ClientConfiguration{});
    classic->initialize();

    auto session = std::make_shared<ConsumerAssignmentSession>(
        classic, "topic://public/default/orders", "sub", "consumer-1", pulsar::ScalableConsumerType_STREAM);

    // Fresh session: empty assignment, a real consumer id, distinct per session.
    ASSERT_TRUE(session->currentAssignment().empty());
    auto other = std::make_shared<ConsumerAssignmentSession>(
        classic, "topic://public/default/other", "sub", "consumer-2", pulsar::ScalableConsumerType_STREAM);
    ASSERT_NE(session->consumerId(), other->consumerId());

    // Closing before start (and closing twice) must be safe.
    session->close();
    session->close();
    other->close();

    classic->shutdown();
}

TEST(ConsumerAssignmentSessionTest, testSetListenerDoesNotReplayBeforeFirstAssignment) {
    auto classic =
        std::make_shared<pulsar::ClientImpl>("pulsar://localhost:6650", pulsar::ClientConfiguration{});
    classic->initialize();

    auto session = std::make_shared<ConsumerAssignmentSession>(
        classic, "topic://public/default/orders", "sub", "consumer-1", pulsar::ScalableConsumerType_STREAM);

    // Before any assignment there is nothing to replay: a listener registered early
    // must not be handed an empty "assignment" (a consumer would take that as a
    // successful, segment-less start). The replay of a received assignment is
    // exercised by the stream-consumer integration tests.
    int calls = 0;
    session->setListener(
        [&](const std::vector<AssignedSegment>&, const std::vector<AssignedSegment>&) { calls++; });
    ASSERT_EQ(calls, 0);

    session->close();
    classic->shutdown();
}

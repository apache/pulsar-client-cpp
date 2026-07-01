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

#include "lib/ClientImpl.h"
#include "lib/st/DagWatchSession.h"

// Broker-free tests for the DAG-watch session: the pure lookup-name mapping and
// the lifecycle paths that must be safe without any connection. The full
// session protocol (lookup, updates, reconnect) is exercised end-to-end against
// a real broker by the producer integration tests.

using namespace pulsar::st;

TEST(DagWatchSessionTest, testLookupCompatibleTopicMapsTopicScheme) {
    // The classic TopicName does not know the topic:// scheme; the connection
    // lookup uses the persistent:// twin while the wire lookup keeps the
    // original spelling.
    ASSERT_EQ(DagWatchSession::lookupCompatibleTopic("topic://public/default/orders"),
              "persistent://public/default/orders");
    ASSERT_EQ(DagWatchSession::lookupCompatibleTopic("persistent://public/default/orders"),
              "persistent://public/default/orders");
    ASSERT_EQ(DagWatchSession::lookupCompatibleTopic("orders"), "orders");
}

TEST(DagWatchSessionTest, testSessionLifecycleWithoutConnection) {
    auto classic =
        std::make_shared<pulsar::ClientImpl>("pulsar://localhost:6650", pulsar::ClientConfiguration{});
    classic->initialize();

    auto session = std::make_shared<DagWatchSession>(classic, "topic://public/default/orders", true);

    // Fresh session: empty layout, input identity, unique session id.
    ASSERT_TRUE(session->currentLayout().activeSegments().empty());
    ASSERT_EQ(session->currentLayout().epoch(), 0u);
    ASSERT_EQ(session->topicName(), "topic://public/default/orders");

    auto other = std::make_shared<DagWatchSession>(classic, "topic://public/default/other", true);
    ASSERT_NE(session->sessionId(), other->sessionId());

    // Closing before start (and closing twice) must be safe.
    session->close();
    session->close();
    other->close();

    classic->shutdown();
}

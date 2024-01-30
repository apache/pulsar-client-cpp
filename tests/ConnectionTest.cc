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
 */
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "lib/ClientConnection.h"
#include "lib/ClientConnectionAdaptor.h"

using namespace pulsar;

class MockClientConnection {
   public:
    MOCK_METHOD(void, close, (Result));

    void checkServerError(ServerError error, const std::string& message) {
        ::pulsar::adaptor::checkServerError(*this, error, message);
    }
};

// These error messages come from
// https://github.com/apache/pulsar/blob/a702e5a582eaa8292720f9e25fc2dcf858078813/pulsar-broker/src/main/java/org/apache/pulsar/broker/lookup/TopicLookupBase.java#L334-L351
static const std::vector<std::string> retryableErrorMessages{
    "Namespace bundle public/default/0x00000000_0xffffffff is being unloaded",
    "org.apache.zookeeper.KeeperException$OperationTimeoutException: KeeperErrorCode = OperationTimeout for "
    "/namespace/public/default/0x00000000_0xffffffff",
    "Failed to acquire ownership for namespace bundle public/default/0x00000000_0xffffffff"};

TEST(ConnectionTest, testCheckServerError) {
    MockClientConnection conn;
    EXPECT_CALL(conn, close(ResultDisconnected)).Times(0);
    for (auto&& msg : retryableErrorMessages) {
        conn.checkServerError(pulsar::proto::ServiceNotReady, msg);
    }
}

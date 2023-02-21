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
#include <sys/resource.h>
#include <time.h>

#include "../HttpHelper.h"

using namespace pulsar;

class ConnectionFailTest : public ::testing::TestWithParam<int> {
   public:
    void SetUp() override {
        struct rlimit limit;
        ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &limit), 0);
        maxFdCount_ = limit.rlim_max;
        int numPartitions = GetParam();
        topic_ = "test-connection-fail-" + std::to_string(numPartitions) + std::to_string(time(nullptr));
        if (numPartitions > 0) {
            int res = makePutRequest(
                "http://localhost:8080/admin/v2/persistent/public/default/" + topic_ + "/partitions",
                std::to_string(numPartitions));
            ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
        }
    }

    void TearDown() override { client_.close(); }

   protected:
    Client client_{"pulsar://localhost:6650"};
    std::string topic_;
    int maxFdCount_;

    void updateFdLimit(int fdLimit) {
        struct rlimit limit;
        limit.rlim_cur = fdLimit;
        limit.rlim_max = maxFdCount_;
        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &limit), 0);
        std::cout << "Updated fd limit to " << limit.rlim_cur << std::endl;
    }
};

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST_P(ConnectionFailTest, test) {
    int fdLimit = 1;
    for (; fdLimit < 100; fdLimit++) {
        updateFdLimit(fdLimit);

        Producer producer;
        auto result = client_.createProducer(topic_, producer);
        ASSERT_TRUE(result == ResultOk || result == ResultConnectError);
        if (result == ResultConnectError) {
            continue;
        }

        Consumer consumer;
        result = client_.subscribe(topic_, "sub", consumer);
        ASSERT_TRUE(result == ResultOk || result == ResultConnectError);
        if (result == ResultConnectError) {
            continue;
        }

        Reader reader;
        result = client_.createReader(topic_, MessageId::earliest(), {}, reader);
        ASSERT_TRUE(result == ResultOk || result == ResultConnectError);
        if (result == ResultOk) {
            break;
        }
    }
    std::cout << "Create producer, consumer and reader successfully when fdLimit is " << fdLimit << std::endl;
    ASSERT_TRUE(fdLimit < 100);
}

INSTANTIATE_TEST_SUITE_P(Unix, ConnectionFailTest, ::testing::Values(0, 5));

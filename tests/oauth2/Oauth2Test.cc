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
// Run `docker-compose up -d` to set up the test environment for this test.
#include <gtest/gtest.h>
#include <pulsar/Client.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "lib/Base64Utils.h"

using namespace pulsar;

#ifndef TEST_ROOT_PATH
#define TEST_ROOT_PATH "."
#endif

static const std::string gKeyPath = std::string(TEST_ROOT_PATH) + "/../test-conf/cpp_credentials_file.json";
static std::string gClientId;
static std::string gClientSecret;
static ParamMap gCommonParams;

static Result testCreateProducer(const std::string& privateKey);

static std::string credentials(const std::string& clientId, const std::string& clientSecret) {
    return base64::encode(R"({"client_id":")" + clientId + R"(","client_secret":")" + clientSecret + R"("})");
}

TEST(Oauth2Test, testBase64Key) {
    ASSERT_EQ(ResultOk,
              testCreateProducer("data:application/json;base64," + credentials(gClientId, gClientSecret)));
    ASSERT_EQ(ResultAuthenticationError,
              testCreateProducer("data:application/json;base64," + credentials("test-id", "test-secret")));
}

TEST(Oauth2Test, testFileKey) {
    ASSERT_EQ(ResultOk, testCreateProducer("file://" + gKeyPath));
    ASSERT_EQ(ResultOk, testCreateProducer("file:" + gKeyPath));
    ASSERT_EQ(ResultOk, testCreateProducer(gKeyPath));
    ASSERT_EQ(ResultAuthenticationError, testCreateProducer("file:///tmp/file-not-exist"));
}

TEST(Oauth2Test, testWrongUrl) {
    ASSERT_EQ(ResultAuthenticationError,
              testCreateProducer("data:text/plain;base64," + credentials(gClientId, gClientSecret)));
    ASSERT_EQ(ResultAuthenticationError,
              testCreateProducer("data:application/json;text," + credentials(gClientId, gClientSecret)));
    ASSERT_EQ(ResultAuthenticationError, testCreateProducer("my-protocol:" + gKeyPath));
}

int main(int argc, char* argv[]) {
    std::cout << "Load Oauth2 configs from " << gKeyPath << "..." << std::endl;
    boost::property_tree::ptree root;
    boost::property_tree::read_json(gKeyPath, root);
    gClientId = root.get<std::string>("client_id");
    gClientSecret = root.get<std::string>("client_secret");
    gCommonParams["issuer_url"] = root.get<std::string>("issuer_url");
    gCommonParams["audience"] = root.get<std::string>("audience");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

static Result testCreateProducer(const std::string& privateKey) {
    ClientConfiguration conf;
    auto params = gCommonParams;
    params["private_key"] = privateKey;
    conf.setAuth(AuthOauth2::create(params));
    Client client{"pulsar://localhost:6650", conf};
    Producer producer;
    const auto result = client.createProducer("oauth2-test", producer);
    client.close();
    return result;
}

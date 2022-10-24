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
#include <pulsar/Authentication.h>
#include <pulsar/Client.h>

#include <string>

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string serviceUrlHttp = "http://localhost:8080";
static const std::string serviceUrlTls = "pulsar+ssl://localhost:6651";
static const std::string serviceUrlHttps = "https://localhost:8443";
static const std::string caPath = "../test-conf/cacert.pem";
static const std::string clientCertificatePath = "../test-conf/client-cert.pem";
static const std::string clientPrivateKeyPath = "../test-conf/client-key.pem";

TEST(AuthPluginBasic, testBasic) {
    ClientConfiguration config = ClientConfiguration();
    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(auth.use_count(), 1);

    config.setAuth(auth);
    Client client(serviceUrl, config);

    std::string topicName = "persistent://private/auth/test-basic";
    std::string subName = "subscription-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    producer.close();
}

TEST(AuthPluginBasic, testBasicWithHttp) {
    ClientConfiguration config = ClientConfiguration();
    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    config.setAuth(auth);
    Client client(serviceUrlHttp, config);

    std::string topicName = "persistent://private/auth/test-basic";
    std::string subName = "subscription-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    producer.close();
}

TEST(AuthPluginBasic, testNoAuth) {
    ClientConfiguration config = ClientConfiguration();
    Client client(serviceUrl, config);

    std::string topicName = "persistent://private/auth/test-basic";
    std::string subName = "subscription-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultAuthorizationError, result);
}

TEST(AuthPluginBasic, testNoAuthWithHttp) {
    ClientConfiguration config = ClientConfiguration();
    Client client(serviceUrlHttp, config);

    std::string topicName = "persistent://private/auth/test-basic";
    std::string subName = "subscription-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);
}

TEST(AuthPluginBasic, testLoadAuth) {
    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");
    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    auth = pulsar::AuthBasic::create("{\"username\":\"super-user\",\"password\":\"123789\"}");
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "super-user:123789");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    auth = pulsar::AuthBasic::create(
        "{\"username\":\"super-user\",\"password\":\"123789\",\"method\":\"my-method\"}");
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "my-method");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "super-user:123789");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    ParamMap p = ParamMap();
    p["username"] = "super-user-2";
    p["password"] = "456789";
    auth = pulsar::AuthBasic::create(p);
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "super-user-2:456789");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    p = ParamMap();
    p["username"] = "super-user-2";
    p["password"] = "456789";
    p["method"] = "my-method-2";
    auth = pulsar::AuthBasic::create(p);
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "my-method-2");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "super-user-2:456789");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);
}

TEST(AuthPluginBasic, testAuthBasicWithServiceUrlTlsWithTlsTransport) {
    ClientConfiguration config = ClientConfiguration();

    config.setTlsPrivateKeyFilePath(clientPrivateKeyPath);
    config.setTlsCertificateFilePath(clientCertificatePath);
    config.setTlsTrustCertsFilePath(caPath);

    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    config.setAuth(auth);
    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-basic";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    producer.close();
}

TEST(AuthPluginBasic, testAuthBasicWithServiceUrlHttpsWithTlsTransport) {
    ClientConfiguration config = ClientConfiguration();

    config.setTlsPrivateKeyFilePath(clientPrivateKeyPath);
    config.setTlsCertificateFilePath(clientCertificatePath);
    config.setTlsTrustCertsFilePath(caPath);

    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    config.setAuth(auth);
    Client client(serviceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-basic";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    producer.close();
}

TEST(AuthPluginBasic, testAuthBasicWithServiceUrlTlsNoTlsTransport) {
    ClientConfiguration config = ClientConfiguration();

    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    config.setAuth(auth);
    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-basic";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);
}

TEST(AuthPluginBasic, testAuthBasicWithServiceUrlHttpsNoTlsTransport) {
    ClientConfiguration config = ClientConfiguration();

    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "basic");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);

    config.setAuth(auth);
    Client client(serviceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-basic";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultLookupError, result);
}

TEST(AuthPluginBasic, testAuthBasicWithCustomMethodName) {
    ClientConfiguration config = ClientConfiguration();

    AuthenticationPtr auth = pulsar::AuthBasic::create("admin", "123456", "method-1");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "method-1");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), "admin:123456");
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);
}

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

#include <array>
#include <boost/algorithm/string.hpp>
#include <sstream>
#ifdef USE_ASIO
#include <asio.hpp>
#else
#include <boost/asio.hpp>
#endif
#include <thread>

#include "lib/AsioDefines.h"
#include "lib/Future.h"
#include "lib/Latch.h"
#include "lib/LogUtils.h"
#include "lib/Utils.h"
#include "lib/auth/AuthOauth2.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

int globalTestTlsMessagesCounter = 0;
static const std::string serviceUrlTls = "pulsar+ssl://localhost:6651";
static const std::string serviceUrlHttps = "https://localhost:8443";

#ifndef TEST_CONF_DIR
#error "TEST_CONF_DIR is not specified"
#endif

static const std::string caPath = TEST_CONF_DIR "/cacert.pem";
static const std::string clientPublicKeyPath = TEST_CONF_DIR "/client-cert.pem";
static const std::string clientPrivateKeyPath = TEST_CONF_DIR "/client-key.pem";
static const std::string chainedClientPublicKeyPath = TEST_CONF_DIR "/chained-client-cert.pem";
static const std::string chainedClientPrivateKeyPath = TEST_CONF_DIR "/chained-client-key.pem";

// Man in middle certificate which tries to act as a broker by sending its own valid certificate
static const std::string mimServiceUrlTls = "pulsar+ssl://localhost:6653";
static const std::string mimServiceUrlHttps = "https://localhost:8444";

static const std::string mimCaPath = TEST_CONF_DIR "/hn-verification/cacert.pem";

static void sendCallBackTls(Result r, const MessageId& msgId) {
    ASSERT_EQ(r, ResultOk);
    globalTestTlsMessagesCounter++;
}

TEST(AuthPluginTest, testTls) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    AuthenticationPtr auth = pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath);

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->getCommandData(), "none");
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(auth.use_count(), 1);

    config.setAuth(auth);
    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-tls";
    std::string subName = "subscription-name";
    int numOfMessages = 10;

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    client.subscribe(topicName, subName, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send Asynchronously
    std::string prefix = "test-tls-message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBackTls);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(globalTestTlsMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(AuthPluginTest, testTlsDetectPulsarSsl) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-tls-detect";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testTlsDetectPulsarSslWithHostNameValidation) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setValidateHostName(true);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlTls, config);
    std::string topicName = "persistent://private/auth/testTlsDetectPulsarSslWithHostNameValidation";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, res);
}

TEST(AuthPluginTest, testTlsDetectPulsarSslWithHostNameValidationMissingCertsFile) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsAllowInsecureConnection(false);
    config.setValidateHostName(true);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlTls, config);
    std::string topicName =
        "persistent://private/auth/testTlsDetectPulsarSslWithHostNameValidationMissingCertsFile";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, res);
}

TEST(AuthPluginTest, testTlsDetectPulsarSslWithInvalidBroker) {
    ClientConfiguration configWithValidateHostname = ClientConfiguration();
    configWithValidateHostname.setTlsTrustCertsFilePath(mimCaPath);
    configWithValidateHostname.setTlsAllowInsecureConnection(false);
    configWithValidateHostname.setValidateHostName(true);
    configWithValidateHostname.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(mimCaPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client clientWithValidateHostname(mimServiceUrlTls, configWithValidateHostname);
    Client client(mimServiceUrlTls, config);

    std::string topicName = "persistent://private/auth/testTlsDetectPulsarSslWithInvalidBroker";

    // 1. Client tries to connect to broker with hostname="localhost"
    // 2. Broker sends x509 certificates with CN = "pulsar"
    // 3. Client verifies the host-name and closes the connection
    Producer producer;
    Result res = clientWithValidateHostname.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, res);

    res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, res);
}

TEST(AuthPluginTest, testTlsDetectHttps) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-tls-detect-https";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testTlsDetectHttpsWithHostNameValidation) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));
    config.setValidateHostName(true);

    Client client(serviceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-tls-detect-https-with-hostname-validation";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, res);
}

TEST(AuthPluginTest, testTlsDetectHttpsWithHostNameValidationMissingCertsFile) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));
    config.setValidateHostName(true);

    Client client(serviceUrlHttps, config);

    std::string topicName =
        "persistent://private/auth/test-tls-detect-https-with-hostname-validation-missing-certs-file";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultLookupError, res);
}

TEST(AuthPluginTest, testTlsDetectHttpsWithInvalidBroker) {
    ClientConfiguration configWithValidateHostname = ClientConfiguration();
    configWithValidateHostname.setTlsTrustCertsFilePath(mimCaPath);
    configWithValidateHostname.setTlsAllowInsecureConnection(false);
    configWithValidateHostname.setValidateHostName(true);
    configWithValidateHostname.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(mimCaPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client clientWithValidateHostname(mimServiceUrlHttps, configWithValidateHostname);
    Client client(mimServiceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-tls-detect-https-with-invalid-broker";

    // 1. Client tries to connect to broker with hostname="localhost"
    // 2. Broker sends x509 certificates with CN = "pulsar"
    // 3. Client verifies the host-name and closes the connection
    Producer producer;
    Result res = clientWithValidateHostname.createProducer(topicName, producer);
    ASSERT_EQ(ResultLookupError, res);

    res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, res);
}

TEST(AuthPluginTest, testTlsDetectClientCertSignedByICA) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setValidateHostName(true);
    config.setAuth(pulsar::AuthTls::create(chainedClientPublicKeyPath, chainedClientPrivateKeyPath));

    Client client(serviceUrlTls, config);
    std::string topicName = "persistent://private/auth/testTlsDetectClientCertSignedByICA";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, res);
}

// There is a bug that clang-tidy could report memory leak for boost::algorithm::split, see
// https://github.com/boostorg/algorithm/issues/63.
static std::vector<std::string> split(const std::string& s, char separator) {
    std::vector<std::string> tokens;
    size_t startPos = 0;
    while (startPos < s.size()) {
        auto pos = s.find(separator, startPos);
        if (pos == std::string::npos) {
            tokens.emplace_back(s.substr(startPos));
            break;
        }
        tokens.emplace_back(s.substr(startPos, pos - startPos));
        startPos = pos + 1;
    }
    return tokens;
}

namespace testAthenz {
std::string principalToken;

// ASIO::ip::tcp::iostream could call a virtual function during destruction, so the clang-tidy will fail by
// clang-analyzer-optin.cplusplus.VirtualCall. Here we write a simple stream to read lines from socket.
class SocketStream {
   public:
    SocketStream(ASIO::ip::tcp::socket& socket) : socket_(socket) {}

    bool getline(std::string& line) {
        auto pos = buffer_.find('\n', bufferPos_);
        if (pos != std::string::npos) {
            line = buffer_.substr(bufferPos_, pos - bufferPos_);
            bufferPos_ = pos + 1;
            return true;
        }

        std::array<char, 1024> buffer;
        ASIO_ERROR error;
        auto length = socket_.read_some(ASIO::buffer(buffer.data(), buffer.size()), error);
        if (error == ASIO::error::eof) {
            return false;
        } else if (error) {
            LOG_ERROR("Failed to read from socket: " << error.message());
            return false;
        }
        buffer_.append(buffer.data(), length);

        pos = buffer_.find('\n', bufferPos_);
        if (pos != std::string::npos) {
            line = buffer_.substr(bufferPos_, pos - bufferPos_);
            bufferPos_ = pos + 1;
        } else {
            line = "";
        }
        return true;
    }

   private:
    ASIO::ip::tcp::socket& socket_;
    std::string buffer_;
    size_t bufferPos_{0};
};

void mockZTS(Latch& latch, int port) {
    LOG_INFO("-- MockZTS started");
    ASIO::io_service io;
    ASIO::ip::tcp::acceptor acceptor(io, ASIO::ip::tcp::endpoint(ASIO::ip::tcp::v4(), port));

    LOG_INFO("-- MockZTS waiting for connnection");
    latch.countdown();
    ASIO::ip::tcp::socket socket(io);
    acceptor.accept(socket);
    LOG_INFO("-- MockZTS got connection");

    std::string headerLine;
    SocketStream stream(socket);
    while (stream.getline(headerLine)) {
        if (headerLine.empty()) {
            continue;
        }
        auto kv = split(headerLine, ' ');
        if (kv[0] == "Athenz-Principal-Auth:") {
            principalToken = kv[1];
        }

        if (headerLine == "\r") {
            std::ostringstream stream;
            std::string mockToken = "{\"token\":\"mockToken\",\"expiryTime\":4133980800}";
            stream << "HTTP/1.1 200 OK" << '\n';
            stream << "Host: localhost" << '\n';
            stream << "Content-Type: application/json" << '\n';
            stream << "Content-Length: " << mockToken.size() << '\n';
            stream << '\n';
            stream << mockToken << '\n';
            auto response = stream.str();
            socket.send(ASIO::const_buffer(response.c_str(), response.length()));
            break;
        }
    }

    socket.close();
    acceptor.close();
    LOG_INFO("-- MockZTS exiting");
}
}  // namespace testAthenz

TEST(AuthPluginTest, testAthenz) {
    Latch latch(1);
    std::thread zts(std::bind(&testAthenz::mockZTS, std::ref(latch), 9999));
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "tenantDomain": "pulsar.test.tenant",
        "tenantService": "service",
        "providerDomain": "pulsar.test.provider",
        "privateKey": "file:)" +
                         clientPrivateKeyPath + R"(",
        "ztsUrl": "http://localhost:9999"
    })";

    LOG_INFO("PARAMS: " << params);
    latch.wait();
    pulsar::AuthenticationPtr auth = pulsar::AuthAthenz::create(params);
    ASSERT_EQ(auth->getAuthMethodName(), "athenz");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getHttpHeaders(), "Athenz-Role-Auth: mockToken");
    ASSERT_EQ(data->getCommandData(), "mockToken");
    zts.join();
    auto kvs = split(testAthenz::principalToken, ';');
    for (std::vector<std::string>::iterator itr = kvs.begin(); itr != kvs.end(); itr++) {
        auto kv = split(*itr, '=');
        if (kv[0] == "d") {
            ASSERT_EQ(kv[1], "pulsar.test.tenant");
        } else if (kv[0] == "n") {
            ASSERT_EQ(kv[1], "service");
        }
    }
}

TEST(AuthPluginTest, testDisable) {
    pulsar::AuthenticationDataPtr data;

    pulsar::AuthenticationPtr auth = pulsar::AuthFactory::Disabled();
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "none");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->getCommandData(), "none");
    ASSERT_EQ(auth.use_count(), 1);
}

TEST(AuthPluginTest, testAuthFactoryTls) {
    pulsar::AuthenticationDataPtr data;
    AuthenticationPtr auth = pulsar::AuthFactory::create(
        "tls", "tlsCertFile:" + clientPublicKeyPath + ",tlsKeyFile:" + clientPrivateKeyPath);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(data->getTlsCertificates(), clientPublicKeyPath);
    ASSERT_EQ(data->getTlsPrivateKey(), clientPrivateKeyPath);

    ClientConfiguration config = ClientConfiguration();
    config.setAuth(auth);
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-tls-factory";
    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testAuthFactoryAthenz) {
    Latch latch(1);
    std::thread zts(std::bind(&testAthenz::mockZTS, std::ref(latch), 9998));
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "tenantDomain": "pulsar.test2.tenant",
        "tenantService": "service",
        "providerDomain": "pulsar.test.provider",
        "privateKey": "file:)" +
                         clientPrivateKeyPath + R"(",
        "ztsUrl": "http://localhost:9998"
    })";
    LOG_INFO("PARAMS: " << params);
    latch.wait();
    pulsar::AuthenticationPtr auth = pulsar::AuthFactory::create("athenz", params);
    ASSERT_EQ(auth->getAuthMethodName(), "athenz");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getHttpHeaders(), "Athenz-Role-Auth: mockToken");
    ASSERT_EQ(data->getCommandData(), "mockToken");

    LOG_INFO("Calling zts.join()");
    zts.join();
    LOG_INFO("Done zts.join()");

    auto kvs = split(testAthenz::principalToken, ';');
    for (std::vector<std::string>::iterator itr = kvs.begin(); itr != kvs.end(); itr++) {
        auto kv = split(*itr, '=');
        if (kv[0] == "d") {
            ASSERT_EQ(kv[1], "pulsar.test2.tenant");
        } else if (kv[0] == "n") {
            ASSERT_EQ(kv[1], "service");
        }
    }
}

TEST(AuthPluginTest, testOauth2) {
    // test success get token from oauth2 server.
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "type": "client_credentials",
        "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
        "client_id": "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
        "client_secret": "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb",
        "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    int expectedTokenLength = 3379;
    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthOauth2::create(params);

    ASSERT_EQ(auth->getAuthMethodName(), "token");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData().length(), expectedTokenLength);
}

TEST(AuthPluginTest, testOauth2WrongSecret) {
    pulsar::AuthenticationDataPtr data;

    std::string params = R"({
    "type": "client_credentials",
    "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
    "client_id": "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
    "client_secret": "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ",
    "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthOauth2::create(params);
    ASSERT_EQ(auth->getAuthMethodName(), "token");
    ASSERT_EQ(auth->getAuthData(data), ResultAuthenticationError);
}

TEST(AuthPluginTest, testOauth2CredentialFile) {
    // test success get token from oauth2 server.
    pulsar::AuthenticationDataPtr data;
    const char* paramsTemplate = R"({
        "type": "client_credentials",
        "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
        "private_key": "%s/cpp_credentials_file.json",
        "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    char params[4096];
    int numWritten = snprintf(params, sizeof(params), paramsTemplate, TEST_CONF_DIR);
    ASSERT_TRUE(numWritten < sizeof(params));

    int expectedTokenLength = 3379;
    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthOauth2::create(params);
    ASSERT_EQ(auth->getAuthMethodName(), "token");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData().length(), expectedTokenLength);
}

TEST(AuthPluginTest, testOauth2RequestBody) {
    ParamMap params;
    params["issuer_url"] = "https://dev-kt-aa9ne.us.auth0.com";
    params["client_id"] = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x";
    params["client_secret"] = "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb";
    params["audience"] = "https://dev-kt-aa9ne.us.auth0.com/api/v2/";

    auto createExpectedResult = [&] {
        auto paramsCopy = params;
        paramsCopy.emplace("grant_type", "client_credentials");
        paramsCopy.erase("issuer_url");
        return paramsCopy;
    };

    const auto expectedResult1 = createExpectedResult();
    ClientCredentialFlow flow1(params);
    ASSERT_EQ(flow1.generateParamMap(), expectedResult1);

    params["scope"] = "test-scope";
    const auto expectedResult2 = createExpectedResult();
    ClientCredentialFlow flow2(params);
    ASSERT_EQ(flow2.generateParamMap(), expectedResult2);
}

TEST(AuthPluginTest, testInitialize) {
    std::string issuerUrl = "https://dev-kt-aa9ne.us.auth0.com";
    std::string expectedTokenEndPoint = issuerUrl + "/oauth/token";

    ParamMap params;
    params["issuer_url"] = issuerUrl;
    params["client_id"] = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x";
    params["client_secret"] = "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb";
    params["audience"] = "https://dev-kt-aa9ne.us.auth0.com/api/v2/";

    ClientCredentialFlow flow1(params);
    flow1.initialize();
    ASSERT_EQ(flow1.getTokenEndPoint(), expectedTokenEndPoint);

    params["issuer_url"] = issuerUrl + "/";
    ClientCredentialFlow flow2(params);
    flow2.initialize();
    ASSERT_EQ(flow2.getTokenEndPoint(), expectedTokenEndPoint);
}

TEST(AuthPluginTest, testOauth2Failure) {
    ParamMap params;
    auto addKeyValue = [&](const std::string& key, const std::string& value) {
        params[key] = value;
        LOG_INFO("Configure \"" << key << "\" to \"" << value << "\"");
    };

    auto createClient = [&]() -> Client {
        ClientConfiguration conf;
        conf.setAuth(AuthOauth2::create(params));
        return {"pulsar://localhost:6650", conf};
    };

    const std::string topic = "AuthPluginTest-testOauth2Failure";
    Producer producer;

    // No issuer_url
    auto client1 = createClient();
    ASSERT_EQ(client1.createProducer(topic, producer), ResultAuthenticationError);
    client1.close();

    // Invalid issuer_url
    addKeyValue("issuer_url", "hello");
    auto client2 = createClient();
    ASSERT_EQ(client2.createProducer(topic, producer), ResultAuthenticationError);
    client2.close();

    addKeyValue("issuer_url", "https://google.com");
    auto client3 = createClient();
    ASSERT_EQ(client3.createProducer(topic, producer), ResultAuthenticationError);
    client3.close();

    // No client id and secret
    addKeyValue("issuer_url", "https://dev-kt-aa9ne.us.auth0.com");
    auto client4 = createClient();
    ASSERT_EQ(client4.createProducer(topic, producer), ResultAuthenticationError);
    client4.close();

    // Invalid client_id and client_secret
    addKeyValue("client_id", "my_id");
    addKeyValue("client_secret", "my-secret");
    auto client5 = createClient();
    ASSERT_EQ(client5.createProducer(topic, producer), ResultAuthenticationError);
    client5.close();
}

TEST(AuthPluginTest, testInvalidPlugin) {
    Client client("pulsar://localhost:6650", ClientConfiguration{}.setAuth(AuthFactory::create("invalid")));
    Producer producer;
    ASSERT_EQ(ResultAuthenticationError, client.createProducer("my-topic", producer));
    client.close();
}

TEST(AuthPluginTest, testTlsConfigError) {
    Client client(serviceUrlTls, ClientConfiguration{}
                                     .setAuth(AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath))
                                     .setTlsTrustCertsFilePath("invalid"));
    Producer producer;
    ASSERT_EQ(ResultAuthenticationError, client.createProducer("my-topic", producer));
    client.close();
}

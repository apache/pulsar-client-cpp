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
#include <chrono>
#include <future>
#include <mutex>
#include <sstream>
#include <stdexcept>
#ifdef USE_ASIO
#include <asio.hpp>
#include <asio/ssl.hpp>
#else
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#endif
#include <thread>

#include "lib/AsioDefines.h"
#include "lib/Future.h"
#include "lib/Latch.h"
#include "lib/LogUtils.h"
#include "lib/Utils.h"
#include "lib/auth/AuthOauth2.h"
#include "lib/auth/InitialAuthData.h"
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
static const std::string brokerPublicKeyPath = TEST_CONF_DIR "/broker-cert.pem";
static const std::string brokerPrivateKeyPath = TEST_CONF_DIR "/broker-key.pem";

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

// ASIO::ip::tcp::iostream could call a virtual function during destruction, so the clang-tidy will fail by
// clang-analyzer-optin.cplusplus.VirtualCall. Here we write a simple stream to read lines from socket.
template <typename Stream>
class SocketStream {
   public:
    explicit SocketStream(Stream& stream) : stream_(stream) {}

    bool getline(std::string& line) {
        auto pos = buffer_.find('\n', bufferPos_);
        if (pos != std::string::npos) {
            line = buffer_.substr(bufferPos_, pos - bufferPos_);
            bufferPos_ = pos + 1;
            return true;
        }

        std::array<char, 1024> buffer;
        ASIO_ERROR error;
        auto length = stream_.read_some(ASIO::buffer(buffer.data(), buffer.size()), error);
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

    bool readBytes(size_t size, std::string& out) {
        while (buffer_.size() - bufferPos_ < size) {
            std::array<char, 1024> buffer;
            ASIO_ERROR error;
            auto length = stream_.read_some(ASIO::buffer(buffer.data(), buffer.size()), error);
            if (error == ASIO::error::eof) return false;
            if (error) return false;
            buffer_.append(buffer.data(), length);
        }
        out.assign(buffer_.data() + bufferPos_, size);
        bufferPos_ += size;
        return true;
    }

   private:
    Stream& stream_;
    std::string buffer_;
    size_t bufferPos_{0};
};

namespace testAthenz {
std::string principalToken;

void mockZTS(Latch& latch, int port) {
    LOG_INFO("-- MockZTS started");
    ASIO::io_context io;
    ASIO::ip::tcp::acceptor acceptor(io, ASIO::ip::tcp::endpoint(ASIO::ip::tcp::v4(), port));

    LOG_INFO("-- MockZTS waiting for connnection");
    latch.countdown();
    ASIO::ip::tcp::socket socket(io);
    acceptor.accept(socket);
    LOG_INFO("-- MockZTS got connection");

    std::string headerLine;
    SocketStream<ASIO::ip::tcp::socket> stream(socket);
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

namespace testOauth2Tls {
static const auto mockServerTimeout = std::chrono::seconds(10);

class MockOauth2Server {
   public:
    MockOauth2Server(const std::string& responseBody, const std::string& responseContentType, int listenPort,
                     bool requireClientCert = true)
        : responseBody_(responseBody),
          responseContentType_(responseContentType),
          acceptor_(io_, ASIO::ip::tcp::endpoint(ASIO::ip::tcp::v4(), static_cast<uint16_t>(listenPort))),
          sslCtx_(ASIO::ssl::context::sslv23) {
        sslCtx_.set_options(ASIO::ssl::context::default_workarounds | ASIO::ssl::context::no_sslv2 |
                            ASIO::ssl::context::no_sslv3);
        sslCtx_.use_certificate_chain_file(brokerPublicKeyPath);
        sslCtx_.use_private_key_file(brokerPrivateKeyPath, ASIO::ssl::context::pem);
        sslCtx_.load_verify_file(caPath);
        sslCtx_.set_verify_mode(requireClientCert
                                    ? (ASIO::ssl::verify_peer | ASIO::ssl::verify_fail_if_no_peer_cert)
                                    : ASIO::ssl::verify_none);
    }

    const std::string& request() const { return request_; }

    bool mockServe() {
        ASIO_ERROR error;
        auto socket = std::make_shared<ASIO::ip::tcp::socket>(io_);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            activeSocket_ = socket;
        }

        acceptor_.accept(*socket, error);
        if (error) {
            clearActiveSocket();
            return false;
        }

        ASIO::ssl::stream<ASIO::ip::tcp::socket&> sslStream(*socket, sslCtx_);
        sslStream.handshake(ASIO::ssl::stream_base::server, error);
        if (error || !readRequest(sslStream)) {
            clearActiveSocket();
            return false;
        }

        const std::string response = "HTTP/1.1 200 OK\r\nContent-Type: " + responseContentType_ +
                                     "\r\nContent-Length: " + std::to_string(responseBody_.size()) +
                                     "\r\nConnection: close\r\n\r\n" + responseBody_;
        ASIO::write(sslStream, ASIO::buffer(response.data(), response.size()), error);
        clearActiveSocket();
        if (error) return false;
        return true;
    }

    void stop() {
        ASIO_ERROR error;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (acceptor_.is_open()) {
                acceptor_.close(error);
            }
            if (activeSocket_ && activeSocket_->is_open()) {
                activeSocket_->cancel(error);
                activeSocket_->shutdown(ASIO::ip::tcp::socket::shutdown_both, error);
                activeSocket_->close(error);
            }
        }
        io_.stop();
    }

   private:
    void clearActiveSocket() {
        std::lock_guard<std::mutex> lock(mutex_);
        activeSocket_.reset();
    }

    bool readRequest(ASIO::ssl::stream<ASIO::ip::tcp::socket&>& sslStream) {
        SocketStream<ASIO::ssl::stream<ASIO::ip::tcp::socket&>> stream(sslStream);
        request_.clear();
        int contentLength = 0;
        const std::string prefix = "Content-Length:";
        std::string headerLine;
        while (stream.getline(headerLine)) {
            if (headerLine.empty()) {
                continue;
            }
            request_.append(headerLine).append("\n");
            if (headerLine.rfind(prefix, 0) == 0) {
                contentLength = std::stoi(headerLine.substr(prefix.size()));
            }
            if (headerLine == "\r") {
                break;
            }
        }
        if (headerLine != "\r") return false;

        if (contentLength > 0) {
            std::string body;
            if (!stream.readBytes(static_cast<size_t>(contentLength), body)) return false;
            request_ += body;
        }
        return true;
    }

    const std::string responseBody_;
    const std::string responseContentType_;
    std::string request_;

    ASIO::io_context io_;
    ASIO::ip::tcp::acceptor acceptor_;
    ASIO::ssl::context sslCtx_;
    std::shared_ptr<ASIO::ip::tcp::socket> activeSocket_;
    std::mutex mutex_;
};

static bool awaitMockServeResult(std::future<bool>& future, MockOauth2Server& server, std::thread& thread,
                                 const char* serverName) {
    if (future.wait_for(mockServerTimeout) != std::future_status::ready) {
        server.stop();
        if (thread.joinable()) {
            thread.join();
        }
        ADD_FAILURE() << serverName << " did not complete within "
                      << std::chrono::duration_cast<std::chrono::seconds>(mockServerTimeout).count()
                      << " seconds";
        return false;
    }

    const bool result = future.get();
    if (thread.joinable()) {
        thread.join();
    }
    return result;
}

}  // namespace testOauth2Tls

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
    params["tls_cert_file"] = "/path/to/cert.pem";
    params["tls_key_file"] = "/path/to/key.pem";

    auto createExpectedResult = [&] {
        auto paramsCopy = params;
        paramsCopy.emplace("grant_type", "client_credentials");
        paramsCopy.erase("issuer_url");
        paramsCopy.erase("tls_cert_file");
        paramsCopy.erase("tls_key_file");
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

TEST(AuthPluginTest, testOauth2TlsClientAuth) {
    const int tokenServerPort = 58081;
    const int wellKnownServerPort = 58082;
    const std::string tokenBody = R"({"access_token":"mockToken","expires_in":3600,"token_type":"Bearer"})";
    std::unique_ptr<testOauth2Tls::MockOauth2Server> tokenServer;
    try {
        tokenServer =
            std::make_unique<testOauth2Tls::MockOauth2Server>(tokenBody, "application/json", tokenServerPort);
    } catch (const std::exception& e) {
        FAIL() << "Failed to bind local mock token server: " << e.what();
    }

    std::promise<bool> tokenPromise;
    auto tokenFuture = tokenPromise.get_future();
    std::thread tokenThread(
        [&tokenServer, &tokenPromise]() { tokenPromise.set_value(tokenServer->mockServe()); });

    std::ostringstream wellKnownBody;
    wellKnownBody << R"({"token_endpoint":"https://localhost:)" << tokenServerPort << R"(/oauth/token"})";
    std::unique_ptr<testOauth2Tls::MockOauth2Server> wellKnownServer;
    try {
        wellKnownServer = std::make_unique<testOauth2Tls::MockOauth2Server>(
            wellKnownBody.str(), "application/json", wellKnownServerPort, false);
    } catch (const std::exception& e) {
        tokenThread.join();
        FAIL() << "Failed to bind local mock well-known server: " << e.what();
    }

    std::promise<bool> wellKnownPromise;
    auto wellKnownFuture = wellKnownPromise.get_future();
    std::thread wellKnownThread([&wellKnownServer, &wellKnownPromise]() {
        wellKnownPromise.set_value(wellKnownServer->mockServe());
    });

    ParamMap params;
    params["tokenEndpointAuthMethod"] = "tls_client_auth";
    params["issuer_url"] = "https://localhost:" + std::to_string(wellKnownServerPort);
    params["client_id"] = "test-client";
    params["tls_cert_file"] = clientPublicKeyPath;
    params["tls_key_file"] = clientPrivateKeyPath;

    AuthenticationDataPtr data =
        std::static_pointer_cast<AuthenticationDataProvider>(std::make_shared<InitialAuthData>(caPath));
    AuthenticationPtr auth = AuthOauth2::create(params);
    ASSERT_EQ(auth->getAuthData(data), ResultOk);
    ASSERT_TRUE(data->hasDataFromCommand());
    ASSERT_EQ(data->getCommandData(), "mockToken");

    ASSERT_TRUE(testOauth2Tls::awaitMockServeResult(wellKnownFuture, *wellKnownServer, wellKnownThread,
                                                    "Well-known mock server"));
    ASSERT_TRUE(
        testOauth2Tls::awaitMockServeResult(tokenFuture, *tokenServer, tokenThread, "Token mock server"));
    ASSERT_NE(wellKnownServer->request().find("GET /.well-known/openid-configuration "), std::string::npos);
    ASSERT_NE(tokenServer->request().find("POST /oauth/token "), std::string::npos);
    ASSERT_NE(tokenServer->request().find("grant_type=client_credentials"), std::string::npos);
}

TEST(AuthPluginTest, testOauth2TlsClientAuthWrongCert) {
    const int tokenServerPort = 58083;
    const int wellKnownServerPort = 58084;
    const std::string tokenBody = R"({"access_token":"mockToken","expires_in":3600,"token_type":"Bearer"})";

    std::unique_ptr<testOauth2Tls::MockOauth2Server> tokenServer;
    try {
        tokenServer =
            std::make_unique<testOauth2Tls::MockOauth2Server>(tokenBody, "application/json", tokenServerPort);
    } catch (const std::exception& e) {
        FAIL() << "Failed to bind local mock token server: " << e.what();
    }

    std::promise<bool> tokenPromise;
    auto tokenFuture = tokenPromise.get_future();
    std::thread tokenThread(
        [&tokenServer, &tokenPromise]() { tokenPromise.set_value(tokenServer->mockServe()); });

    std::ostringstream wellKnownBody;
    wellKnownBody << R"({"token_endpoint":"https://localhost:)" << tokenServerPort << R"(/oauth/token"})";
    std::unique_ptr<testOauth2Tls::MockOauth2Server> wellKnownServer;
    try {
        wellKnownServer = std::make_unique<testOauth2Tls::MockOauth2Server>(
            wellKnownBody.str(), "application/json", wellKnownServerPort, false);
    } catch (const std::exception& e) {
        tokenThread.join();
        FAIL() << "Failed to bind local mock well-known server: " << e.what();
    }

    std::promise<bool> wellKnownPromise;
    auto wellKnownFuture = wellKnownPromise.get_future();
    std::thread wellKnownThread([&wellKnownServer, &wellKnownPromise]() {
        wellKnownPromise.set_value(wellKnownServer->mockServe());
    });

    ParamMap params;
    params["tokenEndpointAuthMethod"] = "tls_client_auth";
    params["issuer_url"] = "https://localhost:" + std::to_string(wellKnownServerPort);
    params["client_id"] = "test-client";
    // set wrong cert and key
    params["tls_cert_file"] = TEST_CONF_DIR "/hn-verification/broker-cert.pem";
    params["tls_key_file"] = TEST_CONF_DIR "/hn-verification/broker-key.pem";

    AuthenticationDataPtr data =
        std::static_pointer_cast<AuthenticationDataProvider>(std::make_shared<InitialAuthData>(caPath));
    AuthenticationPtr auth = AuthOauth2::create(params);
    ASSERT_EQ(auth->getAuthData(data), ResultAuthenticationError);

    ASSERT_TRUE(testOauth2Tls::awaitMockServeResult(wellKnownFuture, *wellKnownServer, wellKnownThread,
                                                    "Well-known mock server"));
    ASSERT_FALSE(
        testOauth2Tls::awaitMockServeResult(tokenFuture, *tokenServer, tokenThread, "Token mock server"));
    ASSERT_NE(wellKnownServer->request().find("GET /.well-known/openid-configuration "), std::string::npos);
}

TEST(AuthPluginTest, testOauth2TlsClientAuthRequestBody) {
    ParamMap params;
    params["tokenEndpointAuthMethod"] = "tls_client_auth";
    params["issuer_url"] = "https://dev-kt-aa9ne.us.auth0.com";
    params["client_id"] = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x";
    params["audience"] = "https://dev-kt-aa9ne.us.auth0.com/api/v2/";
    params["tls_cert_file"] = "/path/to/cert.pem";
    params["tls_key_file"] = "/path/to/key.pem";

    auto createExpectedResult = [&] {
        auto paramsCopy = params;
        paramsCopy.emplace("grant_type", "client_credentials");
        paramsCopy.erase("tokenEndpointAuthMethod");
        paramsCopy.erase("issuer_url");
        paramsCopy.erase("tls_cert_file");
        paramsCopy.erase("tls_key_file");
        return paramsCopy;
    };

    const auto expectedResult1 = createExpectedResult();
    TlsClientAuthFlow flow1(params);
    ASSERT_EQ(flow1.generateParamMap(), expectedResult1);

    params["scope"] = "test-scope";
    const auto expectedResult2 = createExpectedResult();
    TlsClientAuthFlow flow2(params);
    ASSERT_EQ(flow2.generateParamMap(), expectedResult2);

    params.erase("client_id");
    auto expectedResult3 = expectedResult2;
    expectedResult3["client_id"] = TlsClientAuthFlow::DEFAULT_CLIENT_ID;
    TlsClientAuthFlow flow3(params);
    ASSERT_EQ(flow3.generateParamMap(), expectedResult3);

    params.erase("audience");
    auto expectedResult4 = expectedResult3;
    expectedResult4.erase("audience");
    TlsClientAuthFlow flow4(params);
    ASSERT_EQ(flow4.generateParamMap(), expectedResult4);
}

TEST(AuthPluginTest, testOauth2TlsClientAuthFailure) {
    ParamMap params;
    auto getAuthDataResult = [&]() -> Result {
        AuthenticationDataPtr data =
            std::static_pointer_cast<AuthenticationDataProvider>(std::make_shared<InitialAuthData>(caPath));
        AuthenticationPtr auth = AuthOauth2::create(params);
        return auth->getAuthData(data);
    };

    params["tokenEndpointAuthMethod"] = "tls_client_auth";
    params["tls_cert_file"] = clientPublicKeyPath;
    params["tls_key_file"] = clientPrivateKeyPath;

    // No issuer_url
    params.erase("issuer_url");
    ASSERT_EQ(getAuthDataResult(), ResultAuthenticationError);

    // Invalid issuer_url
    params["issuer_url"] = "hello";
    ASSERT_EQ(getAuthDataResult(), ResultAuthenticationError);

    // No cert and key
    params["issuer_url"] = "https://localhost:58086";
    params.erase("tls_cert_file");
    params.erase("tls_key_file");
    ASSERT_EQ(getAuthDataResult(), ResultAuthenticationError);

    // Only cert
    params["tls_cert_file"] = clientPublicKeyPath;
    params.erase("tls_key_file");
    ASSERT_EQ(getAuthDataResult(), ResultAuthenticationError);

    // Invalid cert and key
    params["tls_cert_file"] = TEST_CONF_DIR "/not-exist-cert.pem";
    params["tls_key_file"] = TEST_CONF_DIR "/not-exist-key.pem";
    ASSERT_EQ(getAuthDataResult(), ResultAuthenticationError);
}

TEST(AuthPluginTest, testOauth2UnknownTokenEndpointAuthMethod) {
    std::string params = R"({
        "type": "client_credentials",
        "tokenEndpointAuthMethod": "client_secret_get",
        "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
        "client_id": "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
        "client_secret": "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb",
        "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    LOG_INFO("PARAMS: " << params);
    ASSERT_THROW(AuthOauth2::create(params), std::invalid_argument);
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

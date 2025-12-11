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
#include <openssl/ssl.h>
#include <pulsar/Authentication.h>
#include <pulsar/Client.h>

#include <atomic>
#include <future>
#include <thread>

#include "lib/AsioDefines.h"
#include "lib/LogUtils.h"

#ifdef USE_ASIO
#include <asio.hpp>
#include <asio/ssl.hpp>
#else
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#endif

DECLARE_LOG_OBJECT()

#ifndef TEST_CONF_DIR
#error "TEST_CONF_DIR is not specified"
#endif

static const std::string caPath = TEST_CONF_DIR "/cacert.pem";
static const std::string clientPublicKeyPath = TEST_CONF_DIR "/client-cert.pem";
static const std::string clientPrivateKeyPath = TEST_CONF_DIR "/client-key.pem";

using namespace pulsar;

class MockTlsServer {
   public:
    MockTlsServer()
        : acceptor_(io_context_, ASIO::ip::tcp::endpoint(ASIO::ip::tcp::v4(), 0)),
          ctx_(ASIO::ssl::context::sslv23) {
        ctx_.set_options(ASIO::ssl::context::default_workarounds | ASIO::ssl::context::no_sslv2 |
                         ASIO::ssl::context::no_sslv3);

        ctx_.use_certificate_chain_file(clientPublicKeyPath);
        ctx_.use_private_key_file(clientPrivateKeyPath, ASIO::ssl::context::pem);
        ctx_.set_verify_mode(ASIO::ssl::context::verify_none);
    }

    int getPort() const { return acceptor_.local_endpoint().port(); }

    void setTls12Only() {
        SSL_CTX* ssl_ctx = ctx_.native_handle();
#if defined(TLS1_2_VERSION)
        SSL_CTX_set_min_proto_version(ssl_ctx, TLS1_2_VERSION);
        SSL_CTX_set_max_proto_version(ssl_ctx, TLS1_2_VERSION);
#else
        LOG_WARN("TLS 1.2 not supported by OpenSSL headers");
#endif
    }

    void setTls13Only() {
        SSL_CTX* ssl_ctx = ctx_.native_handle();
#if defined(TLS1_3_VERSION)
        SSL_CTX_set_min_proto_version(ssl_ctx, TLS1_3_VERSION);
        SSL_CTX_set_max_proto_version(ssl_ctx, TLS1_3_VERSION);
#else
        LOG_WARN("TLS 1.3 not supported by OpenSSL headers");
#endif
    }

    bool acceptAndHandshake() {
        auto socket = std::make_shared<ASIO::ip::tcp::socket>(io_context_);
        acceptor_.accept(*socket);

        ASIO::ssl::stream<ASIO::ip::tcp::socket&> ssl_stream(*socket, ctx_);

        ASIO_ERROR error;
        ssl_stream.handshake(ASIO::ssl::stream_base::server, error);

        if (error) {
            LOG_ERROR("Handshake failed: " << error.message());
            return false;
        }
        LOG_INFO("Handshake success!");
        return true;
    }

   private:
    ASIO::io_context io_context_;
    ASIO::ip::tcp::acceptor acceptor_;
    ASIO::ssl::context ctx_;
};

TEST(TlsNegotiationTest, testTls12) {
#if !defined(TLS1_2_VERSION)
    return;  // Skip if TLS 1.2 is not available
#endif

    MockTlsServer server;
    server.setTls12Only();
    int port = server.getPort();

    std::promise<bool> handshakePromise;
    auto handshakeFuture = handshakePromise.get_future();

    std::thread serverThread([&server, &handshakePromise]() {
        bool result = server.acceptAndHandshake();
        handshakePromise.set_value(result);
    });

    std::string serviceUrl = "pulsar+ssl://localhost:" + std::to_string(port);
    ClientConfiguration config;
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(true);  // Self-signed certs match
    config.setValidateHostName(false);

    Client client(serviceUrl, config);

    // Trigger connection by creating a producer.
    // It will fail to create producer because mock server doesn't speak Pulsar,
    // but we only care about the handshake.
    Producer producer;
    client.createProducerAsync("topic", [](Result, Producer) {});

    // Wait for handshake
    ASSERT_TRUE(handshakeFuture.get());

    serverThread.join();
    client.close();
}

TEST(TlsNegotiationTest, testTls13) {
#if !defined(TLS1_3_VERSION)
    LOG_INFO("Skipping TLS 1.3 test because OpenSSL does not support it");
    return;
#endif

    MockTlsServer server;
    server.setTls13Only();
    int port = server.getPort();

    std::promise<bool> handshakePromise;
    auto handshakeFuture = handshakePromise.get_future();

    std::thread serverThread([&server, &handshakePromise]() {
        bool result = server.acceptAndHandshake();
        handshakePromise.set_value(result);
    });

    std::string serviceUrl = "pulsar+ssl://localhost:" + std::to_string(port);
    ClientConfiguration config;
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(true);
    config.setValidateHostName(false);

    Client client(serviceUrl, config);

    client.createProducerAsync("topic", [](Result, Producer) {});

    ASSERT_TRUE(handshakeFuture.get());

    serverThread.join();
    client.close();
}

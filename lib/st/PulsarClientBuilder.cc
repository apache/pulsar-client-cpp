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
#include <pulsar/ClientConfiguration.h>
#include <pulsar/st/Client.h>

#include <exception>
#include <memory>
#include <string>
#include <utility>

#include "ClientImpl.h"

namespace pulsar::st {

namespace {

bool hasTlsScheme(const std::string& url) {
    return url.rfind("pulsar+ssl://", 0) == 0 || url.rfind("https://", 0) == 0;
}

}  // namespace

Expected<PulsarClient> PulsarClientBuilder::build() {
    if (serviceUrl_.empty()) {
        return unexpected(ResultInvalidUrl, "serviceUrl is required; set it on the builder");
    }
    if (tlsPolicy_.enabled && !hasTlsScheme(serviceUrl_)) {
        return unexpected(
            ResultInvalidConfiguration,
            "tlsPolicy.enabled requires a pulsar+ssl:// or https:// service URL, got: " + serviceUrl_);
    }

    // Map the scalable-topics policy structs onto the classic client
    // configuration: the underlying transport stack (connection pool, executor
    // pools, lookup) is reused as-is. Fields left unset keep the classic
    // defaults.
    pulsar::ClientConfiguration conf;
    if (authentication_) {
        conf.setAuth(authentication_);
    }
    const ConnectionPolicy& connection = connectionPolicy_;
    if (connection.connectionsPerBroker) {
        conf.setConnectionsPerBroker(*connection.connectionsPerBroker);
    }
    if (connection.connectionTimeout) {
        conf.setConnectionTimeout(static_cast<int>(connection.connectionTimeout->count()));
    }
    if (connection.operationTimeout) {
        conf.setOperationTimeoutMs(static_cast<int>(connection.operationTimeout->count()));
    }
    if (connection.keepAliveInterval) {
        conf.setKeepAliveIntervalInSeconds(static_cast<unsigned int>(connection.keepAliveInterval->count()));
    }
    if (connection.maxLookupRequests) {
        conf.setConcurrentLookupRequest(*connection.maxLookupRequests);
    }
    if (connection.maxLookupRedirects) {
        conf.setMaxLookupRedirects(*connection.maxLookupRedirects);
    }
    // TODO(scalable-topics): connection.maxConnectionIdleTime has no classic
    // ClientConfiguration counterpart; wire it up when lib/st manages its own
    // connection reaping.
    if (connection.listenerName) {
        conf.setListenerName(*connection.listenerName);
    }
    if (threadPolicy_.ioThreads) {
        conf.setIOThreads(*threadPolicy_.ioThreads);
    }
    if (threadPolicy_.messageListenerThreads) {
        conf.setMessageListenerThreads(*threadPolicy_.messageListenerThreads);
    }
    if (memoryPolicy_.limit) {
        conf.setMemoryLimit(memoryPolicy_.limit->bytes);
    }
    if (backoffPolicy_.initialBackoff) {
        conf.setInitialBackoffIntervalMs(static_cast<int>(backoffPolicy_.initialBackoff->count()));
    }
    if (backoffPolicy_.maxBackoff) {
        conf.setMaxBackoffIntervalMs(static_cast<int>(backoffPolicy_.maxBackoff->count()));
    }
    if (tlsPolicy_.trustCertsFilePath) {
        conf.setTlsTrustCertsFilePath(*tlsPolicy_.trustCertsFilePath);
    }
    if (tlsPolicy_.certificateFilePath) {
        conf.setTlsCertificateFilePath(*tlsPolicy_.certificateFilePath);
    }
    if (tlsPolicy_.privateKeyFilePath) {
        conf.setTlsPrivateKeyFilePath(*tlsPolicy_.privateKeyFilePath);
    }
    conf.setTlsAllowInsecureConnection(tlsPolicy_.allowInsecureConnection);
    conf.setValidateHostName(tlsPolicy_.validateHostname);

    try {
        auto classic = std::make_shared<pulsar::ClientImpl>(serviceUrl_, conf);
        classic->initialize();
        auto impl = std::make_shared<ClientImpl>(std::move(classic), transactionPolicy_);
        return PulsarClient(detail::ClientCore(std::move(impl)));
    } catch (const std::exception& e) {
        return unexpected(ResultInvalidUrl, std::string("failed to initialize the client: ") + e.what());
    }
}

}  // namespace pulsar::st

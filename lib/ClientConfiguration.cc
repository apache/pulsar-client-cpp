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
#include <chrono>
#include <stdexcept>

#include "ClientConfigurationImpl.h"
#include "auth/AuthOauth2.h"

namespace pulsar {

ClientConfiguration::ClientConfiguration() : impl_(std::make_shared<ClientConfigurationImpl>()) {}

ClientConfiguration::~ClientConfiguration() {}

ClientConfiguration::ClientConfiguration(const ClientConfiguration& x) : impl_(x.impl_) {}

ClientConfiguration& ClientConfiguration::operator=(const ClientConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

ClientConfiguration& ClientConfiguration::setMemoryLimit(uint64_t memoryLimitBytes) {
    impl_->memoryLimit = memoryLimitBytes;
    return *this;
}

uint64_t ClientConfiguration::getMemoryLimit() const { return impl_->memoryLimit; }

ClientConfiguration& ClientConfiguration::setConnectionsPerBroker(int connectionsPerBroker) {
    if (connectionsPerBroker <= 0) {
        throw std::invalid_argument("connectionsPerBroker should be greater than 0");
    }
    impl_->connectionsPerBroker = connectionsPerBroker;
    return *this;
}

int ClientConfiguration::getConnectionsPerBroker() const { return impl_->connectionsPerBroker; }

ClientConfiguration& ClientConfiguration::setAuth(const AuthenticationPtr& authentication) {
    impl_->authenticationPtr = authentication;
    return *this;
}

Authentication& ClientConfiguration::getAuth() const { return *impl_->authenticationPtr; }

const AuthenticationPtr& ClientConfiguration::getAuthPtr() const { return impl_->authenticationPtr; }

ClientConfiguration& ClientConfiguration::setOperationTimeoutSeconds(int timeout) {
    impl_->operationTimeout = std::chrono::seconds(timeout);
    return *this;
}

int ClientConfiguration::getOperationTimeoutSeconds() const {
    return std::chrono::duration_cast<std::chrono::seconds>(impl_->operationTimeout).count();
}

ClientConfiguration& ClientConfiguration::setIOThreads(int threads) {
    impl_->ioThreads = threads;
    return *this;
}

int ClientConfiguration::getIOThreads() const { return impl_->ioThreads; }

ClientConfiguration& ClientConfiguration::setMessageListenerThreads(int threads) {
    impl_->messageListenerThreads = threads;
    return *this;
}

int ClientConfiguration::getMessageListenerThreads() const { return impl_->messageListenerThreads; }

ClientConfiguration& ClientConfiguration::setUseTls(bool useTls) {
    impl_->useTls = useTls;
    return *this;
}

bool ClientConfiguration::isUseTls() const { return impl_->useTls; }

ClientConfiguration& ClientConfiguration::setValidateHostName(bool validateHostName) {
    impl_->validateHostName = validateHostName;
    return *this;
}

bool ClientConfiguration::isValidateHostName() const { return impl_->validateHostName; }

ClientConfiguration& ClientConfiguration::setTlsPrivateKeyFilePath(const std::string& filePath) {
    impl_->tlsPrivateKeyFilePath = filePath;
    return *this;
}

const std::string& ClientConfiguration::getTlsPrivateKeyFilePath() const {
    return impl_->tlsPrivateKeyFilePath;
}

ClientConfiguration& ClientConfiguration::setTlsCertificateFilePath(const std::string& filePath) {
    impl_->tlsCertificateFilePath = filePath;
    return *this;
}

const std::string& ClientConfiguration::getTlsCertificateFilePath() const {
    return impl_->tlsCertificateFilePath;
}

ClientConfiguration& ClientConfiguration::setTlsTrustCertsFilePath(const std::string& filePath) {
    impl_->tlsTrustCertsFilePath = filePath;
    return *this;
}

const std::string& ClientConfiguration::getTlsTrustCertsFilePath() const {
    return impl_->tlsTrustCertsFilePath;
}

ClientConfiguration& ClientConfiguration::setTlsAllowInsecureConnection(bool allowInsecure) {
    impl_->tlsAllowInsecureConnection = allowInsecure;
    return *this;
}

bool ClientConfiguration::isTlsAllowInsecureConnection() const { return impl_->tlsAllowInsecureConnection; }

ClientConfiguration& ClientConfiguration::setConcurrentLookupRequest(int concurrentLookupRequest) {
    impl_->concurrentLookupRequest = concurrentLookupRequest;
    return *this;
}

ClientConfiguration& ClientConfiguration::setProxyServiceUrl(const std::string& proxyServiceUrl) {
    impl_->proxyServiceUrl = proxyServiceUrl;
    return *this;
}

const std::string& ClientConfiguration::getProxyServiceUrl() const { return impl_->proxyServiceUrl; }

ClientConfiguration& ClientConfiguration::setProxyProtocol(ClientConfiguration::ProxyProtocol proxyProtocol) {
    impl_->proxyProtocol = proxyProtocol;
    return *this;
}

ClientConfiguration::ProxyProtocol ClientConfiguration::getProxyProtocol() const {
    return impl_->proxyProtocol;
}

int ClientConfiguration::getConcurrentLookupRequest() const { return impl_->concurrentLookupRequest; }

ClientConfiguration& ClientConfiguration::setMaxLookupRedirects(int maxLookupRedirects) {
    impl_->maxLookupRedirects = maxLookupRedirects;
    return *this;
}

int ClientConfiguration::getMaxLookupRedirects() const { return impl_->maxLookupRedirects; }

ClientConfiguration& ClientConfiguration::setInitialBackoffIntervalMs(int initialBackoffIntervalMs) {
    impl_->initialBackoffIntervalMs = initialBackoffIntervalMs;
    return *this;
}

int ClientConfiguration::getInitialBackoffIntervalMs() const { return impl_->initialBackoffIntervalMs; }

ClientConfiguration& ClientConfiguration::setMaxBackoffIntervalMs(int maxBackoffIntervalMs) {
    impl_->maxBackoffIntervalMs = maxBackoffIntervalMs;
    return *this;
}

int ClientConfiguration::getMaxBackoffIntervalMs() const { return impl_->maxBackoffIntervalMs; }

ClientConfiguration& ClientConfiguration::setLogger(LoggerFactory* loggerFactory) {
    impl_->loggerFactory.reset(loggerFactory);
    return *this;
}

ClientConfiguration& ClientConfiguration::setStatsIntervalInSeconds(
    const unsigned int& statsIntervalInSeconds) {
    impl_->statsIntervalInSeconds = statsIntervalInSeconds;
    return *this;
}

const unsigned int& ClientConfiguration::getStatsIntervalInSeconds() const {
    return impl_->statsIntervalInSeconds;
}

ClientConfiguration& ClientConfiguration::setPartititionsUpdateInterval(unsigned int intervalInSeconds) {
    impl_->partitionsUpdateInterval = intervalInSeconds;
    return *this;
}

unsigned int ClientConfiguration::getPartitionsUpdateInterval() const {
    return impl_->partitionsUpdateInterval;
}

ClientConfiguration& ClientConfiguration::setListenerName(const std::string& listenerName) {
    impl_->listenerName = listenerName;
    return *this;
}

const std::string& ClientConfiguration::getListenerName() const { return impl_->listenerName; }

ClientConfiguration& ClientConfiguration::setConnectionTimeout(int timeoutMs) {
    impl_->connectionTimeoutMs = timeoutMs;
    return *this;
}

int ClientConfiguration::getConnectionTimeout() const { return impl_->connectionTimeoutMs; }

ClientConfiguration& ClientConfiguration::setKeepAliveIntervalInSeconds(
    unsigned int keepAliveIntervalInSeconds) {
    impl_->keepAliveIntervalInSeconds = keepAliveIntervalInSeconds;
    return *this;
}

unsigned int ClientConfiguration::getKeepAliveIntervalInSeconds() const {
    return impl_->keepAliveIntervalInSeconds;
}

ClientConfiguration& ClientConfiguration::setDescription(const std::string& description) {
    if (description.length() > 64) {
        throw std::invalid_argument("The description length exceeds 64");
    }
    impl_->description = description;
    return *this;
}

const std::string& ClientConfiguration::getDescription() const noexcept { return impl_->description; }

}  // namespace pulsar

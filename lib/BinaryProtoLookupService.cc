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
#include "BinaryProtoLookupService.h"

#include "ClientConnection.h"
#include "ConnectionPool.h"
#include "LogUtils.h"
#include "NamespaceName.h"
#include "ServiceNameResolver.h"
#include "TopicName.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

auto BinaryProtoLookupService::getBroker(const TopicName& topicName) -> LookupResultFuture {
    return findBroker(serviceNameResolver_.resolveHost(), false, topicName.toString(), 0);
}

auto BinaryProtoLookupService::findBroker(const std::string& address, bool authoritative,
                                          const std::string& topic, size_t redirectCount)
    -> LookupResultFuture {
    LOG_DEBUG("find broker from " << address << ", authoritative: " << authoritative << ", topic: " << topic
                                  << ", redirect count: " << redirectCount);
    auto promise = std::make_shared<Promise<Result, LookupResult>>();
    if (maxLookupRedirects_ > 0 && redirectCount > maxLookupRedirects_) {
        LOG_ERROR("Too many lookup request redirects on topic " << topic << ", configured limit is "
                                                                << maxLookupRedirects_);
        promise->setFailed(ResultTooManyLookupRequestException);
        return promise->getFuture();
    }

    // NOTE: we can use move capture for topic since C++14
    cnxPool_.getConnectionAsync(address).addListener([this, promise, topic, address, authoritative,
                                                      redirectCount](Result result,
                                                                     const ClientConnectionWeakPtr& weakCnx) {
        if (result != ResultOk) {
            promise->setFailed(result);
            return;
        }
        auto cnx = weakCnx.lock();
        if (!cnx) {
            LOG_ERROR("Connection to " << address << " is expired before lookup");
            promise->setFailed(ResultNotConnected);
            return;
        }
        auto lookupPromise = std::make_shared<LookupDataResultPromise>();
        cnx->newTopicLookup(topic, authoritative, listenerName_, newRequestId(), lookupPromise);
        lookupPromise->getFuture().addListener([this, cnx, promise, topic, address, redirectCount](
                                                   Result result, const LookupDataResultPtr& data) {
            if (result != ResultOk || !data) {
                LOG_ERROR("Lookup failed for " << topic << ", result " << result);
                promise->setFailed(result);
                return;
            }

            const auto responseBrokerAddress =
                (serviceNameResolver_.useTls() ? data->getBrokerUrlTls() : data->getBrokerUrl());
            if (data->isRedirect()) {
                LOG_DEBUG("Lookup request is for " << topic << " redirected to " << responseBrokerAddress);
                findBroker(responseBrokerAddress, data->isAuthoritative(), topic, redirectCount + 1)
                    .addListener([promise](Result result, const LookupResult& value) {
                        if (result == ResultOk) {
                            promise->setValue(value);
                        } else {
                            promise->setFailed(result);
                        }
                    });
            } else {
                LOG_DEBUG("Lookup response for " << topic << ", lookup-broker-url " << data->getBrokerUrl());
                if (data->shouldProxyThroughServiceUrl()) {
                    // logicalAddress is the proxy's address, we should still connect through proxy
                    promise->setValue({responseBrokerAddress, address});
                } else {
                    promise->setValue({responseBrokerAddress, responseBrokerAddress});
                }
            }
        });
    });
    return promise->getFuture();
}

/*
 * @param    topicName topic to get number of partitions.
 *
 */
Future<Result, LookupDataResultPtr> BinaryProtoLookupService::getPartitionMetadataAsync(
    const TopicNamePtr& topicName) {
    LookupDataResultPromisePtr promise = std::make_shared<LookupDataResultPromise>();
    if (!topicName) {
        promise->setFailed(ResultInvalidTopicName);
        return promise->getFuture();
    }
    std::string lookupName = topicName->toString();
    const auto address = serviceNameResolver_.resolveHost();
    cnxPool_.getConnectionAsync(address, address)
        .addListener(std::bind(&BinaryProtoLookupService::sendPartitionMetadataLookupRequest, this,
                               lookupName, std::placeholders::_1, std::placeholders::_2, promise));
    return promise->getFuture();
}

void BinaryProtoLookupService::sendPartitionMetadataLookupRequest(const std::string& topicName, Result result,
                                                                  const ClientConnectionWeakPtr& clientCnx,
                                                                  LookupDataResultPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(result);
        return;
    }
    LookupDataResultPromisePtr lookupPromise = std::make_shared<LookupDataResultPromise>();
    ClientConnectionPtr conn = clientCnx.lock();
    uint64_t requestId = newRequestId();
    conn->newPartitionedMetadataLookup(topicName, requestId, lookupPromise);
    lookupPromise->getFuture().addListener(std::bind(&BinaryProtoLookupService::handlePartitionMetadataLookup,
                                                     this, topicName, std::placeholders::_1,
                                                     std::placeholders::_2, clientCnx, promise));
}

void BinaryProtoLookupService::handlePartitionMetadataLookup(const std::string& topicName, Result result,
                                                             LookupDataResultPtr data,
                                                             const ClientConnectionWeakPtr& clientCnx,
                                                             LookupDataResultPromisePtr promise) {
    if (data) {
        LOG_DEBUG("PartitionMetadataLookup response for " << topicName << ", lookup-broker-url "
                                                          << data->getBrokerUrl());
        promise->setValue(data);
    } else {
        LOG_DEBUG("PartitionMetadataLookup failed for " << topicName << ", result " << result);
        promise->setFailed(result);
    }
}

uint64_t BinaryProtoLookupService::newRequestId() {
    Lock lock(mutex_);
    return ++requestIdGenerator_;
}

Future<Result, NamespaceTopicsPtr> BinaryProtoLookupService::getTopicsOfNamespaceAsync(
    const NamespaceNamePtr& nsName, CommandGetTopicsOfNamespace_Mode mode) {
    NamespaceTopicsPromisePtr promise = std::make_shared<Promise<Result, NamespaceTopicsPtr>>();
    if (!nsName) {
        promise->setFailed(ResultInvalidTopicName);
        return promise->getFuture();
    }
    std::string namespaceName = nsName->toString();
    cnxPool_.getConnectionAsync(serviceNameResolver_.resolveHost())
        .addListener(std::bind(&BinaryProtoLookupService::sendGetTopicsOfNamespaceRequest, this,
                               namespaceName, mode, std::placeholders::_1, std::placeholders::_2, promise));
    return promise->getFuture();
}

Future<Result, SchemaInfo> BinaryProtoLookupService::getSchema(const TopicNamePtr& topicName,
                                                               const std::string& version) {
    GetSchemaPromisePtr promise = std::make_shared<Promise<Result, SchemaInfo>>();

    if (!topicName) {
        promise->setFailed(ResultInvalidTopicName);
        return promise->getFuture();
    }
    cnxPool_.getConnectionAsync(serviceNameResolver_.resolveHost())
        .addListener(std::bind(&BinaryProtoLookupService::sendGetSchemaRequest, this, topicName->toString(),
                               version, std::placeholders::_1, std::placeholders::_2, promise));

    return promise->getFuture();
}

void BinaryProtoLookupService::sendGetSchemaRequest(const std::string& topicName, const std::string& version,
                                                    Result result, const ClientConnectionWeakPtr& clientCnx,
                                                    GetSchemaPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(result);
        return;
    }

    ClientConnectionPtr conn = clientCnx.lock();
    uint64_t requestId = newRequestId();
    LOG_DEBUG("sendGetSchemaRequest. requestId: " << requestId << " topicName: " << topicName
                                                  << " version: " << version);

    conn->newGetSchema(topicName, version, requestId)
        .addListener([promise](Result result, SchemaInfo schemaInfo) {
            if (result != ResultOk) {
                promise->setFailed(result);
                return;
            }
            promise->setValue(schemaInfo);
        });
}

void BinaryProtoLookupService::sendGetTopicsOfNamespaceRequest(const std::string& nsName,
                                                               CommandGetTopicsOfNamespace_Mode mode,
                                                               Result result,
                                                               const ClientConnectionWeakPtr& clientCnx,
                                                               NamespaceTopicsPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(result);
        return;
    }

    ClientConnectionPtr conn = clientCnx.lock();
    uint64_t requestId = newRequestId();
    LOG_DEBUG("sendGetTopicsOfNamespaceRequest. requestId: " << requestId << " nsName: " << nsName);
    conn->newGetTopicsOfNamespace(nsName, mode, requestId)
        .addListener(std::bind(&BinaryProtoLookupService::getTopicsOfNamespaceListener, this,
                               std::placeholders::_1, std::placeholders::_2, promise));
}

void BinaryProtoLookupService::getTopicsOfNamespaceListener(Result result, NamespaceTopicsPtr topicsPtr,
                                                            NamespaceTopicsPromisePtr promise) {
    if (result != ResultOk) {
        promise->setFailed(ResultLookupError);
        return;
    }

    promise->setValue(topicsPtr);
}

}  // namespace pulsar

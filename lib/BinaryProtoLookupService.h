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
#ifndef _PULSAR_BINARY_LOOKUP_SERVICE_HEADER_
#define _PULSAR_BINARY_LOOKUP_SERVICE_HEADER_

#include <pulsar/Authentication.h>
#include <pulsar/ClientConfiguration.h>
#include <pulsar/Schema.h>

#include <mutex>

#include "LookupService.h"

namespace pulsar {
class ClientConnection;
using ClientConnectionWeakPtr = std::weak_ptr<ClientConnection>;
class ConnectionPool;
class LookupDataResult;
class ServiceNameResolver;
using NamespaceTopicsPromisePtr = std::shared_ptr<Promise<Result, NamespaceTopicsPtr>>;
using GetSchemaPromisePtr = std::shared_ptr<Promise<Result, SchemaInfo>>;

class PULSAR_PUBLIC BinaryProtoLookupService : public LookupService {
   public:
    BinaryProtoLookupService(ServiceNameResolver& serviceNameResolver, ConnectionPool& pool,
                             const ClientConfiguration& clientConfiguration)
        : serviceNameResolver_(serviceNameResolver),
          cnxPool_(pool),
          listenerName_(clientConfiguration.getListenerName()),
          maxLookupRedirects_(clientConfiguration.getMaxLookupRedirects()) {}

    LookupResultFuture getBroker(const TopicName& topicName) override;

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName) override;

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(
        const NamespaceNamePtr& nsName, CommandGetTopicsOfNamespace_Mode mode) override;

    Future<Result, SchemaInfo> getSchema(const TopicNamePtr& topicName, const std::string& version) override;

   protected:
    // Mark findBroker as protected to make it accessible from test.
    LookupResultFuture findBroker(const std::string& address, bool authoritative, const std::string& topic,
                                  size_t redirectCount);

   private:
    std::mutex mutex_;
    uint64_t requestIdGenerator_ = 0;

    ServiceNameResolver& serviceNameResolver_;
    ConnectionPool& cnxPool_;
    std::string listenerName_;
    const int32_t maxLookupRedirects_;

    void sendPartitionMetadataLookupRequest(const std::string& topicName, Result result,
                                            const ClientConnectionWeakPtr& clientCnx,
                                            LookupDataResultPromisePtr promise);

    void handlePartitionMetadataLookup(const std::string& topicName, Result result, LookupDataResultPtr data,
                                       const ClientConnectionWeakPtr& clientCnx,
                                       LookupDataResultPromisePtr promise);

    void sendGetTopicsOfNamespaceRequest(const std::string& nsName, CommandGetTopicsOfNamespace_Mode mode,
                                         Result result, const ClientConnectionWeakPtr& clientCnx,
                                         NamespaceTopicsPromisePtr promise);

    void sendGetSchemaRequest(const std::string& topicName, const std::string& version, Result result,
                              const ClientConnectionWeakPtr& clientCnx, GetSchemaPromisePtr promise);

    void getTopicsOfNamespaceListener(Result result, NamespaceTopicsPtr topicsPtr,
                                      NamespaceTopicsPromisePtr promise);

    uint64_t newRequestId();
};
typedef std::shared_ptr<BinaryProtoLookupService> BinaryProtoLookupServicePtr;
}  // namespace pulsar

#endif  //_PULSAR_BINARY_LOOKUP_SERVICE_HEADER_

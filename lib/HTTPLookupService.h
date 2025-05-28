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
#ifndef PULSAR_CPP_HTTPLOOKUPSERVICE_H
#define PULSAR_CPP_HTTPLOOKUPSERVICE_H

#include <cstdint>

#include "ClientImpl.h"
#include "LookupService.h"
#include "Url.h"

namespace pulsar {

class ServiceNameResolver;
using NamespaceTopicsPromise = Promise<Result, NamespaceTopicsPtr>;
using NamespaceTopicsPromisePtr = std::shared_ptr<NamespaceTopicsPromise>;
using GetSchemaPromise = Promise<Result, SchemaInfo>;

class HTTPLookupService : public LookupService, public std::enable_shared_from_this<HTTPLookupService> {
    enum RequestType : uint8_t
    {
        Lookup,
        PartitionMetaData
    };

    typedef Promise<Result, LookupDataResultPtr> LookupPromise;

    ExecutorServiceProviderPtr executorProvider_;
    ServiceNameResolver serviceNameResolver_;
    AuthenticationPtr authenticationPtr_;
    int lookupTimeoutInSeconds_;
    const int maxLookupRedirects_;
    std::string tlsPrivateFilePath_;
    std::string tlsCertificateFilePath_;
    std::string tlsTrustCertsFilePath_;
    bool isUseTls_;
    bool tlsAllowInsecure_;
    bool tlsValidateHostname_;

    static LookupDataResultPtr parsePartitionData(const std::string&);
    static LookupDataResultPtr parseLookupData(const std::string&);
    static NamespaceTopicsPtr parseNamespaceTopicsData(const std::string&);

    void handleLookupHTTPRequest(const LookupPromise&, const std::string&, RequestType);
    void handleNamespaceTopicsHTTPRequest(const NamespaceTopicsPromise& promise,
                                          const std::string& completeUrl);
    void handleGetSchemaHTTPRequest(const GetSchemaPromise& promise, const std::string& completeUrl);

    Result sendHTTPRequest(const std::string& completeUrl, std::string& responseData);

    Result sendHTTPRequest(const std::string& completeUrl, std::string& responseData, long& responseCode);

   public:
    HTTPLookupService(const std::string&, const ClientConfiguration&, const AuthenticationPtr&);

    LookupResultFuture getBroker(const TopicName& topicName) override;

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr&) override;

    Future<Result, SchemaInfo> getSchema(const TopicNamePtr& topicName, const std::string& version) override;

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(
        const NamespaceNamePtr& nsName, CommandGetTopicsOfNamespace_Mode mode) override;

    ServiceNameResolver& getServiceNameResolver() override { return serviceNameResolver_; }
};
}  // namespace pulsar

#endif  // PULSAR_CPP_HTTPLOOKUPSERVICE_H

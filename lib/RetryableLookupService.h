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
#pragma once

#include <string>

#include "LookupDataResult.h"
#include "LookupService.h"
#include "NamespaceName.h"
#include "RetryableOperationCache.h"
#include "TopicName.h"

namespace pulsar {

class RetryableLookupService : public LookupService {
   private:
    friend class LookupServiceTest;
    struct PassKey {
        explicit PassKey() {}
    };

   public:
    template <typename... Args>
    explicit RetryableLookupService(PassKey, Args&&... args)
        : RetryableLookupService(std::forward<Args>(args)...) {}

    void close() override {
        lookupCache_->close();
        partitionLookupCache_->close();
        namespaceLookupCache_->close();
        getSchemaCache_->close();
    }

    template <typename... Args>
    static std::shared_ptr<RetryableLookupService> create(Args&&... args) {
        return std::make_shared<RetryableLookupService>(PassKey{}, std::forward<Args>(args)...);
    }

    LookupResultFuture getBroker(const TopicName& topicName) override {
        return lookupCache_->run("get-broker-" + topicName.toString(),
                                 [this, topicName] { return lookupService_->getBroker(topicName); });
    }

    Future<Error, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName) override {
        return partitionLookupCache_->run(
            "get-partition-metadata-" + topicName->toString(),
            [this, topicName] { return lookupService_->getPartitionMetadataAsync(topicName); });
    }

    Future<Error, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(
        const NamespaceNamePtr& nsName, CommandGetTopicsOfNamespace_Mode mode) override {
        return namespaceLookupCache_->run(
            "get-topics-of-namespace-" + nsName->toString() + "-" + std::to_string(mode),
            [this, nsName, mode] { return lookupService_->getTopicsOfNamespaceAsync(nsName, mode); });
    }

    Future<Error, SchemaInfo> getSchema(const TopicNamePtr& topicName, const std::string& version) override {
        return getSchemaCache_->run("get-schema" + topicName->toString(), [this, topicName, version] {
            return lookupService_->getSchema(topicName, version);
        });
    }

    ServiceNameResolver& getServiceNameResolver() override {
        return lookupService_->getServiceNameResolver();
    }

   private:
    const std::shared_ptr<LookupService> lookupService_;
    RetryableOperationCachePtr<LookupResult, Error> lookupCache_;
    RetryableOperationCachePtr<LookupDataResultPtr, Error> partitionLookupCache_;
    RetryableOperationCachePtr<NamespaceTopicsPtr, Error> namespaceLookupCache_;
    RetryableOperationCachePtr<SchemaInfo, Error> getSchemaCache_;

    RetryableLookupService(std::shared_ptr<LookupService> lookupService, TimeDuration timeout,
                           ExecutorServiceProviderPtr executorProvider)
        : lookupService_(std::move(lookupService)),
          lookupCache_(RetryableOperationCache<LookupResult, Error>::create(executorProvider, timeout)),
          partitionLookupCache_(
              RetryableOperationCache<LookupDataResultPtr, Error>::create(executorProvider, timeout)),
          namespaceLookupCache_(
              RetryableOperationCache<NamespaceTopicsPtr, Error>::create(executorProvider, timeout)),
          getSchemaCache_(RetryableOperationCache<SchemaInfo, Error>::create(executorProvider, timeout)) {}
};

}  // namespace pulsar

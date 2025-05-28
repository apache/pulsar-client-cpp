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
#ifndef LIB_CLIENTIMPL_H_
#define LIB_CLIENTIMPL_H_

#include <pulsar/Client.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "ConnectionPool.h"
#include "Future.h"
#include "LookupDataResult.h"
#include "MemoryLimitController.h"
#include "ProtoApiEnums.h"
#include "SynchronizedHashMap.h"

namespace pulsar {

class PulsarFriend;
class ClientImpl;
typedef std::shared_ptr<ClientImpl> ClientImplPtr;
typedef std::weak_ptr<ClientImpl> ClientImplWeakPtr;

class ReaderImpl;
typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;
typedef std::weak_ptr<ReaderImpl> ReaderImplWeakPtr;

class TableViewImpl;
typedef std::shared_ptr<TableViewImpl> TableViewImplPtr;

class ConsumerImplBase;
typedef std::weak_ptr<ConsumerImplBase> ConsumerImplBaseWeakPtr;

class ClientConnection;
using ClientConnectionPtr = std::shared_ptr<ClientConnection>;

class LookupService;
using LookupServicePtr = std::shared_ptr<LookupService>;

class ProducerImplBase;
using ProducerImplBaseWeakPtr = std::weak_ptr<ProducerImplBase>;
class ConsumerImplBase;
using ConsumerImplBaseWeakPtr = std::weak_ptr<ConsumerImplBase>;
class TopicName;
using TopicNamePtr = std::shared_ptr<TopicName>;

using NamespaceTopicsPtr = std::shared_ptr<std::vector<std::string>>;
using GetConnectionFuture = Future<Result, ClientConnectionPtr>;

std::string generateRandomName();

class ClientImpl : public std::enable_shared_from_this<ClientImpl> {
   public:
    ClientImpl(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration);
    virtual ~ClientImpl();

    /**
     * @param autoDownloadSchema When it is true, Before creating a producer, it will try to get the schema
     * that exists for the topic.
     */
    void createProducerAsync(const std::string& topic, const ProducerConfiguration& conf,
                             const CreateProducerCallback& callback, bool autoDownloadSchema = false);

    void subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                        const ConsumerConfiguration& conf, const SubscribeCallback& callback);

    void subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                        const ConsumerConfiguration& conf, const SubscribeCallback& callback);

    void subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                 const ConsumerConfiguration& conf, const SubscribeCallback& callback);

    void createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf, const ReaderCallback& callback);

    void createTableViewAsync(const std::string& topic, const TableViewConfiguration& conf,
                              const TableViewCallback& callback);

    void getPartitionsForTopicAsync(const std::string& topic, const GetPartitionsCallback& callback);

    // Use virtual method to test
    virtual GetConnectionFuture getConnection(const std::string& redirectedClusterURI,
                                              const std::string& topic, size_t key);

    GetConnectionFuture connect(const std::string& redirectedClusterURI, const std::string& logicalAddress,
                                size_t key);

    void closeAsync(const CloseCallback& callback);
    void shutdown();

    MemoryLimitController& getMemoryLimitController();

    uint64_t newProducerId();
    uint64_t newConsumerId();
    uint64_t newRequestId();

    uint64_t getNumberOfProducers();
    uint64_t getNumberOfConsumers();

    const ClientConfiguration& getClientConfig() const;

    const ClientConfiguration& conf() const;
    ExecutorServiceProviderPtr getIOExecutorProvider();
    ExecutorServiceProviderPtr getListenerExecutorProvider();
    ExecutorServiceProviderPtr getPartitionListenerExecutorProvider();
    LookupServicePtr getLookup(const std::string& redirectedClusterURI = "");

    void cleanupProducer(ProducerImplBase* address) { producers_.remove(address); }

    void cleanupConsumer(ConsumerImplBase* address) { consumers_.remove(address); }

    std::shared_ptr<std::atomic<uint64_t>> getRequestIdGenerator() const { return requestIdGenerator_; }

    ConnectionPool& getConnectionPool() noexcept { return pool_; }
    uint64_t getLookupCount() { return lookupCount_; }

    static std::chrono::nanoseconds getOperationTimeout(const ClientConfiguration& clientConfiguration);

    friend class PulsarFriend;

   private:
    void handleCreateProducer(Result result, const LookupDataResultPtr& partitionMetadata,
                              const TopicNamePtr& topicName, const ProducerConfiguration& conf,
                              const CreateProducerCallback& callback);

    void handleSubscribe(Result result, const LookupDataResultPtr& partitionMetadata,
                         const TopicNamePtr& topicName, const std::string& consumerName,
                         ConsumerConfiguration conf, const SubscribeCallback& callback);

    void handleReaderMetadataLookup(Result result, const LookupDataResultPtr& partitionMetadata,
                                    const TopicNamePtr& topicName, const MessageId& startMessageId,
                                    const ReaderConfiguration& conf, const ReaderCallback& callback);

    void handleGetPartitions(Result result, const LookupDataResultPtr& partitionMetadata,
                             const TopicNamePtr& topicName, const GetPartitionsCallback& callback);

    void handleProducerCreated(Result result, const ProducerImplBaseWeakPtr& producerWeakPtr,
                               const CreateProducerCallback& callback, const ProducerImplBasePtr& producer);
    void handleConsumerCreated(Result result, const ConsumerImplBaseWeakPtr& consumerWeakPtr,
                               const SubscribeCallback& callback, const ConsumerImplBasePtr& consumer);

    typedef std::shared_ptr<int> SharedInt;

    void handleClose(Result result, const SharedInt& remaining, const ResultCallback& callback);

    void createPatternMultiTopicsConsumer(Result result, const NamespaceTopicsPtr& topics,
                                          const std::string& regexPattern,
                                          CommandGetTopicsOfNamespace_Mode mode,
                                          const std::string& consumerName, const ConsumerConfiguration& conf,
                                          const SubscribeCallback& callback);

    const std::string& getPhysicalAddress(const std::string& redirectedClusterURI,
                                          const std::string& logicalAddress);

    LookupServicePtr createLookup(const std::string& serviceUrl);

    static std::string getClientVersion(const ClientConfiguration& clientConfiguration);

    enum State : uint8_t
    {
        Open,
        Closing,
        Closed
    };

    std::mutex mutex_;

    State state_;
    ClientConfiguration clientConfiguration_;
    MemoryLimitController memoryLimitController_;

    ExecutorServiceProviderPtr ioExecutorProvider_;
    ExecutorServiceProviderPtr listenerExecutorProvider_;
    ExecutorServiceProviderPtr partitionListenerExecutorProvider_;

    LookupServicePtr lookupServicePtr_;
    std::unordered_map<std::string, LookupServicePtr> redirectedClusterLookupServicePtrs_;
    ConnectionPool pool_;

    uint64_t producerIdGenerator_;
    uint64_t consumerIdGenerator_;
    std::shared_ptr<std::atomic<uint64_t>> requestIdGenerator_{std::make_shared<std::atomic<uint64_t>>(0)};

    SynchronizedHashMap<ProducerImplBase*, ProducerImplBaseWeakPtr> producers_;
    SynchronizedHashMap<ConsumerImplBase*, ConsumerImplBaseWeakPtr> consumers_;

    std::atomic<Result> closingError;
    std::atomic<bool> useProxy_;
    std::atomic<uint64_t> lookupCount_;

    friend class Client;
};
} /* namespace pulsar */

#endif /* LIB_CLIENTIMPL_H_ */

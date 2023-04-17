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
#include <memory>

#include "ConnectionPool.h"
#include "Future.h"
#include "LookupDataResult.h"
#include "MemoryLimitController.h"
#include "ProtoApiEnums.h"
#include "ServiceNameResolver.h"
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
using ClientConnectionWeakPtr = std::weak_ptr<ClientConnection>;

class LookupService;
using LookupServicePtr = std::shared_ptr<LookupService>;

class ProducerImplBase;
using ProducerImplBaseWeakPtr = std::weak_ptr<ProducerImplBase>;
class ConsumerImplBase;
using ConsumerImplBaseWeakPtr = std::weak_ptr<ConsumerImplBase>;
class TopicName;
using TopicNamePtr = std::shared_ptr<TopicName>;

using NamespaceTopicsPtr = std::shared_ptr<std::vector<std::string>>;

std::string generateRandomName();

class ClientImpl : public std::enable_shared_from_this<ClientImpl> {
   public:
    ClientImpl(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
               bool poolConnections);
    ~ClientImpl();

    /**
     * @param autoDownloadSchema When it is true, Before creating a producer, it will try to get the schema
     * that exists for the topic.
     */
    void createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                             CreateProducerCallback callback, bool autoDownloadSchema = false);

    void subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

    void subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

    void subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                 const ConsumerConfiguration& conf, SubscribeCallback callback);

    void createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf, ReaderCallback callback);

    void createTableViewAsync(const std::string& topic, const TableViewConfiguration& conf,
                              TableViewCallback callback);

    void getPartitionsForTopicAsync(const std::string& topic, GetPartitionsCallback callback);

    Future<Result, ClientConnectionWeakPtr> getConnection(const std::string& topic);

    void closeAsync(CloseCallback callback);
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
    LookupServicePtr getLookup();

    void cleanupProducer(ProducerImplBase* address) { producers_.remove(address); }

    void cleanupConsumer(ConsumerImplBase* address) { consumers_.remove(address); }

    std::shared_ptr<std::atomic<uint64_t>> getRequestIdGenerator() const { return requestIdGenerator_; }

    friend class PulsarFriend;

   private:
    void handleCreateProducer(const Result result, const LookupDataResultPtr partitionMetadata,
                              TopicNamePtr topicName, ProducerConfiguration conf,
                              CreateProducerCallback callback);

    void handleSubscribe(const Result result, const LookupDataResultPtr partitionMetadata,
                         TopicNamePtr topicName, const std::string& consumerName, ConsumerConfiguration conf,
                         SubscribeCallback callback);

    void handleReaderMetadataLookup(const Result result, const LookupDataResultPtr partitionMetadata,
                                    TopicNamePtr topicName, MessageId startMessageId,
                                    ReaderConfiguration conf, ReaderCallback callback);

    void handleGetPartitions(const Result result, const LookupDataResultPtr partitionMetadata,
                             TopicNamePtr topicName, GetPartitionsCallback callback);

    void handleProducerCreated(Result result, ProducerImplBaseWeakPtr producerWeakPtr,
                               CreateProducerCallback callback, ProducerImplBasePtr producer);
    void handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerWeakPtr,
                               SubscribeCallback callback, ConsumerImplBasePtr consumer);

    typedef std::shared_ptr<int> SharedInt;

    void handleClose(Result result, SharedInt remaining, ResultCallback callback);

    void createPatternMultiTopicsConsumer(const Result result, const NamespaceTopicsPtr topics,
                                          const std::string& regexPattern,
                                          CommandGetTopicsOfNamespace_Mode mode,
                                          const std::string& consumerName, const ConsumerConfiguration& conf,
                                          SubscribeCallback callback);

    static std::string getClientVersion(const ClientConfiguration& clientConfiguration);

    enum State
    {
        Open,
        Closing,
        Closed
    };

    std::mutex mutex_;

    State state_;
    ServiceNameResolver serviceNameResolver_;
    ClientConfiguration clientConfiguration_;
    MemoryLimitController memoryLimitController_;

    ExecutorServiceProviderPtr ioExecutorProvider_;
    ExecutorServiceProviderPtr listenerExecutorProvider_;
    ExecutorServiceProviderPtr partitionListenerExecutorProvider_;

    LookupServicePtr lookupServicePtr_;
    ConnectionPool pool_;

    uint64_t producerIdGenerator_;
    uint64_t consumerIdGenerator_;
    std::shared_ptr<std::atomic<uint64_t>> requestIdGenerator_{std::make_shared<std::atomic<uint64_t>>(0)};

    SynchronizedHashMap<ProducerImplBase*, ProducerImplBaseWeakPtr> producers_;
    SynchronizedHashMap<ConsumerImplBase*, ConsumerImplBaseWeakPtr> consumers_;

    std::atomic<Result> closingError;

    friend class Client;
};
} /* namespace pulsar */

#endif /* LIB_CLIENTIMPL_H_ */

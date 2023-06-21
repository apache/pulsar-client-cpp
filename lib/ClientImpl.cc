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
#include "ClientImpl.h"

#include <pulsar/ClientConfiguration.h>
#include <pulsar/Version.h>

#include <random>
#include <sstream>

#include "BinaryProtoLookupService.h"
#include "ClientConfigurationImpl.h"
#include "Commands.h"
#include "ConsumerImpl.h"
#include "ConsumerInterceptors.h"
#include "ExecutorService.h"
#include "HTTPLookupService.h"
#include "LogUtils.h"
#include "MultiTopicsConsumerImpl.h"
#include "PartitionedProducerImpl.h"
#include "PatternMultiTopicsConsumerImpl.h"
#include "ProducerImpl.h"
#include "ProducerInterceptors.h"
#include "ReaderImpl.h"
#include "RetryableLookupService.h"
#include "TableViewImpl.h"
#include "TimeUtils.h"
#include "TopicName.h"

#ifdef PULSAR_USE_BOOST_REGEX
#include <boost/regex.hpp>
#define PULSAR_REGEX_NAMESPACE boost
#else
#include <regex>
#define PULSAR_REGEX_NAMESPACE std
#endif

DECLARE_LOG_OBJECT()

namespace pulsar {

static const char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                 '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
static std::uniform_int_distribution<> hexDigitsDist(0, sizeof(hexDigits) - 1);
static std::mt19937 randomEngine =
    std::mt19937(std::chrono::high_resolution_clock::now().time_since_epoch().count());

std::string generateRandomName() {
    const int randomNameLength = 10;

    std::string randomName;
    for (int i = 0; i < randomNameLength; ++i) {
        randomName += hexDigits[hexDigitsDist(randomEngine)];
    }
    return randomName;
}

typedef std::unique_lock<std::mutex> Lock;

typedef std::vector<std::string> StringList;

ClientImpl::ClientImpl(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
                       bool poolConnections)
    : mutex_(),
      state_(Open),
      serviceNameResolver_(serviceUrl),
      clientConfiguration_(ClientConfiguration(clientConfiguration).setUseTls(serviceNameResolver_.useTls())),
      memoryLimitController_(clientConfiguration.getMemoryLimit()),
      ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(clientConfiguration_.getIOThreads())),
      listenerExecutorProvider_(
          std::make_shared<ExecutorServiceProvider>(clientConfiguration_.getMessageListenerThreads())),
      partitionListenerExecutorProvider_(
          std::make_shared<ExecutorServiceProvider>(clientConfiguration_.getMessageListenerThreads())),
      pool_(clientConfiguration_, ioExecutorProvider_, clientConfiguration_.getAuthPtr(), poolConnections,
            ClientImpl::getClientVersion(clientConfiguration)),
      producerIdGenerator_(0),
      consumerIdGenerator_(0),
      closingError(ResultOk) {
    std::unique_ptr<LoggerFactory> loggerFactory = clientConfiguration_.impl_->takeLogger();
    if (!loggerFactory) {
        // Use default simple console logger
        loggerFactory.reset(new ConsoleLoggerFactory);
    }
    LogUtils::setLoggerFactory(std::move(loggerFactory));

    LookupServicePtr underlyingLookupServicePtr;
    if (serviceNameResolver_.useHttp()) {
        LOG_DEBUG("Using HTTP Lookup");
        underlyingLookupServicePtr = std::make_shared<HTTPLookupService>(
            std::ref(serviceNameResolver_), std::cref(clientConfiguration_),
            std::cref(clientConfiguration_.getAuthPtr()));
    } else {
        LOG_DEBUG("Using Binary Lookup");
        underlyingLookupServicePtr = std::make_shared<BinaryProtoLookupService>(
            std::ref(serviceNameResolver_), std::ref(pool_), std::cref(clientConfiguration_));
    }

    lookupServicePtr_ = RetryableLookupService::create(
        underlyingLookupServicePtr, clientConfiguration_.getOperationTimeoutSeconds(), ioExecutorProvider_);
}

ClientImpl::~ClientImpl() { shutdown(); }

const ClientConfiguration& ClientImpl::conf() const { return clientConfiguration_; }

MemoryLimitController& ClientImpl::getMemoryLimitController() { return memoryLimitController_; }

ExecutorServiceProviderPtr ClientImpl::getIOExecutorProvider() { return ioExecutorProvider_; }

ExecutorServiceProviderPtr ClientImpl::getListenerExecutorProvider() { return listenerExecutorProvider_; }

ExecutorServiceProviderPtr ClientImpl::getPartitionListenerExecutorProvider() {
    return partitionListenerExecutorProvider_;
}

LookupServicePtr ClientImpl::getLookup() { return lookupServicePtr_; }

void ClientImpl::createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                                     CreateProducerCallback callback, bool autoDownloadSchema) {
    if (conf.isChunkingEnabled() && conf.getBatchingEnabled()) {
        throw std::invalid_argument("Batching and chunking of messages can't be enabled together");
    }
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, Producer());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Producer());
            return;
        }
    }

    if (autoDownloadSchema) {
        auto self = shared_from_this();
        lookupServicePtr_->getSchema(topicName).addListener(
            [self, topicName, callback](Result res, SchemaInfo topicSchema) {
                if (res != ResultOk) {
                    callback(res, Producer());
                    return;
                }
                ProducerConfiguration conf;
                conf.setSchema(topicSchema);
                self->lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
                    std::bind(&ClientImpl::handleCreateProducer, self, std::placeholders::_1,
                              std::placeholders::_2, topicName, conf, callback));
            });
    } else {
        lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
            std::bind(&ClientImpl::handleCreateProducer, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, topicName, conf, callback));
    }
}

void ClientImpl::handleCreateProducer(const Result result, const LookupDataResultPtr partitionMetadata,
                                      TopicNamePtr topicName, ProducerConfiguration conf,
                                      CreateProducerCallback callback) {
    if (!result) {
        ProducerImplBasePtr producer;

        auto interceptors = std::make_shared<ProducerInterceptors>(conf.getInterceptors());

        try {
            if (partitionMetadata->getPartitions() > 0) {
                producer = std::make_shared<PartitionedProducerImpl>(
                    shared_from_this(), topicName, partitionMetadata->getPartitions(), conf, interceptors);
            } else {
                producer = std::make_shared<ProducerImpl>(shared_from_this(), *topicName, conf, interceptors);
            }
        } catch (const std::runtime_error& e) {
            LOG_ERROR("Failed to create producer: " << e.what());
            callback(ResultConnectError, {});
            return;
        }
        producer->getProducerCreatedFuture().addListener(
            std::bind(&ClientImpl::handleProducerCreated, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, callback, producer));
        producer->start();
    } else {
        LOG_ERROR("Error Checking/Getting Partition Metadata while creating producer on "
                  << topicName->toString() << " -- " << result);
        callback(result, Producer());
    }
}

void ClientImpl::handleProducerCreated(Result result, ProducerImplBaseWeakPtr producerBaseWeakPtr,
                                       CreateProducerCallback callback, ProducerImplBasePtr producer) {
    if (result == ResultOk) {
        auto pair = producers_.emplace(producer.get(), producer);
        if (!pair.second) {
            auto existingProducer = pair.first->second.lock();
            LOG_ERROR("Unexpected existing producer at the same address: "
                      << pair.first->first << ", producer: "
                      << (existingProducer ? existingProducer->getProducerName() : "(null)"));
            callback(ResultUnknownError, {});
            return;
        }
        callback(result, Producer(producer));
    } else {
        callback(result, {});
    }
}

void ClientImpl::createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                                   const ReaderConfiguration& conf, ReaderCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, Reader());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Reader());
            return;
        }
    }

    MessageId msgId(startMessageId);
    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleReaderMetadataLookup, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, msgId, conf, callback));
}

void ClientImpl::createTableViewAsync(const std::string& topic, const TableViewConfiguration& conf,
                                      TableViewCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, TableView());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, TableView());
            return;
        }
    }

    TableViewImplPtr tableViewPtr =
        std::make_shared<TableViewImpl>(shared_from_this(), topicName->toString(), conf);
    tableViewPtr->start().addListener([callback](Result result, TableViewImplPtr tableViewImplPtr) {
        if (result == ResultOk) {
            callback(result, TableView{tableViewImplPtr});
        } else {
            callback(result, {});
        }
    });
}

void ClientImpl::handleReaderMetadataLookup(const Result result, const LookupDataResultPtr partitionMetadata,
                                            TopicNamePtr topicName, MessageId startMessageId,
                                            ReaderConfiguration conf, ReaderCallback callback) {
    if (result != ResultOk) {
        LOG_ERROR("Error Checking/Getting Partition Metadata while creating readeron "
                  << topicName->toString() << " -- " << result);
        callback(result, Reader());
        return;
    }

    ReaderImplPtr reader;
    try {
        reader.reset(new ReaderImpl(shared_from_this(), topicName->toString(),
                                    partitionMetadata->getPartitions(), conf,
                                    getListenerExecutorProvider()->get(), callback));
    } catch (const std::runtime_error& e) {
        LOG_ERROR("Failed to create reader: " << e.what());
        callback(ResultConnectError, {});
        return;
    }
    ConsumerImplBasePtr consumer = reader->getConsumer();
    auto self = shared_from_this();
    reader->start(startMessageId, [this, self](const ConsumerImplBaseWeakPtr& weakConsumerPtr) {
        auto consumer = weakConsumerPtr.lock();
        if (consumer) {
            auto pair = consumers_.emplace(consumer.get(), consumer);
            if (!pair.second) {
                auto existingConsumer = pair.first->second.lock();
                LOG_ERROR("Unexpected existing consumer at the same address: "
                          << pair.first->first
                          << ", consumer: " << (existingConsumer ? existingConsumer->getName() : "(null)"));
            }
        } else {
            LOG_ERROR("Unexpected case: the consumer is somehow expired");
        }
    });
}

void ClientImpl::subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                         const ConsumerConfiguration& conf, SubscribeCallback callback) {
    TopicNamePtr topicNamePtr = TopicName::get(regexPattern);

    Lock lock(mutex_);
    if (state_ != Open) {
        lock.unlock();
        callback(ResultAlreadyClosed, Consumer());
        return;
    } else {
        lock.unlock();
        if (!topicNamePtr) {
            LOG_ERROR("Topic pattern not valid: " << regexPattern);
            callback(ResultInvalidTopicName, Consumer());
            return;
        }
    }

    if (TopicName::containsDomain(regexPattern)) {
        LOG_WARN("Ignore invalid domain: "
                 << topicNamePtr->getDomain()
                 << ", use the RegexSubscriptionMode parameter to set the topic type");
    }

    CommandGetTopicsOfNamespace_Mode mode;
    auto regexSubscriptionMode = conf.getRegexSubscriptionMode();
    switch (regexSubscriptionMode) {
        case PersistentOnly:
            mode = CommandGetTopicsOfNamespace_Mode_PERSISTENT;
            break;
        case NonPersistentOnly:
            mode = CommandGetTopicsOfNamespace_Mode_NON_PERSISTENT;
            break;
        case AllTopics:
            mode = CommandGetTopicsOfNamespace_Mode_ALL;
            break;
        default:
            LOG_ERROR("RegexSubscriptionMode not valid: " << regexSubscriptionMode);
            callback(ResultInvalidConfiguration, Consumer());
            return;
    }

    lookupServicePtr_->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName(), mode)
        .addListener(std::bind(&ClientImpl::createPatternMultiTopicsConsumer, shared_from_this(),
                               std::placeholders::_1, std::placeholders::_2, regexPattern, mode,
                               subscriptionName, conf, callback));
}

void ClientImpl::createPatternMultiTopicsConsumer(const Result result, const NamespaceTopicsPtr topics,
                                                  const std::string& regexPattern,
                                                  CommandGetTopicsOfNamespace_Mode mode,
                                                  const std::string& subscriptionName,
                                                  const ConsumerConfiguration& conf,
                                                  SubscribeCallback callback) {
    if (result == ResultOk) {
        ConsumerImplBasePtr consumer;

        PULSAR_REGEX_NAMESPACE::regex pattern(TopicName::removeDomain(regexPattern));

        NamespaceTopicsPtr matchTopics =
            PatternMultiTopicsConsumerImpl::topicsPatternFilter(*topics, pattern);

        auto interceptors = std::make_shared<ConsumerInterceptors>(conf.getInterceptors());

        consumer = std::make_shared<PatternMultiTopicsConsumerImpl>(shared_from_this(), regexPattern, mode,
                                                                    *matchTopics, subscriptionName, conf,
                                                                    lookupServicePtr_, interceptors);

        consumer->getConsumerCreatedFuture().addListener(
            std::bind(&ClientImpl::handleConsumerCreated, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, callback, consumer));
        consumer->start();
    } else {
        LOG_ERROR("Error Getting topicsOfNameSpace while createPatternMultiTopicsConsumer:  " << result);
        callback(result, Consumer());
    }
}

void ClientImpl::subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                                const ConsumerConfiguration& conf, SubscribeCallback callback) {
    TopicNamePtr topicNamePtr;

    Lock lock(mutex_);
    if (state_ != Open) {
        lock.unlock();
        callback(ResultAlreadyClosed, Consumer());
        return;
    } else {
        if (!topics.empty() && !(topicNamePtr = MultiTopicsConsumerImpl::topicNamesValid(topics))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Consumer());
            return;
        }
    }
    lock.unlock();

    if (topicNamePtr) {
        std::string randomName = generateRandomName();
        std::stringstream consumerTopicNameStream;
        consumerTopicNameStream << topicNamePtr->toString() << "-TopicsConsumerFakeName-" << randomName;
        topicNamePtr = TopicName::get(consumerTopicNameStream.str());
    }

    auto interceptors = std::make_shared<ConsumerInterceptors>(conf.getInterceptors());

    ConsumerImplBasePtr consumer = std::make_shared<MultiTopicsConsumerImpl>(
        shared_from_this(), topics, subscriptionName, topicNamePtr, conf, lookupServicePtr_, interceptors);

    consumer->getConsumerCreatedFuture().addListener(std::bind(&ClientImpl::handleConsumerCreated,
                                                               shared_from_this(), std::placeholders::_1,
                                                               std::placeholders::_2, callback, consumer));
    consumer->start();
}

void ClientImpl::subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                                const ConsumerConfiguration& conf, SubscribeCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, Consumer());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Consumer());
            return;
        } else if (conf.isReadCompacted() && (topicName->getDomain().compare("persistent") != 0 ||
                                              (conf.getConsumerType() != ConsumerExclusive &&
                                               conf.getConsumerType() != ConsumerFailover))) {
            lock.unlock();
            callback(ResultInvalidConfiguration, Consumer());
            return;
        }
    }

    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleSubscribe, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, subscriptionName, conf, callback));
}

void ClientImpl::handleSubscribe(const Result result, const LookupDataResultPtr partitionMetadata,
                                 TopicNamePtr topicName, const std::string& subscriptionName,
                                 ConsumerConfiguration conf, SubscribeCallback callback) {
    if (result == ResultOk) {
        // generate random name if not supplied by the customer.
        if (conf.getConsumerName().empty()) {
            conf.setConsumerName(generateRandomName());
        }
        ConsumerImplBasePtr consumer;
        auto interceptors = std::make_shared<ConsumerInterceptors>(conf.getInterceptors());

        try {
            if (partitionMetadata->getPartitions() > 0) {
                if (conf.getReceiverQueueSize() == 0) {
                    LOG_ERROR("Can't use partitioned topic if the queue size is 0.");
                    callback(ResultInvalidConfiguration, Consumer());
                    return;
                }
                consumer = std::make_shared<MultiTopicsConsumerImpl>(
                    shared_from_this(), topicName, partitionMetadata->getPartitions(), subscriptionName, conf,
                    lookupServicePtr_, interceptors);
            } else {
                auto consumerImpl = std::make_shared<ConsumerImpl>(shared_from_this(), topicName->toString(),
                                                                   subscriptionName, conf,
                                                                   topicName->isPersistent(), interceptors);
                consumerImpl->setPartitionIndex(topicName->getPartitionIndex());
                consumer = consumerImpl;
            }
        } catch (const std::runtime_error& e) {
            LOG_ERROR("Failed to create consumer: " << e.what());
            callback(ResultConnectError, {});
            return;
        }
        consumer->getConsumerCreatedFuture().addListener(
            std::bind(&ClientImpl::handleConsumerCreated, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, callback, consumer));
        consumer->start();
    } else {
        LOG_ERROR("Error Checking/Getting Partition Metadata while Subscribing on " << topicName->toString()
                                                                                    << " -- " << result);
        callback(result, Consumer());
    }
}

void ClientImpl::handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                       SubscribeCallback callback, ConsumerImplBasePtr consumer) {
    if (result == ResultOk) {
        auto pair = consumers_.emplace(consumer.get(), consumer);
        if (!pair.second) {
            auto existingConsumer = pair.first->second.lock();
            LOG_ERROR("Unexpected existing consumer at the same address: "
                      << pair.first->first
                      << ", consumer: " << (existingConsumer ? existingConsumer->getName() : "(null)"));
            callback(ResultUnknownError, {});
            return;
        }
        callback(result, Consumer(consumer));
    } else {
        // In order to be compatible with the current broker error code confusion.
        // https://github.com/apache/pulsar/blob/cd2aa550d0fe4e72b5ff88c4f6c1c2795b3ff2cd/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/BrokerServiceException.java#L240-L241
        if (result == ResultProducerBusy) {
            LOG_ERROR("Failed to create consumer: SubscriptionName cannot be empty.");
            callback(ResultInvalidConfiguration, {});
        } else {
            callback(result, {});
        }
    }
}

Future<Result, ClientConnectionWeakPtr> ClientImpl::getConnection(const std::string& topic) {
    Promise<Result, ClientConnectionWeakPtr> promise;

    const auto topicNamePtr = TopicName::get(topic);
    if (!topicNamePtr) {
        LOG_ERROR("Unable to parse topic - " << topic);
        promise.setFailed(ResultInvalidTopicName);
        return promise.getFuture();
    }

    auto self = shared_from_this();
    lookupServicePtr_->getBroker(*topicNamePtr)
        .addListener([this, self, promise](Result result, const LookupService::LookupResult& data) {
            if (result != ResultOk) {
                promise.setFailed(result);
                return;
            }
            pool_.getConnectionAsync(data.logicalAddress, data.physicalAddress)
                .addListener([promise](Result result, const ClientConnectionWeakPtr& weakCnx) {
                    if (result == ResultOk) {
                        promise.setValue(weakCnx);
                    } else {
                        promise.setFailed(result);
                    }
                });
        });

    return promise.getFuture();
}

void ClientImpl::handleGetPartitions(const Result result, const LookupDataResultPtr partitionMetadata,
                                     TopicNamePtr topicName, GetPartitionsCallback callback) {
    if (result != ResultOk) {
        LOG_ERROR("Error getting topic partitions metadata: " << result);
        callback(result, StringList());
        return;
    }

    StringList partitions;

    if (partitionMetadata->getPartitions() > 0) {
        for (unsigned int i = 0; i < partitionMetadata->getPartitions(); i++) {
            partitions.push_back(topicName->getTopicPartitionName(i));
        }
    } else {
        partitions.push_back(topicName->toString());
    }

    callback(ResultOk, partitions);
}

void ClientImpl::getPartitionsForTopicAsync(const std::string& topic, GetPartitionsCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, StringList());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, StringList());
            return;
        }
    }
    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleGetPartitions, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, callback));
}

void ClientImpl::closeAsync(CloseCallback callback) {
    if (state_ != Open) {
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }
    // Set the state to Closing so that no producers could get added
    state_ = Closing;

    memoryLimitController_.close();

    auto producers = producers_.move();
    auto consumers = consumers_.move();

    SharedInt numberOfOpenHandlers = std::make_shared<int>(producers.size() + consumers.size());
    LOG_INFO("Closing Pulsar client with " << producers.size() << " producers and " << consumers.size()
                                           << " consumers");

    for (auto&& kv : producers) {
        ProducerImplBasePtr producer = kv.second.lock();
        if (producer && !producer->isClosed()) {
            producer->closeAsync(std::bind(&ClientImpl::handleClose, shared_from_this(),
                                           std::placeholders::_1, numberOfOpenHandlers, callback));
        } else {
            // Since the connection is already closed
            (*numberOfOpenHandlers)--;
        }
    }

    for (auto&& kv : consumers) {
        ConsumerImplBasePtr consumer = kv.second.lock();
        if (consumer && !consumer->isClosed()) {
            consumer->closeAsync(std::bind(&ClientImpl::handleClose, shared_from_this(),
                                           std::placeholders::_1, numberOfOpenHandlers, callback));
        } else {
            // Since the connection is already closed
            (*numberOfOpenHandlers)--;
        }
    }

    if (*numberOfOpenHandlers == 0 && callback) {
        handleClose(ResultOk, numberOfOpenHandlers, callback);
    }
}

void ClientImpl::handleClose(Result result, SharedInt numberOfOpenHandlers, ResultCallback callback) {
    Result expected = ResultOk;
    if (!closingError.compare_exchange_strong(expected, result)) {
        LOG_DEBUG("Tried to updated closingError, but already set to "
                  << expected << ". This means multiple errors have occurred while closing the client");
    }

    if (*numberOfOpenHandlers > 0) {
        --(*numberOfOpenHandlers);
    }
    if (*numberOfOpenHandlers == 0) {
        Lock lock(mutex_);
        if (state_ == Closed) {
            LOG_DEBUG("Client is already shutting down, possible race condition in handleClose");
            return;
        } else {
            state_ = Closed;
            lock.unlock();
        }

        LOG_DEBUG("Shutting down producers and consumers for client");
        // handleClose() is called in ExecutorService's event loop, while shutdown() tried to wait the event
        // loop exits. So here we use another thread to call shutdown().
        auto self = shared_from_this();
        std::thread shutdownTask{[this, self, callback] {
            shutdown();
            if (callback) {
                if (closingError != ResultOk) {
                    LOG_DEBUG(
                        "Problem in closing client, could not close one or more consumers or producers");
                }
                callback(closingError);
            }
        }};
        shutdownTask.detach();
    }
}

void ClientImpl::shutdown() {
    auto producers = producers_.move();
    auto consumers = consumers_.move();

    for (auto&& kv : producers) {
        ProducerImplBasePtr producer = kv.second.lock();
        if (producer) {
            producer->shutdown();
        }
    }

    for (auto&& kv : consumers) {
        ConsumerImplBasePtr consumer = kv.second.lock();
        if (consumer) {
            consumer->shutdown();
        }
    }

    if (producers.size() + consumers.size() > 0) {
        LOG_DEBUG(producers.size() << " producers and " << consumers.size()
                                   << " consumers have been shutdown.");
    }
    if (!pool_.close()) {
        // pool_ has already been closed. It means shutdown() has been called before.
        return;
    }
    LOG_DEBUG("ConnectionPool is closed");

    // 500ms as the timeout is long enough because ExecutorService::close calls io_service::stop() internally
    // and waits until io_service::run() in another thread returns, which should be as soon as possible after
    // stop() is called.
    TimeoutProcessor<std::chrono::milliseconds> timeoutProcessor{500};

    timeoutProcessor.tik();
    ioExecutorProvider_->close(timeoutProcessor.getLeftTimeout());
    timeoutProcessor.tok();
    LOG_DEBUG("ioExecutorProvider_ is closed");

    timeoutProcessor.tik();
    listenerExecutorProvider_->close(timeoutProcessor.getLeftTimeout());
    timeoutProcessor.tok();
    LOG_DEBUG("listenerExecutorProvider_ is closed");

    timeoutProcessor.tik();
    partitionListenerExecutorProvider_->close(timeoutProcessor.getLeftTimeout());
    timeoutProcessor.tok();
    LOG_DEBUG("partitionListenerExecutorProvider_ is closed");
}

uint64_t ClientImpl::newProducerId() {
    Lock lock(mutex_);
    return producerIdGenerator_++;
}

uint64_t ClientImpl::newConsumerId() {
    Lock lock(mutex_);
    return consumerIdGenerator_++;
}

uint64_t ClientImpl::newRequestId() { return (*requestIdGenerator_)++; }

uint64_t ClientImpl::getNumberOfProducers() {
    uint64_t numberOfAliveProducers = 0;
    producers_.forEachValue([&numberOfAliveProducers](const ProducerImplBaseWeakPtr& producer) {
        const auto& producerImpl = producer.lock();
        if (producerImpl) {
            numberOfAliveProducers += producerImpl->getNumberOfConnectedProducer();
        }
    });
    return numberOfAliveProducers;
}

uint64_t ClientImpl::getNumberOfConsumers() {
    uint64_t numberOfAliveConsumers = 0;
    consumers_.forEachValue([&numberOfAliveConsumers](const ConsumerImplBaseWeakPtr& consumer) {
        const auto consumerImpl = consumer.lock();
        if (consumerImpl) {
            numberOfAliveConsumers += consumerImpl->getNumberOfConnectedConsumer();
        }
    });
    return numberOfAliveConsumers;
}

const ClientConfiguration& ClientImpl::getClientConfig() const { return clientConfiguration_; }

std::string ClientImpl::getClientVersion(const ClientConfiguration& clientConfiguration) {
    std::ostringstream oss;
    oss << "Pulsar-CPP-v" << PULSAR_VERSION_STR;
    if (!clientConfiguration.getDescription().empty()) {
        oss << "-" << clientConfiguration.getDescription();
    }
    return oss.str();
}

} /* namespace pulsar */

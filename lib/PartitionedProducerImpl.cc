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
#include "PartitionedProducerImpl.h"

#include <sstream>

#include "ClientImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "LookupService.h"
#include "ProducerImpl.h"
#include "RoundRobinMessageRouter.h"
#include "SinglePartitionMessageRouter.h"
#include "TopicMetadataImpl.h"
#include "TopicName.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

const std::string PartitionedProducerImpl::PARTITION_NAME_SUFFIX = "-partition-";

PartitionedProducerImpl::PartitionedProducerImpl(const ClientImplPtr& client, const TopicNamePtr& topicName,
                                                 unsigned int numPartitions,
                                                 const ProducerConfiguration& config,
                                                 const ProducerInterceptorsPtr& interceptors)
    : client_(client),
      topicName_(topicName),
      topic_(topicName_->toString()),
      conf_(config),
      topicMetadata_(new TopicMetadataImpl(numPartitions)),
      flushedPartitions_(0),
      interceptors_(interceptors) {
    routerPolicy_ = getMessageRouter();

    int maxPendingMessagesPerPartition =
        std::min(config.getMaxPendingMessages(),
                 (int)(config.getMaxPendingMessagesAcrossPartitions() / numPartitions));
    conf_.setMaxPendingMessages(maxPendingMessagesPerPartition);

    auto partitionsUpdateInterval = static_cast<unsigned int>(client->conf().getPartitionsUpdateInterval());
    if (partitionsUpdateInterval > 0) {
        listenerExecutor_ = client->getListenerExecutorProvider()->get();
        partitionsUpdateTimer_ = listenerExecutor_->createDeadlineTimer();
        partitionsUpdateInterval_ = std::chrono::seconds(partitionsUpdateInterval);
        lookupServicePtr_ = client->getLookup();
    }
}

MessageRoutingPolicyPtr PartitionedProducerImpl::getMessageRouter() {
    switch (conf_.getPartitionsRoutingMode()) {
        case ProducerConfiguration::RoundRobinDistribution:
            return std::make_shared<RoundRobinMessageRouter>(
                conf_.getHashingScheme(), conf_.getBatchingEnabled(), conf_.getBatchingMaxMessages(),
                conf_.getBatchingMaxAllowedSizeInBytes(),
                std::chrono::milliseconds(conf_.getBatchingMaxPublishDelayMs()));
        case ProducerConfiguration::CustomPartition:
            return conf_.getMessageRouterPtr();
        case ProducerConfiguration::UseSinglePartition:
        default:
            return std::make_shared<SinglePartitionMessageRouter>(getNumPartitions(),
                                                                  conf_.getHashingScheme());
    }
}

PartitionedProducerImpl::~PartitionedProducerImpl() { internalShutdown(); }
// override
const std::string& PartitionedProducerImpl::getTopic() const { return topic_; }

unsigned int PartitionedProducerImpl::getNumPartitions() const {
    return static_cast<unsigned int>(topicMetadata_->getNumPartitions());
}

unsigned int PartitionedProducerImpl::getNumPartitionsWithLock() const {
    Lock lock(producersMutex_);
    return getNumPartitions();
}

ProducerImplPtr PartitionedProducerImpl::newInternalProducer(unsigned int partition, bool lazy,
                                                             bool retryOnCreationError) {
    using namespace std::placeholders;
    auto client = client_.lock();
    auto producer = std::make_shared<ProducerImpl>(client, *topicName_, conf_, interceptors_, partition,
                                                   retryOnCreationError);
    if (!client) {
        return producer;
    }

    if (lazy) {
        createLazyPartitionProducer(partition);
    } else {
        producer->getProducerCreatedFuture().addListener(
            std::bind(&PartitionedProducerImpl::handleSinglePartitionProducerCreated,
                      const_cast<PartitionedProducerImpl*>(this)->shared_from_this(), _1, _2, partition));
    }

    LOG_DEBUG("Creating Producer for single Partition - " << topicName_ << "-partition-" << partition);
    return producer;
}

// override
void PartitionedProducerImpl::start() {
    // create producer per partition
    // Here we don't need `producersMutex` to protect `producers_`, because `producers_` can only be increased
    // when `state_` is Ready

    if (conf_.getLazyStartPartitionedProducers() && conf_.getAccessMode() == ProducerConfiguration::Shared) {
        // start one producer now, to ensure authz errors occur now
        // if the SinglePartition router is used, then this producer will serve
        // all non-keyed messages in the future
        Message msg = MessageBuilder().setContent("x").build();
        short partition = (short)(routerPolicy_->getPartition(msg, *topicMetadata_));

        for (unsigned int i = 0; i < getNumPartitions(); i++) {
            bool lazy = (short)i != partition;
            producers_.push_back(newInternalProducer(i, lazy, false));
        }

        producers_[partition]->start();
    } else {
        for (unsigned int i = 0; i < getNumPartitions(); i++) {
            producers_.push_back(newInternalProducer(i, false, false));
        }

        for (ProducerList::const_iterator prod = producers_.begin(); prod != producers_.end(); prod++) {
            (*prod)->start();
        }
    }
}

void PartitionedProducerImpl::handleSinglePartitionProducerCreated(
    Result result, const ProducerImplBaseWeakPtr& producerWeakPtr, unsigned int partitionIndex) {
    // to indicate, we are doing cleanup using closeAsync after producer create
    // has failed and the invocation of closeAsync is not from client
    const auto numPartitions = getNumPartitionsWithLock();
    assert(numProducersCreated_ <= numPartitions && partitionIndex <= numPartitions);

    if (state_ == Closing) {
        return;
    }

    if (state_ == Failed) {
        // We have already informed client that producer creation failed
        if (++numProducersCreated_ == numPartitions) {
            closeAsync(nullptr);
        }
        return;
    }

    if (result != ResultOk) {
        LOG_ERROR("Unable to create Producer for partition - " << partitionIndex << " Error - " << result);
        partitionedProducerCreatedPromise_.setFailed(result);
        state_ = Failed;
        if (++numProducersCreated_ == numPartitions) {
            closeAsync(nullptr);
        }
        return;
    }

    if (++numProducersCreated_ == numPartitions) {
        state_ = Ready;
        if (partitionsUpdateTimer_) {
            runPartitionUpdateTask();
        }
        partitionedProducerCreatedPromise_.setValue(shared_from_this());
    }
}

void PartitionedProducerImpl::createLazyPartitionProducer(unsigned int partitionIndex) {
    const auto numPartitions = getNumPartitions();
    assert(numProducersCreated_ <= numPartitions);
    assert(partitionIndex <= numPartitions);
    numProducersCreated_++;
    if (numProducersCreated_ == numPartitions) {
        state_ = Ready;
        if (partitionsUpdateTimer_) {
            runPartitionUpdateTask();
        }
        partitionedProducerCreatedPromise_.setValue(shared_from_this());
    }
}

// override
void PartitionedProducerImpl::sendAsync(const Message& msg, SendCallback callback) {
    if (state_ != Ready) {
        if (callback) {
            callback(ResultAlreadyClosed, msg.getMessageId());
        }
        return;
    }

    // get partition for this message from router policy
    Lock producersLock(producersMutex_);
    short partition = (short)(routerPolicy_->getPartition(msg, *topicMetadata_));
    if (partition >= getNumPartitions() || partition >= producers_.size()) {
        LOG_ERROR("Got Invalid Partition for message from Router Policy, Partition - " << partition);
        // change me: abort or notify failure in callback?
        //          change to appropriate error if callback
        if (callback) {
            callback(ResultUnknownError, msg.getMessageId());
        }
        return;
    }
    // find a producer for that partition, index should start from 0
    ProducerImplPtr producer = producers_[partition];

    // if the producer is not started (lazy producer), then kick-off the start process
    if (!producer->isStarted()) {
        producer->start();
    }

    producersLock.unlock();

    // send message on that partition
    if (!conf_.getLazyStartPartitionedProducers() || producer->ready()) {
        producer->sendAsync(msg, std::move(callback));
    } else {
        // Wrapping the callback into a lambda has overhead, so we check if the producer is ready first
        producer->getProducerCreatedFuture().addListener(
            [msg, callback](Result result, const ProducerImplBaseWeakPtr& weakProducer) {
                if (result == ResultOk) {
                    weakProducer.lock()->sendAsync(msg, callback);
                } else if (callback) {
                    callback(result, {});
                }
            });
    }
}

// override
void PartitionedProducerImpl::shutdown() { internalShutdown(); }

void PartitionedProducerImpl::internalShutdown() {
    cancelTimers();
    interceptors_->close();
    auto client = client_.lock();
    if (client) {
        client->cleanupProducer(this);
    }
    partitionedProducerCreatedPromise_.setFailed(ResultAlreadyClosed);
    state_ = Closed;
}

const std::string& PartitionedProducerImpl::getProducerName() const {
    Lock producersLock(producersMutex_);
    return producers_[0]->getProducerName();
}

const std::string& PartitionedProducerImpl::getSchemaVersion() const {
    Lock producersLock(producersMutex_);
    // Since the schema is atomically assigned on the partitioned-topic,
    // it's guaranteed that all the partitions will have the same schema version.
    return producers_[0]->getSchemaVersion();
}

int64_t PartitionedProducerImpl::getLastSequenceId() const {
    int64_t currentMax = -1L;
    Lock producersLock(producersMutex_);
    for (int i = 0; i < producers_.size(); i++) {
        currentMax = std::max(currentMax, producers_[i]->getLastSequenceId());
    }

    return currentMax;
}

/*
 * if createProducerCallback is set, it means the closeAsync is called from CreateProducer API which failed to
 * create one or many producers for partitions. So, we have to notify with ERROR on createProducerFailure
 */
void PartitionedProducerImpl::closeAsync(CloseCallback originalCallback) {
    auto closeCallback = [this, originalCallback](Result result) {
        if (result == ResultOk) {
            internalShutdown();
        }
        if (originalCallback) {
            originalCallback(result);
        }
    };

    if (state_ == Closed || state_.exchange(Closing) == Closing) {
        closeCallback(ResultAlreadyClosed);
        return;
    }

    cancelTimers();

    unsigned int producerAlreadyClosed = 0;

    // Here we don't need `producersMutex` to protect `producers_`, because `producers_` can only be increased
    // when `state_` is Ready
    for (auto& producer : producers_) {
        if (!producer->isClosed()) {
            auto self = shared_from_this();
            const auto partition = static_cast<unsigned int>(producer->partition());
            producer->closeAsync([this, self, partition, closeCallback](Result result) {
                handleSinglePartitionProducerClose(result, partition, closeCallback);
            });
        } else {
            producerAlreadyClosed++;
        }
    }
    const auto numProducers = producers_.size();

    /*
     * No need to set state since:-
     * a. If closeAsync before creation then state == Closed, since producers_.size() = producerAlreadyClosed
     * = 0
     * b. If closeAsync called after all sub partitioned producer connected -
     * handleSinglePartitionProducerClose handles the closing
     * c. If closeAsync called due to failure in creating just one sub producer then state is set by
     * handleSinglePartitionProducerCreated
     */
    if (producerAlreadyClosed == numProducers) {
        closeCallback(ResultOk);
    }
}

// `callback` is a wrapper of user provided callback, it's not null and will call `shutdown()`
void PartitionedProducerImpl::handleSinglePartitionProducerClose(Result result, unsigned int partitionIndex,
                                                                 const CloseCallback& callback) {
    if (state_ == Failed) {
        // we should have already notified the client by callback
        return;
    }
    if (result != ResultOk) {
        LOG_ERROR("Closing the producer failed for partition - " << partitionIndex);
        callback(result);
        state_ = Failed;
        return;
    }
    assert(partitionIndex < getNumPartitionsWithLock());
    if (numProducersCreated_ > 0) {
        numProducersCreated_--;
    }
    // closed all successfully
    if (!numProducersCreated_) {
        // set the producerCreatedPromise to failure, if client called
        // closeAsync and it's not failure to create producer, the promise
        // is set second time here, first time it was successful. So check
        // if there's any adverse effect of setting it again. It should not
        // be but must check. MUSTCHECK changeme
        partitionedProducerCreatedPromise_.setFailed(ResultUnknownError);
        callback(result);
        return;
    }
}

// override
Future<Result, ProducerImplBaseWeakPtr> PartitionedProducerImpl::getProducerCreatedFuture() {
    return partitionedProducerCreatedPromise_.getFuture();
}

// override
bool PartitionedProducerImpl::isClosed() { return state_ == Closed; }

void PartitionedProducerImpl::triggerFlush() {
    Lock producersLock(producersMutex_);
    for (ProducerList::const_iterator prod = producers_.begin(); prod != producers_.end(); prod++) {
        if ((*prod)->isStarted()) {
            (*prod)->triggerFlush();
        }
    }
}

void PartitionedProducerImpl::flushAsync(FlushCallback callback) {
    if (!flushPromise_ || flushPromise_->isComplete()) {
        flushPromise_ = std::make_shared<Promise<Result, bool>>();
    } else {
        // already in flushing, register a listener callback
        auto listenerCallback = [callback](Result result, bool v) {
            if (v) {
                callback(ResultOk);
            } else {
                callback(ResultUnknownError);
            }
            return;
        };

        flushPromise_->getFuture().addListener(listenerCallback);
        return;
    }

    Lock producersLock(producersMutex_);
    const int numProducers = static_cast<int>(producers_.size());
    FlushCallback subFlushCallback = [this, callback, numProducers](Result result) {
        // We shouldn't lock `producersMutex_` here because `subFlushCallback` may be called in
        // `ProducerImpl::flushAsync`, and then deadlock occurs.
        int previous = flushedPartitions_.fetch_add(1);
        if (previous == numProducers - 1) {
            flushedPartitions_.store(0);
            flushPromise_->setValue(true);
            callback(result);
        }
        return;
    };

    for (ProducerList::const_iterator prod = producers_.begin(); prod != producers_.end(); prod++) {
        if ((*prod)->isStarted()) {
            (*prod)->flushAsync(subFlushCallback);
        } else {
            subFlushCallback(ResultOk);
        }
    }
}

void PartitionedProducerImpl::runPartitionUpdateTask() {
    auto weakSelf = weak_from_this();
    partitionsUpdateTimer_->expires_from_now(partitionsUpdateInterval_);
    partitionsUpdateTimer_->async_wait([weakSelf](const ASIO_ERROR& ec) {
        auto self = weakSelf.lock();
        if (self) {
            self->getPartitionMetadata();
        }
    });
}

void PartitionedProducerImpl::getPartitionMetadata() {
    using namespace std::placeholders;
    auto weakSelf = weak_from_this();
    lookupServicePtr_->getPartitionMetadataAsync(topicName_)
        .addListener([weakSelf](Result result, const LookupDataResultPtr& lookupDataResult) {
            auto self = weakSelf.lock();
            if (self) {
                self->handleGetPartitions(result, lookupDataResult);
            }
        });
}

void PartitionedProducerImpl::handleGetPartitions(Result result,
                                                  const LookupDataResultPtr& lookupDataResult) {
    if (state_ != Ready) {
        return;
    }

    if (!result) {
        const auto newNumPartitions = static_cast<unsigned int>(lookupDataResult->getPartitions());
        Lock producersLock(producersMutex_);
        const auto currentNumPartitions = getNumPartitions();
        assert(currentNumPartitions == producers_.size());
        if (newNumPartitions > currentNumPartitions) {
            LOG_INFO("new partition count: " << newNumPartitions);
            topicMetadata_.reset(new TopicMetadataImpl(newNumPartitions));

            std::vector<ProducerImplPtr> producers;
            auto lazy = conf_.getLazyStartPartitionedProducers() &&
                        conf_.getAccessMode() == ProducerConfiguration::Shared;
            for (unsigned int i = currentNumPartitions; i < newNumPartitions; i++) {
                ProducerImplPtr producer;
                try {
                    producer = newInternalProducer(i, lazy, true);
                } catch (const std::runtime_error& e) {
                    LOG_ERROR("Failed to create producer for partition " << i << ": " << e.what());
                    producers.clear();
                    break;
                }
                producers.emplace_back(producer);
            }
            if (producers.empty()) {
                runPartitionUpdateTask();
                return;
            }
            for (unsigned int i = 0; i < producers.size(); i++) {
                auto&& producer = producers[i];
                producers_.emplace_back(producer);
                if (!lazy) {
                    producer->start();
                }
            }
            producersLock.unlock();
            interceptors_->onPartitionsChange(getTopic(), newNumPartitions);
            // `runPartitionUpdateTask()` will be called in `handleSinglePartitionProducerCreated()`
            return;
        }
    } else {
        LOG_WARN("Failed to getPartitionMetadata: " << strResult(result));
    }
    runPartitionUpdateTask();
}

bool PartitionedProducerImpl::isConnected() const {
    if (state_ != Ready) {
        return false;
    }

    Lock producersLock(producersMutex_);
    const auto producers = producers_;
    producersLock.unlock();
    for (const auto& producer : producers) {
        if (producer->isStarted() && !producer->isConnected()) {
            return false;
        }
    }
    return true;
}

uint64_t PartitionedProducerImpl::getNumberOfConnectedProducer() {
    uint64_t numberOfConnectedProducer = 0;
    Lock producersLock(producersMutex_);
    const auto producers = producers_;
    producersLock.unlock();
    for (const auto& producer : producers) {
        if (producer->isConnected()) {
            numberOfConnectedProducer++;
        }
    }
    return numberOfConnectedProducer;
}

void PartitionedProducerImpl::cancelTimers() noexcept {
    if (partitionsUpdateTimer_) {
        ASIO_ERROR ec;
        partitionsUpdateTimer_->cancel(ec);
    }
}

}  // namespace pulsar

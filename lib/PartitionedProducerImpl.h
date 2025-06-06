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
#include <pulsar/MessageRoutingPolicy.h>
#include <pulsar/TopicMetadata.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "AsioTimer.h"
#include "LookupDataResult.h"
#include "ProducerImplBase.h"
#include "ProducerInterceptors.h"
#include "TimeUtils.h"

namespace pulsar {

class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using ClientImplWeakPtr = std::weak_ptr<ClientImpl>;
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;
class LookupService;
using LookupServicePtr = std::shared_ptr<LookupService>;
class ProducerImpl;
using ProducerImplPtr = std::shared_ptr<ProducerImpl>;
class TopicName;
using TopicNamePtr = std::shared_ptr<TopicName>;

class PartitionedProducerImpl : public ProducerImplBase,
                                public std::enable_shared_from_this<PartitionedProducerImpl> {
   public:
    enum State : uint8_t
    {
        Pending,
        Ready,
        Closing,
        Closed,
        Failed
    };
    const static std::string PARTITION_NAME_SUFFIX;

    typedef std::unique_lock<std::mutex> Lock;

    PartitionedProducerImpl(const ClientImplPtr& ptr, const TopicNamePtr& topicName,
                            unsigned int numPartitions, const ProducerConfiguration& config,
                            const ProducerInterceptorsPtr& interceptors);
    virtual ~PartitionedProducerImpl();

    // overrided methods from ProducerImplBase
    const std::string& getProducerName() const override;
    int64_t getLastSequenceId() const override;
    const std::string& getSchemaVersion() const override;
    void sendAsync(const Message& msg, SendCallback callback) override;
    /*
     * closes all active producers, it can be called explicitly from client as well as createProducer
     * when it fails to create one of the producers and we want to fail createProducer
     */
    void closeAsync(CloseCallback callback) override;
    void start() override;
    void shutdown() override;
    void internalShutdown();
    bool isClosed() override;
    const std::string& getTopic() const override;
    Future<Result, ProducerImplBaseWeakPtr> getProducerCreatedFuture() override;
    void triggerFlush() override;
    void flushAsync(FlushCallback callback) override;
    bool isConnected() const override;
    uint64_t getNumberOfConnectedProducer() override;
    void handleSinglePartitionProducerCreated(Result result,
                                              const ProducerImplBaseWeakPtr& producerBaseWeakPtr,
                                              const unsigned int partitionIndex);
    void createLazyPartitionProducer(const unsigned int partitionIndex);
    void handleSinglePartitionProducerClose(Result result, unsigned int partitionIndex,
                                            const CloseCallback& callback);

    void notifyResult(CloseCallback closeCallback);

    std::weak_ptr<PartitionedProducerImpl> weak_from_this() noexcept { return shared_from_this(); }

    friend class PulsarFriend;

   private:
    ClientImplWeakPtr client_;

    const TopicNamePtr topicName_;
    const std::string topic_;

    std::atomic_uint numProducersCreated_{0};

    /*
     * set when one or more Single Partition Creation fails, close will cleanup and fail the create callbackxo
     */
    bool cleanup_ = false;

    ProducerConfiguration conf_;

    typedef std::vector<ProducerImplPtr> ProducerList;
    ProducerList producers_;

    // producersMutex_ is used to share producers_ and topicMetadata_
    mutable std::mutex producersMutex_;
    MessageRoutingPolicyPtr routerPolicy_;

    std::atomic<State> state_{Pending};

    // only set this promise to value, when producers on all partitions are created.
    Promise<Result, ProducerImplBaseWeakPtr> partitionedProducerCreatedPromise_;

    std::unique_ptr<TopicMetadata> topicMetadata_;

    std::atomic<int> flushedPartitions_;
    std::shared_ptr<Promise<Result, bool>> flushPromise_;

    ExecutorServicePtr listenerExecutor_;
    DeadlineTimerPtr partitionsUpdateTimer_;
    TimeDuration partitionsUpdateInterval_;
    LookupServicePtr lookupServicePtr_;

    ProducerInterceptorsPtr interceptors_;

    unsigned int getNumPartitions() const;
    unsigned int getNumPartitionsWithLock() const;
    ProducerImplPtr newInternalProducer(unsigned int partition, bool lazy, bool retryOnCreationError);
    MessageRoutingPolicyPtr getMessageRouter();
    void runPartitionUpdateTask();
    void getPartitionMetadata();
    void handleGetPartitions(const Result result, const LookupDataResultPtr& partitionMetadata);
    void cancelTimers() noexcept;
};

}  // namespace pulsar

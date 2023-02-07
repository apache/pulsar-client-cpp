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

#ifndef LIB_READERIMPL_H_
#define LIB_READERIMPL_H_

#include <pulsar/Client.h>
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/ReaderConfiguration.h>

#include <atomic>
#include <memory>
#include <mutex>

#include "Future.h"

namespace pulsar {

class ReaderImpl;

typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;
typedef std::weak_ptr<ReaderImpl> ReaderImplWeakPtr;

class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using ClientImplWeakPtr = std::weak_ptr<ClientImpl>;
class ConsumerImplBase;
using ConsumerImplBaseWeakPtr = std::weak_ptr<ConsumerImplBase>;
class ConsumerImpl;
using ConsumerImplPtr = std::shared_ptr<ConsumerImpl>;
using ConsumerImplWeakPtr = std::weak_ptr<ConsumerImpl>;
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;

namespace test {

extern PULSAR_PUBLIC std::mutex readerConfigTestMutex;
extern PULSAR_PUBLIC std::atomic_bool readerConfigTestEnabled;
extern PULSAR_PUBLIC ConsumerConfiguration consumerConfigOfReader;

}  // namespace test

class PULSAR_PUBLIC ReaderImpl : public std::enable_shared_from_this<ReaderImpl> {
   public:
    ReaderImpl(const ClientImplPtr client, const std::string& topic, int partitions,
               const ReaderConfiguration& conf, const ExecutorServicePtr listenerExecutor,
               ReaderCallback readerCreatedCallback);

    void start(const MessageId& startMessageId, std::function<void(const ConsumerImplBaseWeakPtr&)> callback);

    const std::string& getTopic() const;

    Result readNext(Message& msg);
    Result readNext(Message& msg, int timeoutMs);
    void readNextAsync(ReceiveCallback callback);

    void closeAsync(ResultCallback callback);

    Future<Result, ReaderImplWeakPtr> getReaderCreatedFuture();

    ConsumerImplBasePtr getConsumer() const noexcept { return consumer_; }

    void hasMessageAvailableAsync(HasMessageAvailableCallback callback);

    void seekAsync(const MessageId& msgId, ResultCallback callback);
    void seekAsync(uint64_t timestamp, ResultCallback callback);

    void getLastMessageIdAsync(GetLastMessageIdCallback callback);

    bool isConnected() const;

   private:
    void messageListener(Consumer consumer, const Message& msg);

    void acknowledgeIfNecessary(Result result, const Message& msg);

    std::string topic_;
    int partitions_;
    ClientImplWeakPtr client_;
    ReaderConfiguration readerConf_;
    ConsumerImplBasePtr consumer_;
    ReaderCallback readerCreatedCallback_;
    ReaderListener readerListener_;
};
}  // namespace pulsar

#endif /* LIB_READERIMPL_H_ */

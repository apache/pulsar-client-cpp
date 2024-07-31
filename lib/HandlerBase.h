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
#ifndef _PULSAR_HANDLER_BASE_HEADER_
#define _PULSAR_HANDLER_BASE_HEADER_
#include <pulsar/Result.h>

#include <boost/optional.hpp>
#include <memory>
#include <mutex>
#include <string>

#include "AsioTimer.h"
#include "Backoff.h"
#include "Future.h"
#include "TimeUtils.h"

namespace pulsar {

class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using ClientImplWeakPtr = std::weak_ptr<ClientImpl>;
class ClientConnection;
using ClientConnectionPtr = std::shared_ptr<ClientConnection>;
using ClientConnectionWeakPtr = std::weak_ptr<ClientConnection>;
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;

class HandlerBase : public std::enable_shared_from_this<HandlerBase> {
   public:
    HandlerBase(const ClientImplPtr&, const std::string&, const Backoff&);

    virtual ~HandlerBase();

    void start();

    ClientConnectionWeakPtr getCnx() const;
    void setCnx(const ClientConnectionPtr& cnx);
    void resetCnx() { setCnx(nullptr); }
    void setRedirectedClusterURI(const std::string& serviceUrl);
    const std::string& getRedirectedClusterURI();

   protected:
    /*
     * tries reconnection and sets connection_ to valid object
     * @param assignedBrokerUrl assigned broker url to directly connect to without lookup
     */
    void grabCnx(const boost::optional<std::string>& assignedBrokerUrl);

    /*
     * tries reconnection and sets connection_ to valid object
     */
    void grabCnx();

    /*
     * Schedule reconnection after backoff time
     * @param assignedBrokerUrl assigned broker url to directly connect to without lookup
     */
    void scheduleReconnection(const boost::optional<std::string>& assignedBrokerUrl);
    /*
     * Schedule reconnection after backoff time
     */
    void scheduleReconnection();

    /**
     * Do some cleanup work before changing `connection_` to `cnx`.
     *
     * @param cnx the current connection
     */
    virtual void beforeConnectionChange(ClientConnection& cnx) = 0;

    /*
     * connectionOpened will be implemented by derived class to receive notification.
     *
     * @return ResultOk if the connection is successfully completed.
     * @return ResultError if there was a failure. ResultRetryable if reconnection is needed.
     * @return Do not use bool, only Result.
     */
    virtual Future<Result, bool> connectionOpened(const ClientConnectionPtr& connection) = 0;

    virtual void connectionFailed(Result result) = 0;

    virtual const std::string& getName() const = 0;

    const std::string& topic() const { return *topic_; }
    const std::shared_ptr<std::string>& getTopicPtr() const { return topic_; }

    long firstRequestIdAfterConnect() const {
        return firstRequestIdAfterConnect_.load(std::memory_order_acquire);
    }

   private:
    const std::shared_ptr<std::string> topic_;

    Future<Result, ClientConnectionPtr> getConnection(const ClientImplPtr& client,
                                                      const boost::optional<std::string>& assignedBrokerUrl);

    void handleDisconnection(Result result, const ClientConnectionPtr& cnx);

    void handleTimeout(const ASIO_ERROR& ec, const boost::optional<std::string>& assignedBrokerUrl);

   protected:
    ClientImplWeakPtr client_;
    const size_t connectionKeySuffix_;
    ExecutorServicePtr executor_;
    mutable std::mutex mutex_;
    std::mutex pendingReceiveMutex_;
    std::mutex batchPendingReceiveMutex_;
    ptime creationTimestamp_;

    const TimeDuration operationTimeut_;
    typedef std::unique_lock<std::mutex> Lock;

    enum State
    {
        NotStarted,       // Not initialized, in Java client: HandlerState.State.Uninitialized
        Pending,          // Client connecting to broker, in Java client: HandlerState.State.Connecting
        Ready,            // Handler is being used, in Java client: HandlerState.State.Ready
        Closing,          // Close cmd has been sent to broker, in Java client: HandlerState.State.Closing
        Closed,           // Broker acked the close, in Java client: HandlerState.State.Closed
        Failed,           // Handler is failed, in Java client: HandlerState.State.Failed
        Producer_Fenced,  // The producer has been fenced by the broker
                          // in Java client: HandlerState.State.ProducerFenced
    };

    std::atomic<State> state_;
    Backoff backoff_;
    uint64_t epoch_;

    Result convertToTimeoutIfNecessary(Result result, ptime startTimestamp) const;

    void setFirstRequestIdAfterConnect(long requestId) {
        firstRequestIdAfterConnect_.store(requestId, std::memory_order_release);
    }

   private:
    DeadlineTimerPtr timer_;
    DeadlineTimerPtr creationTimer_;

    mutable std::mutex connectionMutex_;
    std::atomic<bool> reconnectionPending_;
    ClientConnectionWeakPtr connection_;
    std::string redirectedClusterURI_;
    std::atomic<long> firstRequestIdAfterConnect_{-1L};
    std::atomic<long> connectionTimeMs_{0};  // only for tests

    friend class ClientConnection;
    friend class PulsarFriend;
    friend class ConsumerTest;
};
}  // namespace pulsar
#endif  //_PULSAR_HANDLER_BASE_HEADER_

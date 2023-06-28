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

#include <boost/asio/deadline_timer.hpp>
#include <memory>
#include <mutex>
#include <string>

#include "Backoff.h"

namespace pulsar {

using namespace boost::posix_time;
using boost::posix_time::milliseconds;
using boost::posix_time::seconds;

class HandlerBase;
typedef std::weak_ptr<HandlerBase> HandlerBaseWeakPtr;
typedef std::shared_ptr<HandlerBase> HandlerBasePtr;
class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using ClientImplWeakPtr = std::weak_ptr<ClientImpl>;
class ClientConnection;
using ClientConnectionPtr = std::shared_ptr<ClientConnection>;
using ClientConnectionWeakPtr = std::weak_ptr<ClientConnection>;
class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;
using DeadlineTimerPtr = std::shared_ptr<boost::asio::deadline_timer>;

class HandlerBase {
   public:
    HandlerBase(const ClientImplPtr&, const std::string&, const Backoff&);

    virtual ~HandlerBase();

    void start();

    ClientConnectionWeakPtr getCnx() const;
    void setCnx(const ClientConnectionPtr& cnx);
    void resetCnx() { setCnx(nullptr); }

   protected:
    /*
     * tries reconnection and sets connection_ to valid object
     */
    void grabCnx();

    /*
     * Schedule reconnection after backoff time
     */
    static void scheduleReconnection(HandlerBasePtr handler);

    /**
     * Do some cleanup work before changing `connection_` to `cnx`.
     *
     * @param cnx the current connection
     */
    virtual void beforeConnectionChange(ClientConnection& cnx) = 0;

    /*
     * connectionOpened will be implemented by derived class to receive notification
     */

    virtual void connectionOpened(const ClientConnectionPtr& connection) = 0;

    virtual void connectionFailed(Result result) = 0;

    virtual HandlerBaseWeakPtr get_weak_from_this() = 0;

    virtual const std::string& getName() const = 0;

   private:
    static void handleNewConnection(Result result, ClientConnectionWeakPtr connection, HandlerBaseWeakPtr wp);
    static void handleDisconnection(Result result, ClientConnectionWeakPtr connection, HandlerBaseWeakPtr wp);

    static void handleTimeout(const boost::system::error_code& ec, HandlerBasePtr handler);

   protected:
    ClientImplWeakPtr client_;
    const std::shared_ptr<std::string> topic_;
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

   private:
    DeadlineTimerPtr timer_;

    mutable std::mutex connectionMutex_;
    ClientConnectionWeakPtr connection_;
    friend class ClientConnection;
    friend class PulsarFriend;
};
}  // namespace pulsar
#endif  //_PULSAR_HANDLER_BASE_HEADER_

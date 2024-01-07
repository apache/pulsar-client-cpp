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
#ifndef _PULSAR_EXECUTOR_SERVICE_HEADER_
#define _PULSAR_EXECUTOR_SERVICE_HEADER_

#include <pulsar/defines.h>

#include <atomic>
#ifdef USE_ASIO
#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/ssl.hpp>
#else
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#endif
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "AsioTimer.h"

namespace pulsar {
typedef std::shared_ptr<ASIO::ip::tcp::socket> SocketPtr;
typedef std::shared_ptr<ASIO::ssl::stream<ASIO::ip::tcp::socket &> > TlsSocketPtr;
typedef std::shared_ptr<ASIO::ip::tcp::resolver> TcpResolverPtr;
class PULSAR_PUBLIC ExecutorService : public std::enable_shared_from_this<ExecutorService> {
   public:
    using IOService = ASIO::io_service;
    using SharedPtr = std::shared_ptr<ExecutorService>;

    static SharedPtr create();
    ~ExecutorService();

    ExecutorService(const ExecutorService &) = delete;
    ExecutorService &operator=(const ExecutorService &) = delete;

    // throws std::runtime_error if failed
    SocketPtr createSocket();
    static TlsSocketPtr createTlsSocket(SocketPtr &socket, ASIO::ssl::context &ctx);
    // throws std::runtime_error if failed
    TcpResolverPtr createTcpResolver();
    // throws std::runtime_error if failed
    DeadlineTimerPtr createDeadlineTimer();
    void postWork(std::function<void(void)> task);

    // See TimeoutProcessor for the semantics of the parameter.
    void close(long timeoutMs = 3000);

    IOService &getIOService() { return io_service_; }
    bool isClosed() const noexcept { return closed_; }

   private:
    /*
     * io_service is our interface to os, io object schedule async ops on this object
     */
    IOService io_service_;

    std::atomic_bool closed_{false};
    std::mutex mutex_;
    std::condition_variable cond_;
    bool ioServiceDone_{false};

    ExecutorService();

    void start();

    void restart();
};

using ExecutorServicePtr = ExecutorService::SharedPtr;

class PULSAR_PUBLIC ExecutorServiceProvider {
   public:
    explicit ExecutorServiceProvider(int nthreads);

    ExecutorServicePtr get() { return get(executorIdx_++); }

    ExecutorServicePtr get(size_t index);

    // See TimeoutProcessor for the semantics of the parameter.
    void close(long timeoutMs = 3000);

   private:
    typedef std::vector<ExecutorServicePtr> ExecutorList;
    ExecutorList executors_;
    std::atomic_size_t executorIdx_;
    std::mutex mutex_;
    typedef std::unique_lock<std::mutex> Lock;
};

typedef std::shared_ptr<ExecutorServiceProvider> ExecutorServiceProviderPtr;
}  // namespace pulsar

#endif  //_PULSAR_EXECUTOR_SERVICE_HEADER_

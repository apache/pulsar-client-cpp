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
#ifndef _PULSAR_CONNECTION_POOL_HEADER_
#define _PULSAR_CONNECTION_POOL_HEADER_

#include <pulsar/ClientConfiguration.h>
#include <pulsar/Result.h>
#include <pulsar/defines.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>

#include "Future.h"
namespace pulsar {

class ClientConnection;
using ClientConnectionWeakPtr = std::weak_ptr<ClientConnection>;
class ExecutorService;
class ExecutorServiceProvider;
using ExecutorServiceProviderPtr = std::shared_ptr<ExecutorServiceProvider>;

class PULSAR_PUBLIC ConnectionPool {
   public:
    ConnectionPool(const ClientConfiguration& conf, const ExecutorServiceProviderPtr& executorProvider,
                   const AuthenticationPtr& authentication, const std::string& clientVersion);

    /**
     * Close the connection pool.
     *
     * @return false if it has already been closed.
     */
    bool close();

    void remove(const std::string& logicalAddress, const std::string& physicalAddress, size_t keySuffix,
                ClientConnection* value);

    /**
     * Get a connection from the pool.
     * <p>
     * The connection can either be created or be coming from the pool itself.
     * <p>
     * When specifying multiple addresses, the logicalAddress is used as a tag for the broker,
     * while the physicalAddress is where the connection is actually happening.
     * <p>
     * These two addresses can be different when the client is forced to connect through
     * a proxy layer. Essentially, the pool is using the logical address as a way to
     * decide whether to reuse a particular connection.
     *
     * There could be many connections to the same broker, so this pool uses an integer key as the suffix of
     * the key that represents the connection.
     *
     * @param logicalAddress the address to use as the broker tag
     * @param physicalAddress the real address where the TCP connection should be made
     * @param keySuffix the key suffix to choose which connection on the same broker
     * @return a future that will produce the ClientCnx object
     */
    Future<Result, ClientConnectionWeakPtr> getConnectionAsync(const std::string& logicalAddress,
                                                               const std::string& physicalAddress,
                                                               size_t keySuffix);

    Future<Result, ClientConnectionWeakPtr> getConnectionAsync(const std::string& logicalAddress,
                                                               const std::string& physicalAddress) {
        return getConnectionAsync(logicalAddress, physicalAddress, generateRandomIndex());
    }

    Future<Result, ClientConnectionWeakPtr> getConnectionAsync(const std::string& address) {
        return getConnectionAsync(address, address);
    }

    size_t generateRandomIndex() { return randomDistribution_(randomEngine_); }

   private:
    ClientConfiguration clientConfiguration_;
    ExecutorServiceProviderPtr executorProvider_;
    AuthenticationPtr authentication_;
    typedef std::map<std::string, std::shared_ptr<ClientConnection>> PoolMap;
    PoolMap pool_;
    const std::string clientVersion_;
    mutable std::recursive_mutex mutex_;
    std::atomic_bool closed_{false};

    std::uniform_int_distribution<> randomDistribution_;
    std::mt19937 randomEngine_;

    friend class PulsarFriend;
};
}  // namespace pulsar
#endif  //_PULSAR_CONNECTION_POOL_HEADER_

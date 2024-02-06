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
#include "ConnectionPool.h"

#ifdef USE_ASIO
#include <asio/ip/tcp.hpp>
#include <asio/ssl.hpp>
#else
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#endif

#include "ClientConnection.h"
#include "ExecutorService.h"
#include "LogUtils.h"

using ASIO::ip::tcp;
namespace ssl = ASIO::ssl;
typedef ssl::stream<tcp::socket> ssl_socket;

DECLARE_LOG_OBJECT()

namespace pulsar {

ConnectionPool::ConnectionPool(const ClientConfiguration& conf, ExecutorServiceProviderPtr executorProvider,
                               const AuthenticationPtr& authentication, const std::string& clientVersion)
    : clientConfiguration_(conf),
      executorProvider_(executorProvider),
      authentication_(authentication),
      clientVersion_(clientVersion),
      randomDistribution_(0, conf.getConnectionsPerBroker() - 1),
      randomEngine_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

bool ConnectionPool::close() {
    bool expectedState = false;
    if (!closed_.compare_exchange_strong(expectedState, true)) {
        return false;
    }

    std::unique_lock<std::recursive_mutex> lock(mutex_);

    for (auto cnxIt = pool_.begin(); cnxIt != pool_.end(); cnxIt++) {
        auto& cnx = cnxIt->second;
        if (cnx) {
            // The 2nd argument is false because removing a value during the iteration will cause segfault
            cnx->close(ResultDisconnected, false);
        }
    }
    pool_.clear();
    return true;
}

Future<Result, ClientConnectionWeakPtr> ConnectionPool::getConnectionAsync(const std::string& logicalAddress,
                                                                           const std::string& physicalAddress,
                                                                           size_t keySuffix) {
    if (closed_) {
        Promise<Result, ClientConnectionWeakPtr> promise;
        promise.setFailed(ResultAlreadyClosed);
        return promise.getFuture();
    }

    std::unique_lock<std::recursive_mutex> lock(mutex_);

    std::stringstream ss;
    ss << logicalAddress << '-' << keySuffix;
    const std::string key = ss.str();

    PoolMap::iterator cnxIt = pool_.find(key);
    if (cnxIt != pool_.end()) {
        auto& cnx = cnxIt->second;

        if (!cnx->isClosed()) {
            // Found a valid or pending connection in the pool
            LOG_DEBUG("Got connection from pool for " << key << " use_count: "  //
                                                      << (cnx.use_count()) << " @ " << cnx.get());
            return cnx->getConnectFuture();
        } else {
            // The closed connection should have been removed from the pool in ClientConnection::close
            LOG_WARN("Deleting stale connection from pool for " << key << " use_count: " << (cnx.use_count())
                                                                << " @ " << cnx.get());
            pool_.erase(key);
        }
    }

    // No valid or pending connection found in the pool, creating a new one
    ClientConnectionPtr cnx;
    try {
        cnx.reset(new ClientConnection(logicalAddress, physicalAddress, executorProvider_->get(keySuffix),
                                       clientConfiguration_, authentication_, clientVersion_, *this,
                                       keySuffix));
    } catch (Result result) {
        Promise<Result, ClientConnectionWeakPtr> promise;
        promise.setFailed(result);
        return promise.getFuture();
    } catch (const std::runtime_error& e) {
        lock.unlock();
        LOG_ERROR("Failed to create connection: " << e.what())
        Promise<Result, ClientConnectionWeakPtr> promise;
        promise.setFailed(ResultConnectError);
        return promise.getFuture();
    }

    LOG_INFO("Created connection for " << key);

    Future<Result, ClientConnectionWeakPtr> future = cnx->getConnectFuture();
    pool_.insert(std::make_pair(key, cnx));

    lock.unlock();

    cnx->tcpConnectAsync();
    return future;
}

void ConnectionPool::remove(const std::string& key, ClientConnection* value) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = pool_.find(key);
    if (it != pool_.end() && it->second.get() == value) {
        LOG_INFO("Remove connection for " << key);
        pool_.erase(it);
    }
}

}  // namespace pulsar

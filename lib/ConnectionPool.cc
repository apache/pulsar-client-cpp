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

ConnectionPool::ConnectionPool(const ClientConfiguration& conf,
                               const ExecutorServiceProviderPtr& executorProvider,
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

    std::vector<ClientConnectionPtr> connectionsToClose;
    // ClientConnection::close() will remove the connection from the pool, which is not allowed when iterating
    // over a map, so we store the connections to close in a vector first and don't iterate the pool when
    // closing the connections.
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    connectionsToClose.reserve(pool_.size());
    for (auto&& kv : pool_) {
        connectionsToClose.emplace_back(kv.second);
    }
    pool_.clear();
    lock.unlock();

    for (auto&& cnx : connectionsToClose) {
        if (cnx) {
            // Close with a fatal error to not let client retry
            auto& future = cnx->close(ResultAlreadyClosed);
            using namespace std::chrono_literals;
            if (auto status = future.wait_for(5s); status != std::future_status::ready) {
                LOG_WARN("Connection close timed out for " << cnx.get()->cnxString());
            }
            if (cnx.use_count() > 1) {
                // There are some asynchronous operations that hold the reference on the connection, we should
                // wait until them to finish. Otherwise, `io_context::stop()` will be called in
                // `ClientImpl::shutdown()` when closing the `ExecutorServiceProvider`. Then
                // `io_context::run()` will return and the `io_context` object will be destroyed. In this
                // case, if there is any pending handler, it will crash.
                for (int i = 0; i < 500 && cnx.use_count() > 1; i++) {
                    std::this_thread::sleep_for(10ms);
                }
                if (cnx.use_count() > 1) {
                    LOG_WARN("Connection still has " << (cnx.use_count() - 1)
                                                     << " references after waiting for 5 seconds for "
                                                     << cnx.get()->cnxString());
                }
            }
        }
    }
    return true;
}

static const std::string getKey(const std::string& logicalAddress, const std::string& physicalAddress,
                                size_t keySuffix) {
    std::stringstream ss;
    ss << logicalAddress << '-' << physicalAddress << '-' << keySuffix;
    return ss.str();
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

    auto key = getKey(logicalAddress, physicalAddress, keySuffix);

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

void ConnectionPool::remove(const std::string& logicalAddress, const std::string& physicalAddress,
                            size_t keySuffix, ClientConnection* value) {
    auto key = getKey(logicalAddress, physicalAddress, keySuffix);
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = pool_.find(key);
    if (it != pool_.end() && it->second.get() == value) {
        LOG_INFO("Remove connection for " << key);
        pool_.erase(it);
    }
}

}  // namespace pulsar

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
#include "HandlerBase.h"

#include <chrono>

#include "AsioDefines.h"
#include "Backoff.h"
#include "ClientConnection.h"
#include "ClientImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "ResultUtils.h"
#include "TimeUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

HandlerBase::HandlerBase(const ClientImplPtr& client, const std::string& topic, const Backoff& backoff)
    : topic_(std::make_shared<std::string>(topic)),
      client_(client),
      connectionKeySuffix_(client->getConnectionPool().generateRandomIndex()),
      executor_(client->getIOExecutorProvider()->get()),
      mutex_(),
      creationTimestamp_(TimeUtils::now()),
      operationTimeut_(std::chrono::seconds(client->conf().getOperationTimeoutSeconds())),
      state_(NotStarted),
      backoff_(backoff),
      epoch_(0),
      reconnectionPending_(false),
      timer_(executor_->createDeadlineTimer()),
      creationTimer_(executor_->createDeadlineTimer()),
      redirectedClusterURI_("") {}

HandlerBase::~HandlerBase() {
    cancelTimer(*timer_);
    cancelTimer(*creationTimer_);
}

void HandlerBase::start() {
    // guard against concurrent state changes such as closing
    State state = NotStarted;
    if (state_.compare_exchange_strong(state, Pending)) {
        grabCnx();
    }
    creationTimer_->expires_after(operationTimeut_);
    auto weakSelf = weak_from_this();
    creationTimer_->async_wait([this, weakSelf](const ASIO_ERROR& error) {
        if (auto self = weakSelf.lock(); self && !error) {
            LOG_WARN("Cancel the pending reconnection due to the start timeout");
            connectionFailed(ResultTimeout);
            cancelTimer(*timer_);
        }
    });
}

ClientConnectionWeakPtr HandlerBase::getCnx() const {
    Lock lock(connectionMutex_);
    return connection_;
}

void HandlerBase::setCnx(const ClientConnectionPtr& cnx) {
    Lock lock(connectionMutex_);
    auto previousCnx = connection_.lock();
    if (previousCnx) {
        beforeConnectionChange(*previousCnx);
    }
    connection_ = cnx;
}

void HandlerBase::grabCnx() { grabCnx(std::nullopt); }

Future<Result, ClientConnectionPtr> HandlerBase::getConnection(
    const ClientImplPtr& client, const optional<std::string>& assignedBrokerUrl) {
    if (assignedBrokerUrl && client->getLookupCount() > 0) {
        return client->connect(getRedirectedClusterURI(), *assignedBrokerUrl, connectionKeySuffix_);
    } else {
        return client->getConnection(getRedirectedClusterURI(), topic(), connectionKeySuffix_);
    }
}

void HandlerBase::grabCnx(const optional<std::string>& assignedBrokerUrl) {
    bool expectedState = false;
    if (!reconnectionPending_.compare_exchange_strong(expectedState, true)) {
        LOG_INFO(getName() << "Ignoring reconnection attempt since there's already a pending reconnection");
        return;
    }

    if (getCnx().lock()) {
        LOG_INFO(getName() << "Ignoring reconnection request since we're already connected");
        reconnectionPending_ = false;
        return;
    }

    LOG_INFO(getName() << "Getting connection from pool");
    ClientImplPtr client = client_.lock();
    if (!client) {
        LOG_WARN(getName() << "Client is invalid when calling grabCnx()");
        connectionFailed(ResultAlreadyClosed);
        reconnectionPending_ = false;
        return;
    }
    auto self = shared_from_this();
    auto cnxFuture = getConnection(client, assignedBrokerUrl);
    using namespace std::chrono;
    auto before = high_resolution_clock::now();
    cnxFuture.addListener([this, self, before](Result result, const ClientConnectionPtr& cnx) {
        if (result == ResultOk) {
            connectionOpened(cnx).addListener([this, self, before](Result result, bool) {
                // Do not use bool, only Result.
                reconnectionPending_ = false;
                if (result == ResultOk) {
                    connectionTimeMs_ =
                        duration_cast<milliseconds>(high_resolution_clock::now() - before).count();
                    // Prevent the creationTimer_ from cancelling the timer_ in future
                    cancelTimer(*creationTimer_);
                    LOG_INFO("Finished connecting to broker after " << connectionTimeMs_ << " ms")
                } else if (isResultRetryable(result)) {
                    scheduleReconnection();
                }
            });
        } else {
            connectionFailed(result);
            reconnectionPending_ = false;
            scheduleReconnection();
        }
    });
}

void HandlerBase::handleDisconnection(Result result, const ClientConnectionPtr& cnx) {
    State state = state_;

    ClientConnectionPtr currentConnection = getCnx().lock();
    if (currentConnection && cnx.get() != currentConnection.get()) {
        LOG_WARN(
            getName() << "Ignoring connection closed since we are already attached to a newer connection");
        return;
    }

    resetCnx();

    if (isResultRetryable(result)) {
        scheduleReconnection();
        return;
    }

    switch (state) {
        case Pending:
        case Ready:
            scheduleReconnection();
            break;

        case NotStarted:
        case Closing:
        case Closed:
        case Producer_Fenced:
        case Failed:
            LOG_DEBUG(getName() << "Ignoring connection closed event since the handler is not used anymore");
            break;
    }
}
void HandlerBase::scheduleReconnection() { scheduleReconnection(std::nullopt); }
void HandlerBase::scheduleReconnection(const optional<std::string>& assignedBrokerUrl) {
    const auto state = state_.load();

    if (state == Pending || state == Ready) {
        TimeDuration delay = assignedBrokerUrl ? std::chrono::milliseconds(0) : backoff_.next();

        LOG_INFO(getName() << "Schedule reconnection in " << (toMillis(delay) / 1000.0) << " s");
        timer_->expires_after(delay);
        // passing shared_ptr here since time_ will get destroyed, so tasks will be cancelled
        // so we will not run into the case where grabCnx is invoked on out of scope handler
        auto name = getName();
        auto weakSelf = weak_from_this();
        timer_->async_wait([name, weakSelf, assignedBrokerUrl](const ASIO_ERROR& ec) {
            if (auto self = weakSelf.lock()) {
                self->handleTimeout(ec, assignedBrokerUrl);
            } else {
                LOG_WARN(name << "Cancel the reconnection since the handler is destroyed");
            }
        });
    }
}

void HandlerBase::handleTimeout(const ASIO_ERROR& ec, const optional<std::string>& assignedBrokerUrl) {
    if (ec) {
        LOG_INFO(getName() << "Ignoring timer cancelled event, code[" << ec << "]");
    } else {
        epoch_++;
        grabCnx(assignedBrokerUrl);
    }
}

Result HandlerBase::convertToTimeoutIfNecessary(Result result, ptime startTimestamp) const {
    if (isResultRetryable(result) && (TimeUtils::now() - startTimestamp >= operationTimeut_)) {
        return ResultTimeout;
    } else {
        return result;
    }
}

void HandlerBase::setRedirectedClusterURI(const std::string& serviceUrl) {
    Lock lock(mutex_);
    redirectedClusterURI_ = serviceUrl;
}
const std::string& HandlerBase::getRedirectedClusterURI() {
    Lock lock(mutex_);
    return redirectedClusterURI_;
}

}  // namespace pulsar

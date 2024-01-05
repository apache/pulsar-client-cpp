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
      timer_(executor_->createDeadlineTimer()),
      reconnectionPending_(false) {}

HandlerBase::~HandlerBase() { timer_->cancel(); }

void HandlerBase::start() {
    // guard against concurrent state changes such as closing
    State state = NotStarted;
    if (state_.compare_exchange_strong(state, Pending)) {
        grabCnx();
    }
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

void HandlerBase::grabCnx() {
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
        connectionFailed(ResultConnectError);
        reconnectionPending_ = false;
        return;
    }
    auto self = shared_from_this();
    auto cnxFuture = client->getConnection(topic(), connectionKeySuffix_);
    cnxFuture.addListener([this, self](Result result, const ClientConnectionPtr& cnx) {
        if (result == ResultOk) {
            LOG_DEBUG(getName() << "Connected to broker: " << cnx->cnxString());
            connectionOpened(cnx).addListener([this, self](Result result, bool) {
                // Do not use bool, only Result.
                reconnectionPending_ = false;
                if (isResultRetryable(result)) {
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

void HandlerBase::scheduleReconnection() {
    const auto state = state_.load();

    if (state == Pending || state == Ready) {
        TimeDuration delay = backoff_.next();

        LOG_INFO(getName() << "Schedule reconnection in " << (toMillis(delay) / 1000.0) << " s");
        timer_->expires_from_now(delay);
        // passing shared_ptr here since time_ will get destroyed, so tasks will be cancelled
        // so we will not run into the case where grabCnx is invoked on out of scope handler
        auto name = getName();
        std::weak_ptr<HandlerBase> weakSelf{shared_from_this()};
        timer_->async_wait([name, weakSelf](const ASIO_ERROR& ec) {
            auto self = weakSelf.lock();
            if (self) {
                self->handleTimeout(ec);
            } else {
                LOG_WARN(name << "Cancel the reconnection since the handler is destroyed");
            }
        });
    }
}

void HandlerBase::handleTimeout(const ASIO_ERROR& ec) {
    if (ec) {
        LOG_DEBUG(getName() << "Ignoring timer cancelled event, code[" << ec << "]");
        return;
    } else {
        epoch_++;
        grabCnx();
    }
}

Result HandlerBase::convertToTimeoutIfNecessary(Result result, ptime startTimestamp) const {
    if (isResultRetryable(result) && (TimeUtils::now() - startTimestamp >= operationTimeut_)) {
        return ResultTimeout;
    } else {
        return result;
    }
}

}  // namespace pulsar

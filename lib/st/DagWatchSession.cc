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
#include "DagWatchSession.h"

#include <chrono>
#include <utility>

#include "PulsarApi.pb.h"
#include "lib/Commands.h"
#include "lib/ExecutorService.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar::st {

namespace {

std::atomic<std::uint64_t> sessionIdGenerator{0};

Result toResult(pulsar::proto::ServerError error) {
    return error == pulsar::proto::TopicNotFound ? ResultTopicNotFound : ResultLookupError;
}

}  // namespace

DagWatchSession::DagWatchSession(pulsar::ClientImplPtr client, std::string topic, bool createIfMissing)
    : client_(std::move(client)),
      inputTopic_(std::move(topic)),
      createIfMissing_(createIfMissing),
      sessionId_(++sessionIdGenerator),
      backoff_(std::chrono::milliseconds(100), std::chrono::seconds(30), std::chrono::milliseconds(0)),
      reconnectTimer_(client_->getIOExecutorProvider()->get()->createDeadlineTimer()) {}

std::string DagWatchSession::lookupCompatibleTopic(const std::string& topic) {
    constexpr std::string_view kTopicScheme = "topic://";
    if (topic.rfind(kTopicScheme, 0) == 0) {
        return "persistent://" + topic.substr(kTopicScheme.size());
    }
    return topic;
}

Future<SegmentLayout> DagWatchSession::start() {
    auto self = shared_from_this();
    client_->getConnection("", lookupCompatibleTopic(inputTopic_), static_cast<size_t>(sessionId_))
        .addListener([self](pulsar::Result result, const pulsar::ClientConnectionPtr& cnx) {
            if (result == pulsar::ResultOk) {
                self->attach(cnx);
            } else {
                self->initialLayoutPromise_.setError(
                    Error{result, "failed to get a connection for the scalable-topic lookup"});
            }
        });
    return initialLayoutPromise_.getFuture();
}

void DagWatchSession::attach(const pulsar::ClientConnectionPtr& cnx) {
    if (closed_) {
        return;
    }
    if (!cnx->supportsScalableTopics()) {
        Error error{ResultUnsupportedVersionError, "the broker does not support scalable topics"};
        if (!initialLayoutPromise_.setError(error)) {
            // Initial layout already delivered: keep retrying, another broker in
            // the cluster may still support the feature.
            LOG_WARN("[" << inputTopic_ << "] session " << sessionId_
                         << ": reconnect target broker does not support scalable topics");
            scheduleReconnect();
        }
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        cnx_ = cnx;
    }

    auto self = shared_from_this();
    bool registered = cnx->registerScalableTopicSession(
        sessionId_, [self](pulsar::Result result, const pulsar::proto::CommandScalableTopicUpdate* update) {
            self->handleSessionEvent(result, update);
        });
    if (!registered) {
        // The connection closed between acquisition and registration.
        handleConnectionClosed();
        return;
    }
    cnx->sendCommand(Commands::newScalableTopicLookup(sessionId_, inputTopic_, createIfMissing_));
    // A failed write closes the connection, which notifies the session through
    // the registry: no separate write-failure path is needed here.
}

void DagWatchSession::handleSessionEvent(pulsar::Result result,
                                         const pulsar::proto::CommandScalableTopicUpdate* update) {
    if (closed_) {
        return;
    }
    if (update == nullptr) {
        (void)result;
        handleConnectionClosed();
    } else if (update->has_error()) {
        handleServerError(*update);
    } else if (update->has_dag()) {
        handleDagUpdate(*update);
    } else {
        LOG_WARN("[" << inputTopic_ << "] session " << sessionId_
                     << ": update carries neither a DAG nor an error; ignoring");
    }
}

void DagWatchSession::handleDagUpdate(const pulsar::proto::CommandScalableTopicUpdate& update) {
    std::string resolved;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (update.has_resolved_topic_name()) {
            resolvedTopicName_ = update.resolved_topic_name();
        }
        resolved = !resolvedTopicName_.empty() ? resolvedTopicName_ : inputTopic_;
    }

    auto layout = SegmentLayout::fromProto(update.dag(), resolved);
    if (!layout) {
        LOG_ERROR("[" << inputTopic_ << "] session " << sessionId_
                      << ": discarding malformed DAG update: " << layout.error().message);
        // A malformed FIRST response must fail start() rather than hang it; after
        // that, keep the last good layout.
        initialLayoutPromise_.setError(layout.error());
        return;
    }

    SegmentLayout oldLayout;
    LayoutChangeListener listener;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        oldLayout = std::move(currentLayout_);
        currentLayout_ = *layout;
        listener = listener_;
    }

    // The broker confirmed the session is live and our state is fresh.
    backoff_.reset();

    LOG_INFO("[" << resolved << "] session " << sessionId_ << ": layout updated, epoch " << oldLayout.epoch()
                 << " -> " << layout->epoch() << ", " << layout->activeSegments().size()
                 << " active segments");

    initialLayoutPromise_.complete(*layout);
    if (listener) {
        listener(*layout, oldLayout);
    }
}

void DagWatchSession::handleServerError(const pulsar::proto::CommandScalableTopicUpdate& update) {
    const std::string message = update.has_message() ? update.message() : "scalable-topic lookup failed";
    LOG_ERROR("[" << inputTopic_ << "] session " << sessionId_ << ": broker reported " << update.error()
                  << ": " << message);
    // Before the initial layout this is a lookup failure and start() surfaces it;
    // afterwards broker-side errors are transient (a reconnect clears them) and
    // the connection-closed path picks them up.
    initialLayoutPromise_.setError(Error{toResult(update.error()), message});
}

void DagWatchSession::handleConnectionClosed() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cnx_.reset();
    }
    if (closed_) {
        return;
    }
    if (initialLayoutPromise_.setError(
            Error{ResultConnectError, "connection closed while waiting for the scalable-topic layout"})) {
        // The initial lookup never completed: surface the failure to start()'s
        // caller instead of retrying behind its back.
        return;
    }
    scheduleReconnect();
}

void DagWatchSession::scheduleReconnect() {
    if (closed_) {
        return;
    }
    auto delay = backoff_.next();
    LOG_INFO("[" << inputTopic_ << "] session " << sessionId_ << ": reconnecting the DAG watch in "
                 << std::chrono::duration_cast<std::chrono::milliseconds>(delay).count() << " ms");
    std::weak_ptr<DagWatchSession> weakSelf = shared_from_this();
    reconnectTimer_->expires_from_now(delay);
    reconnectTimer_->async_wait([weakSelf](const ASIO_ERROR& error) {
        auto self = weakSelf.lock();
        if (self && !error) {
            self->reconnect();
        }
    });
}

void DagWatchSession::reconnect() {
    if (closed_) {
        return;
    }
    auto self = shared_from_this();
    client_->getConnection("", lookupCompatibleTopic(inputTopic_), static_cast<size_t>(sessionId_))
        .addListener([self](pulsar::Result result, const pulsar::ClientConnectionPtr& cnx) {
            if (result == pulsar::ResultOk) {
                self->attach(cnx);
            } else {
                LOG_WARN("[" << self->inputTopic_ << "] session " << self->sessionId_
                             << ": DAG watch reconnect failed (" << result << "); will retry");
                self->scheduleReconnect();
            }
        });
}

SegmentLayout DagWatchSession::currentLayout() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return currentLayout_;
}

std::string DagWatchSession::topicName() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !resolvedTopicName_.empty() ? resolvedTopicName_ : inputTopic_;
}

void DagWatchSession::setLayoutChangeListener(LayoutChangeListener listener) {
    std::lock_guard<std::mutex> lock(mutex_);
    listener_ = std::move(listener);
}

void DagWatchSession::close() {
    if (closed_.exchange(true)) {
        return;
    }
    ASIO_ERROR ignored;
    reconnectTimer_->cancel(ignored);

    pulsar::ClientConnectionPtr cnx;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cnx = cnx_.lock();
        cnx_.reset();
    }
    if (cnx) {
        cnx->removeScalableTopicSession(sessionId_);
        cnx->sendCommand(Commands::newScalableTopicClose(sessionId_));
    }
}

}  // namespace pulsar::st

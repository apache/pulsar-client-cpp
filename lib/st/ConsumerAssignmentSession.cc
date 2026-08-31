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
#include "ConsumerAssignmentSession.h"

#include <chrono>
#include <utility>

#include "DagWatchSession.h"
#include "PulsarApi.pb.h"
#include "lib/Commands.h"
#include "lib/ExecutorService.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar::st {

namespace {

Result toSubscribeResult(pulsar::proto::ServerError error) {
    switch (error) {
        case pulsar::proto::TopicNotFound:
            return ResultTopicNotFound;
        case pulsar::proto::ConsumerBusy:
            return ResultConsumerBusy;
        default:
            return ResultUnknownError;
    }
}

ConsumerAssignment fromProto(const pulsar::proto::ScalableConsumerAssignment& proto) {
    ConsumerAssignment assignment;
    assignment.layoutEpoch = proto.layout_epoch();
    assignment.segments.reserve(proto.segments_size());
    for (int i = 0; i < proto.segments_size(); i++) {
        const auto& s = proto.segments(i);
        AssignedSegment assigned;
        assigned.segment.segmentId = s.segment_id();
        assigned.segment.range = HashRange{s.hash_start(), s.hash_end()};
        assigned.segment.segmentTopicName = s.segment_topic();
        assigned.ownedBucketRanges.reserve(s.bucket_ranges_size());
        for (int j = 0; j < s.bucket_ranges_size(); j++) {
            const auto& range = s.bucket_ranges(j);
            assigned.ownedBucketRanges.push_back(HashRange{static_cast<std::uint32_t>(range.start()),
                                                           static_cast<std::uint32_t>(range.end())});
        }
        assignment.segments.push_back(std::move(assigned));
    }
    return assignment;
}

}  // namespace

ConsumerAssignmentSession::ConsumerAssignmentSession(pulsar::ClientImplPtr client, std::string topic,
                                                     std::string subscription, std::string consumerName,
                                                     pulsar::ScalableConsumerType consumerType)
    : client_(std::move(client)),
      topic_(std::move(topic)),
      subscription_(std::move(subscription)),
      consumerName_(std::move(consumerName)),
      consumerType_(consumerType),
      consumerId_(client_->newConsumerId()),
      backoff_(std::chrono::milliseconds(100), std::chrono::seconds(30), std::chrono::milliseconds(0)),
      reconnectTimer_(client_->getIOExecutorProvider()->get()->createDeadlineTimer()) {}

Future<std::vector<AssignedSegment>> ConsumerAssignmentSession::start() {
    detail::Promise<ConsumerAssignment> attempt;
    auto self = shared_from_this();
    attempt.getFuture().addListener([self](const Expected<ConsumerAssignment>& result) {
        if (result) {
            // Mark before applying so a connection drop racing the response reconnects
            // instead of failing the (already satisfied) subscribe.
            self->sawInitialAssignment_.store(true);
            self->handleAssignmentReceived(*result);
            self->initialAssignmentPromise_.setValue(result->segments);
        } else {
            self->initialAssignmentPromise_.setError(result.error());
        }
    });
    connectAndSubscribe(attempt);
    return initialAssignmentPromise_.getFuture();
}

void ConsumerAssignmentSession::connectAndSubscribe(const detail::Promise<ConsumerAssignment>& promise) {
    // Resolve the controller leader through a one-shot DAG-watch lookup: scalable
    // topic URIs are not resolvable through the classic lookup service, and the
    // controller pushes assignment updates itself, so no long-lived layout watch is
    // needed. The watch is closed as soon as the layout arrives.
    auto self = shared_from_this();
    auto watch = std::make_shared<DagWatchSession>(client_, topic_, /*createIfMissing*/ true);
    watch->start().addListener([self, watch, promise](const Expected<SegmentLayout>& result) {
        watch->close();
        if (!result) {
            promise.setError(result.error());
            return;
        }
        if (self->closed_.load()) {
            promise.setError(Error{ResultAlreadyClosed, "consumer session closed"});
            return;
        }
        const bool useTls = self->client_->getServiceInfo().useTls();
        const auto& controllerUrl = useTls ? result->controllerBrokerUrlTls() : result->controllerBrokerUrl();
        // Behind a proxy the controller's advertised address is not directly reachable,
        // and before leader election completes there is no address at all: in both
        // cases connect through the regular lookup path — any broker forwards the
        // subscribe to the controller and relays assignment updates back.
        const bool useDirect = controllerUrl.has_value() && !controllerUrl->empty() &&
                               self->client_->getClientConfig().getProxyServiceUrl().empty();
        auto connectionFuture =
            useDirect ? self->client_->connect("", *controllerUrl, static_cast<size_t>(self->consumerId_))
                      : self->client_->getConnection("", DagWatchSession::lookupCompatibleTopic(self->topic_),
                                                     static_cast<size_t>(self->consumerId_));
        connectionFuture.addListener(
            [self, promise](pulsar::Result result, const pulsar::ClientConnectionPtr& cnx) {
                if (result == pulsar::ResultOk) {
                    self->subscribeOn(cnx, promise);
                } else {
                    promise.setError(Error{result, "failed to connect to the scalable-topics controller"});
                }
            });
    });
}

void ConsumerAssignmentSession::subscribeOn(const pulsar::ClientConnectionPtr& cnx,
                                            const detail::Promise<ConsumerAssignment>& promise) {
    if (closed_.load()) {
        promise.setError(Error{ResultAlreadyClosed, "consumer session closed"});
        return;
    }
    if (!cnx->supportsScalableTopics()) {
        promise.setError(Error{ResultUnsupportedVersionError, "the broker does not support scalable topics"});
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cnx_ = cnx;
    }
    std::weak_ptr<ConsumerAssignmentSession> weakSelf = weak_from_this();
    bool registered = cnx->registerScalableConsumerSession(
        consumerId_,
        [weakSelf](pulsar::Result result, const pulsar::proto::CommandScalableTopicAssignmentUpdate* update) {
            if (auto self = weakSelf.lock()) self->handleSessionEvent(result, update);
        });
    if (!registered) {
        // The connection closed between acquisition and registration.
        promise.setError(Error{ResultNotConnected, "connection closed before the consumer registration"});
        return;
    }
    const std::uint64_t requestId = client_->newRequestId();
    bool added = cnx->addScalableSubscribeRequest(
        requestId, [promise](pulsar::Result result,
                             const pulsar::proto::CommandScalableTopicSubscribeResponse* response) {
            if (result != pulsar::ResultOk) {
                promise.setError(Error{result, "connection closed before the subscribe response"});
                return;
            }
            if (response->has_error()) {
                promise.setError(
                    Error{toSubscribeResult(response->error()),
                          response->has_message() ? response->message() : "scalable-topic subscribe failed"});
                return;
            }
            if (!response->has_assignment()) {
                promise.setError(
                    Error{ResultUnknownError, "subscribe response carried neither assignment nor error"});
                return;
            }
            promise.setValue(fromProto(response->assignment()));
        });
    if (!added) {
        promise.setError(Error{ResultNotConnected, "connection closed before the subscribe request"});
        return;
    }
    // A failed write closes the connection, which fails the pending request and the
    // session registration through the close notification — no separate error path.
    cnx->sendCommand(Commands::newScalableTopicSubscribe(requestId, topic_, subscription_, consumerName_,
                                                         consumerId_, consumerType_));
}

void ConsumerAssignmentSession::handleSessionEvent(
    pulsar::Result result, const pulsar::proto::CommandScalableTopicAssignmentUpdate* update) {
    if (closed_.load()) {
        return;
    }
    if (result != pulsar::ResultOk) {
        handleConnectionClosed();
        return;
    }
    handleAssignmentReceived(fromProto(update->assignment()));
}

void ConsumerAssignmentSession::handleAssignmentReceived(const ConsumerAssignment& assignment) {
    std::vector<AssignedSegment> newSegments;
    std::vector<AssignedSegment> oldSegments;
    AssignmentChangeListener listener;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto epoch = static_cast<std::int64_t>(assignment.layoutEpoch);
        if (epoch < currentEpoch_) {
            LOG_INFO("[" << topic_ << "] consumer " << consumerId_ << ": ignoring stale assignment (epoch "
                         << epoch << " < " << currentEpoch_ << ")");
            return;
        }
        oldSegments = std::move(currentAssignment_);
        currentAssignment_ = assignment.segments;
        currentEpoch_ = epoch;
        newSegments = currentAssignment_;
        listener = listener_;
    }
    LOG_INFO("[" << topic_ << "] consumer " << consumerId_ << ": assignment updated (epoch "
                 << assignment.layoutEpoch << ", " << newSegments.size() << " segments)");
    if (listener) {
        listener(newSegments, oldSegments);
    }
}

void ConsumerAssignmentSession::handleConnectionClosed() {
    LOG_WARN("[" << topic_ << "] consumer " << consumerId_ << ": assignment session connection closed");
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cnx_.reset();
    }
    if (closed_.load()) {
        return;
    }
    if (!sawInitialAssignment_.load()) {
        // The initial subscribe never completed: surface the failure to the caller
        // rather than retrying silently. (First-writer-wins, so this cannot override
        // a response that raced the close.)
        initialAssignmentPromise_.setError(
            Error{ResultNotConnected, "connection closed before the initial assignment arrived"});
        return;
    }
    scheduleReconnect();
}

void ConsumerAssignmentSession::scheduleReconnect() {
    if (closed_.load()) {
        return;
    }
    auto delay = backoff_.next();
    LOG_INFO("[" << topic_ << "] consumer " << consumerId_ << ": reconnecting the assignment session in "
                 << std::chrono::duration_cast<std::chrono::milliseconds>(delay).count() << " ms");
    std::weak_ptr<ConsumerAssignmentSession> weakSelf = shared_from_this();
    reconnectTimer_->expires_from_now(delay);
    reconnectTimer_->async_wait([weakSelf](const ASIO_ERROR& error) {
        auto self = weakSelf.lock();
        if (self && !error) {
            self->reconnect();
        }
    });
}

void ConsumerAssignmentSession::reconnect() {
    if (closed_.load()) {
        return;
    }
    detail::Promise<ConsumerAssignment> attempt;
    auto self = shared_from_this();
    attempt.getFuture().addListener([self](const Expected<ConsumerAssignment>& result) {
        if (self->closed_.load()) {
            return;
        }
        if (result) {
            // Feed the response through the standard update path so the listener gets
            // the diff. Within the controller's grace period this is a no-op (same
            // segments); past it the controller has rebalanced and the listener
            // attaches/detaches accordingly.
            self->backoff_.reset();
            self->handleAssignmentReceived(*result);
        } else {
            LOG_WARN("[" << self->topic_ << "] consumer " << self->consumerId_
                         << ": assignment session reconnect failed (" << result.error() << "); will retry");
            self->scheduleReconnect();
        }
    });
    connectAndSubscribe(attempt);
}

std::vector<AssignedSegment> ConsumerAssignmentSession::currentAssignment() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return currentAssignment_;
}

void ConsumerAssignmentSession::setListener(AssignmentChangeListener listener) {
    std::vector<AssignedSegment> current;
    AssignmentChangeListener toReplay;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        listener_ = std::move(listener);
        current = currentAssignment_;
        toReplay = listener_;
    }
    // Replay the current assignment: an update that raced the registration would
    // otherwise be lost — there is no periodic refresh to recover it. Appliers are
    // idempotent, so a redundant replay is a no-op.
    if (toReplay) {
        toReplay(current, current);
    }
}

void ConsumerAssignmentSession::close() {
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
        // No wire command: the broker reaps the registration through its grace timer
        // on disconnect (Java parity).
        cnx->removeScalableConsumerSession(consumerId_);
    }
}

}  // namespace pulsar::st

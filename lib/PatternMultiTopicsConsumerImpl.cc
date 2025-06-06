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
#include "PatternMultiTopicsConsumerImpl.h"

#include "ClientImpl.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "LookupService.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

using std::chrono::seconds;

PatternMultiTopicsConsumerImpl::PatternMultiTopicsConsumerImpl(
    const ClientImplPtr& client, const std::string& pattern, CommandGetTopicsOfNamespace_Mode getTopicsMode,
    const std::vector<std::string>& topics, const std::string& subscriptionName,
    const ConsumerConfiguration& conf, const LookupServicePtr& lookupServicePtr_,
    const ConsumerInterceptorsPtr& interceptors)
    : MultiTopicsConsumerImpl(client, topics, subscriptionName, TopicName::get(pattern), conf,
                              lookupServicePtr_, interceptors),
      patternString_(pattern),
      pattern_(PULSAR_REGEX_NAMESPACE::regex(TopicName::removeDomain(pattern))),
      getTopicsMode_(getTopicsMode),
      autoDiscoveryTimer_(client->getIOExecutorProvider()->get()->createDeadlineTimer()),
      autoDiscoveryRunning_(false) {
    namespaceName_ = TopicName::get(pattern)->getNamespaceName();
}

const PULSAR_REGEX_NAMESPACE::regex PatternMultiTopicsConsumerImpl::getPattern() { return pattern_; }

void PatternMultiTopicsConsumerImpl::resetAutoDiscoveryTimer() {
    autoDiscoveryRunning_ = false;
    autoDiscoveryTimer_->expires_from_now(seconds(conf_.getPatternAutoDiscoveryPeriod()));

    auto weakSelf = weak_from_this();
    autoDiscoveryTimer_->async_wait([weakSelf](const ASIO_ERROR& err) {
        if (auto self = weakSelf.lock()) {
            self->autoDiscoveryTimerTask(err);
        }
    });
}

void PatternMultiTopicsConsumerImpl::autoDiscoveryTimerTask(const ASIO_ERROR& err) {
    if (err == ASIO::error::operation_aborted) {
        LOG_DEBUG(getName() << "Timer cancelled: " << err.message());
        return;
    } else if (err) {
        LOG_ERROR(getName() << "Timer error: " << err.message());
        return;
    }

    const auto state = state_.load();
    if (state != Ready) {
        LOG_ERROR("Error in autoDiscoveryTimerTask consumer state not ready: " << state);
        resetAutoDiscoveryTimer();
        return;
    }

    if (autoDiscoveryRunning_) {
        LOG_DEBUG("autoDiscoveryTimerTask still running, cancel this running. ");
        return;
    }

    autoDiscoveryRunning_ = true;

    // already get namespace from pattern.
    assert(namespaceName_);

    lookupServicePtr_->getTopicsOfNamespaceAsync(namespaceName_, getTopicsMode_)
        .addListener(std::bind(&PatternMultiTopicsConsumerImpl::timerGetTopicsOfNamespace, this,
                               std::placeholders::_1, std::placeholders::_2));
}

void PatternMultiTopicsConsumerImpl::timerGetTopicsOfNamespace(Result result,
                                                               const NamespaceTopicsPtr& topics) {
    if (result != ResultOk) {
        LOG_ERROR("Error in Getting topicsOfNameSpace. result: " << result);
        resetAutoDiscoveryTimer();
        return;
    }

    NamespaceTopicsPtr newTopics = PatternMultiTopicsConsumerImpl::topicsPatternFilter(*topics, pattern_);
    // get old topics in consumer:
    NamespaceTopicsPtr oldTopics = std::make_shared<std::vector<std::string>>();
    for (std::map<std::string, int>::iterator it = topicsPartitions_.begin(); it != topicsPartitions_.end();
         it++) {
        oldTopics->push_back(it->first);
    }
    NamespaceTopicsPtr topicsAdded = topicsListsMinus(*newTopics, *oldTopics);
    NamespaceTopicsPtr topicsRemoved = topicsListsMinus(*oldTopics, *newTopics);

    // callback method when removed topics all un-subscribed.
    ResultCallback topicsRemovedCallback = [this](Result result) {
        if (result != ResultOk) {
            LOG_ERROR("Failed to unsubscribe topics: " << result);
        }
        resetAutoDiscoveryTimer();
    };

    // callback method when added topics all subscribed.
    ResultCallback topicsAddedCallback = [this, topicsRemoved, topicsRemovedCallback](Result result) {
        if (result == ResultOk) {
            if (messageListener_) {
                resumeMessageListener();
            }
            // call to unsubscribe all removed topics.
            onTopicsRemoved(topicsRemoved, topicsRemovedCallback);
        } else {
            resetAutoDiscoveryTimer();
        }
    };

    // call to subscribe new added topics, then in its callback do unsubscribe
    onTopicsAdded(topicsAdded, topicsAddedCallback);
}

void PatternMultiTopicsConsumerImpl::onTopicsAdded(const NamespaceTopicsPtr& addedTopics,
                                                   const ResultCallback& callback) {
    // start call subscribeOneTopicAsync for each single topic

    if (addedTopics->empty()) {
        LOG_DEBUG("no topics need subscribe");
        callback(ResultOk);
        return;
    }
    int topicsNumber = addedTopics->size();

    std::shared_ptr<std::atomic<int>> topicsNeedCreate = std::make_shared<std::atomic<int>>(topicsNumber);
    // subscribe for each passed in topic
    for (std::vector<std::string>::const_iterator itr = addedTopics->begin(); itr != addedTopics->end();
         itr++) {
        MultiTopicsConsumerImpl::subscribeOneTopicAsync(*itr).addListener(
            std::bind(&PatternMultiTopicsConsumerImpl::handleOneTopicAdded, this, std::placeholders::_1, *itr,
                      topicsNeedCreate, callback));
    }
}

void PatternMultiTopicsConsumerImpl::handleOneTopicAdded(
    Result result, const std::string& topic, const std::shared_ptr<std::atomic<int>>& topicsNeedCreate,
    const ResultCallback& callback) {
    (*topicsNeedCreate)--;

    if (result != ResultOk) {
        LOG_ERROR("Failed when subscribed to topic " << topic << "  Error - " << result);
        callback(result);
        return;
    }

    if (topicsNeedCreate->load() == 0) {
        LOG_DEBUG("Subscribed all new added topics");
        callback(result);
    }
}

void PatternMultiTopicsConsumerImpl::onTopicsRemoved(const NamespaceTopicsPtr& removedTopics,
                                                     const ResultCallback& callback) {
    // start call subscribeOneTopicAsync for each single topic
    if (removedTopics->empty()) {
        LOG_DEBUG("no topics need unsubscribe");
        callback(ResultOk);
        return;
    }

    auto topicsNeedUnsub = std::make_shared<std::atomic<int>>(removedTopics->size());

    ResultCallback oneTopicUnsubscribedCallback = [topicsNeedUnsub, callback](Result result) {
        (*topicsNeedUnsub)--;

        if (result != ResultOk) {
            LOG_ERROR("Failed when unsubscribe to one topic.  Error - " << result);
            callback(result);
            return;
        }

        if (topicsNeedUnsub->load() == 0) {
            LOG_DEBUG("unSubscribed all needed topics");
            callback(result);
        }
    };

    // unsubscribe for each passed in topic
    for (std::vector<std::string>::const_iterator itr = removedTopics->begin(); itr != removedTopics->end();
         itr++) {
        MultiTopicsConsumerImpl::unsubscribeOneTopicAsync(*itr, oneTopicUnsubscribedCallback);
    }
}

NamespaceTopicsPtr PatternMultiTopicsConsumerImpl::topicsPatternFilter(
    const std::vector<std::string>& topics, const PULSAR_REGEX_NAMESPACE::regex& pattern) {
    NamespaceTopicsPtr topicsResultPtr = std::make_shared<std::vector<std::string>>();
    for (const auto& topicStr : topics) {
        auto topic = TopicName::removeDomain(topicStr);
        if (PULSAR_REGEX_NAMESPACE::regex_match(topic, pattern)) {
            topicsResultPtr->push_back(topicStr);
        }
    }
    return topicsResultPtr;
}

NamespaceTopicsPtr PatternMultiTopicsConsumerImpl::topicsListsMinus(std::vector<std::string>& list1,
                                                                    std::vector<std::string>& list2) {
    NamespaceTopicsPtr topicsResultPtr = std::make_shared<std::vector<std::string>>();
    std::remove_copy_if(list1.begin(), list1.end(), std::back_inserter(*topicsResultPtr),
                        [&list2](const std::string& arg) {
                            return (std::find(list2.begin(), list2.end(), arg) != list2.end());
                        });

    return topicsResultPtr;
}

void PatternMultiTopicsConsumerImpl::start() {
    MultiTopicsConsumerImpl::start();

    LOG_DEBUG("PatternMultiTopicsConsumerImpl start autoDiscoveryTimer_.");

    if (conf_.getPatternAutoDiscoveryPeriod() > 0) {
        autoDiscoveryTimer_->expires_from_now(seconds(conf_.getPatternAutoDiscoveryPeriod()));
        auto weakSelf = weak_from_this();
        autoDiscoveryTimer_->async_wait([weakSelf](const ASIO_ERROR& err) {
            if (auto self = weakSelf.lock()) {
                self->autoDiscoveryTimerTask(err);
            }
        });
    }
}

PatternMultiTopicsConsumerImpl::~PatternMultiTopicsConsumerImpl() {
    cancelTimers();
    internalShutdown();
}

void PatternMultiTopicsConsumerImpl::closeAsync(const ResultCallback& callback) {
    cancelTimers();
    MultiTopicsConsumerImpl::closeAsync(callback);
}

void PatternMultiTopicsConsumerImpl::cancelTimers() noexcept {
    ASIO_ERROR ec;
    autoDiscoveryTimer_->cancel(ec);
}

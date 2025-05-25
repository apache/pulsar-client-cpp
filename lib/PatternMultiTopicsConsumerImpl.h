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
#ifndef PULSAR_PATTERN_MULTI_TOPICS_CONSUMER_HEADER
#define PULSAR_PATTERN_MULTI_TOPICS_CONSUMER_HEADER
#include <memory>
#include <string>
#include <vector>

#include "AsioTimer.h"
#include "LookupDataResult.h"
#include "MultiTopicsConsumerImpl.h"
#include "NamespaceName.h"
#include "TopicName.h"

#ifdef PULSAR_USE_BOOST_REGEX
#include <boost/regex.hpp>
#define PULSAR_REGEX_NAMESPACE boost
#else
#include <regex>
#define PULSAR_REGEX_NAMESPACE std
#endif

namespace pulsar {

class ClientImpl;
using ClientImplPtr = std::shared_ptr<ClientImpl>;
using NamespaceTopicsPtr = std::shared_ptr<std::vector<std::string>>;

class PatternMultiTopicsConsumerImpl : public MultiTopicsConsumerImpl {
   public:
    // currently we support topics under same namespace, so `patternString` is a regex,
    // which only contains after namespace part.
    // when subscribe, client will first get all topics that match given pattern.
    // `topics` contains the topics that match `patternString`.
    PatternMultiTopicsConsumerImpl(const ClientImplPtr& client, const std::string& patternString,
                                   CommandGetTopicsOfNamespace_Mode getTopicsMode,
                                   const std::vector<std::string>& topics,
                                   const std::string& subscriptionName, const ConsumerConfiguration& conf,
                                   const LookupServicePtr& lookupServicePtr_,
                                   const ConsumerInterceptorsPtr& interceptors);
    ~PatternMultiTopicsConsumerImpl() override;

    const PULSAR_REGEX_NAMESPACE::regex getPattern();

    void autoDiscoveryTimerTask(const ASIO_ERROR& err);

    // filter input `topics` with given `pattern`, return matched topics. Do not match topic domain.
    static NamespaceTopicsPtr topicsPatternFilter(const std::vector<std::string>& topics,
                                                  const PULSAR_REGEX_NAMESPACE::regex& pattern);

    // Find out topics, which are in `list1` but not in `list2`.
    static NamespaceTopicsPtr topicsListsMinus(std::vector<std::string>& list1,
                                               std::vector<std::string>& list2);

    void closeAsync(const ResultCallback& callback) override;
    void start() override;

   private:
    const std::string patternString_;
    const PULSAR_REGEX_NAMESPACE::regex pattern_;
    const CommandGetTopicsOfNamespace_Mode getTopicsMode_;
    typedef std::shared_ptr<ASIO::steady_timer> TimerPtr;
    TimerPtr autoDiscoveryTimer_;
    bool autoDiscoveryRunning_;
    NamespaceNamePtr namespaceName_;

    void cancelTimers() noexcept;
    void resetAutoDiscoveryTimer();
    void timerGetTopicsOfNamespace(Result result, const NamespaceTopicsPtr& topics);
    void onTopicsAdded(const NamespaceTopicsPtr& addedTopics, const ResultCallback& callback);
    void onTopicsRemoved(const NamespaceTopicsPtr& removedTopics, const ResultCallback& callback);
    void handleOneTopicAdded(Result result, const std::string& topic,
                             const std::shared_ptr<std::atomic<int>>& topicsNeedCreate,
                             const ResultCallback& callback);

    std::weak_ptr<PatternMultiTopicsConsumerImpl> weak_from_this() noexcept {
        return std::static_pointer_cast<PatternMultiTopicsConsumerImpl>(shared_from_this());
    }
};

}  // namespace pulsar
#endif  // PULSAR_PATTERN_MULTI_TOPICS_CONSUMER_HEADER

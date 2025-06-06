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
#include "MultiTopicsBrokerConsumerStatsImpl.h"

#include <algorithm>
#include <numeric>
#include <sstream>

using namespace pulsar;

const std::string MultiTopicsBrokerConsumerStatsImpl::DELIMITER = ";";

MultiTopicsBrokerConsumerStatsImpl::MultiTopicsBrokerConsumerStatsImpl(size_t size) {
    statsList_.resize(size);
}

bool MultiTopicsBrokerConsumerStatsImpl::isValid() const {
    bool isValid = true;
    for (int i = 0; i < statsList_.size(); i++) {
        isValid = isValid && statsList_[i].isValid();
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const MultiTopicsBrokerConsumerStatsImpl& obj) {
    os << "\nMultiTopicsBrokerConsumerStatsImpl ["
       << "validTill_ = " << obj.isValid() << ", msgRateOut_ = " << obj.getMsgRateOut()
       << ", msgThroughputOut_ = " << obj.getMsgThroughputOut()
       << ", msgRateRedeliver_ = " << obj.getMsgRateRedeliver()
       << ", consumerName_ = " << obj.getConsumerName()
       << ", availablePermits_ = " << obj.getAvailablePermits()
       << ", unackedMessages_ = " << obj.getUnackedMessages()
       << ", blockedConsumerOnUnackedMsgs_ = " << obj.isBlockedConsumerOnUnackedMsgs()
       << ", address_ = " << obj.getAddress() << ", connectedSince_ = " << obj.getConnectedSince()
       << ", type_ = " << obj.getType() << ", msgRateExpired_ = " << obj.getMsgRateExpired()
       << ", msgBacklog_ = " << obj.getMsgBacklog() << "]";
    return os;
}

double MultiTopicsBrokerConsumerStatsImpl::getMsgRateOut() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgRateOut();
    }
    return sum;
}

double MultiTopicsBrokerConsumerStatsImpl::getMsgThroughputOut() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgThroughputOut();
    }
    return sum;
}

double MultiTopicsBrokerConsumerStatsImpl::getMsgRateRedeliver() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgRateRedeliver();
    }
    return sum;
}

const std::string MultiTopicsBrokerConsumerStatsImpl::getConsumerName() const {
    std::string str;
    for (int i = 0; i < statsList_.size(); i++) {
        str += statsList_[i].getConsumerName() + DELIMITER;
    }
    return str;
}

uint64_t MultiTopicsBrokerConsumerStatsImpl::getAvailablePermits() const {
    uint64_t sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getAvailablePermits();
    }
    return sum;
}

uint64_t MultiTopicsBrokerConsumerStatsImpl::getUnackedMessages() const {
    uint64_t sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getUnackedMessages();
    }
    return sum;
}

bool MultiTopicsBrokerConsumerStatsImpl::isBlockedConsumerOnUnackedMsgs() const {
    if (statsList_.size() == 0) {
        return false;
    }

    return isValid();
}

const std::string MultiTopicsBrokerConsumerStatsImpl::getAddress() const {
    std::stringstream str;
    for (int i = 0; i < statsList_.size(); i++) {
        str << statsList_[i].getAddress() << DELIMITER;
    }
    return str.str();
}

const std::string MultiTopicsBrokerConsumerStatsImpl::getConnectedSince() const {
    std::stringstream str;
    for (int i = 0; i < statsList_.size(); i++) {
        str << statsList_[i].getConnectedSince() << DELIMITER;
    }
    return str.str();
}

const ConsumerType MultiTopicsBrokerConsumerStatsImpl::getType() const {
    if (!statsList_.size()) {
        return ConsumerExclusive;
    }
    return statsList_[0].getType();
}

double MultiTopicsBrokerConsumerStatsImpl::getMsgRateExpired() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgRateExpired();
    }
    return sum;
}

uint64_t MultiTopicsBrokerConsumerStatsImpl::getMsgBacklog() const {
    uint64_t sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgBacklog();
    }
    return sum;
}

BrokerConsumerStats MultiTopicsBrokerConsumerStatsImpl::getBrokerConsumerStats(int index) {
    return statsList_[index];
}

void MultiTopicsBrokerConsumerStatsImpl::add(const BrokerConsumerStats& stats, int index) {
    statsList_[index] = stats;
}

void MultiTopicsBrokerConsumerStatsImpl::clear() { statsList_.clear(); }

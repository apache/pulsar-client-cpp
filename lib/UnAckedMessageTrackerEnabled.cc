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
#include "UnAckedMessageTrackerEnabled.h"

#include <functional>

#include "ChunkMessageIdImpl.h"
#include "ClientConnection.h"
#include "ClientImpl.h"
#include "ConsumerImplBase.h"
#include "ExecutorService.h"
#include "LogUtils.h"
#include "MessageIdUtil.h"

DECLARE_LOG_OBJECT();

namespace pulsar {

void UnAckedMessageTrackerEnabled::timeoutHandler() {
    timeoutHandlerHelper();
    auto client = client_.lock();
    if (client == nullptr) {
        return;
    }
    ExecutorServicePtr executorService = client->getIOExecutorProvider()->get();
    timer_ = executorService->createDeadlineTimer();
    timer_->expires_after(std::chrono::milliseconds(tickDurationInMs_));
    auto weakSelf = weak_from_this();
    timer_->async_wait([weakSelf](const ASIO_ERROR& ec) {
        if (auto self = weakSelf.lock(); self && !ec) {
            self->timeoutHandler();
        }
    });
}

void UnAckedMessageTrackerEnabled::timeoutHandlerHelper() {
    std::unique_lock<std::recursive_mutex> acquire(lock_);
    LOG_DEBUG("UnAckedMessageTrackerEnabled::timeoutHandlerHelper invoked for consumerPtr_ "
              << consumerReference_.getName().c_str());

    std::set<MessageId> headPartition = timePartitions.front();
    timePartitions.pop_front();

    std::set<MessageId> msgIdsToRedeliver;
    if (!headPartition.empty()) {
        auto cnx = consumerReference_.getCnx().lock();
        if (cnx) {
            LOG_WARN(consumerReference_.getName()
                     << " Unacked messages timeout: " << headPartition.size() << " messages not acked within "
                     << timeoutMs_ << " ms, connection: " << cnx->cnxString());
        } else {
            LOG_WARN(consumerReference_.getName()
                     << " Unacked messages timeout: " << headPartition.size() << " messages not acked within "
                     << timeoutMs_ << " ms, no connection");
        }
        for (auto it = headPartition.begin(); it != headPartition.end(); it++) {
            msgIdsToRedeliver.insert(*it);
            messageIdPartitionMap.erase(*it);
        }
    }
    headPartition.clear();
    timePartitions.push_back(headPartition);

    if (msgIdsToRedeliver.size() > 0) {
        // redeliverUnacknowledgedMessages() may call clear() that acquire the lock again, so we should unlock
        // here to avoid deadlock
        acquire.unlock();
        consumerReference_.redeliverUnacknowledgedMessages(msgIdsToRedeliver);
    }
}

UnAckedMessageTrackerEnabled::UnAckedMessageTrackerEnabled(long timeoutMs, const ClientImplPtr& client,
                                                           ConsumerImplBase& consumer)
    : UnAckedMessageTrackerEnabled(timeoutMs, timeoutMs, client, consumer) {}

UnAckedMessageTrackerEnabled::UnAckedMessageTrackerEnabled(long timeoutMs, long tickDurationInMs,
                                                           const ClientImplPtr& client,
                                                           ConsumerImplBase& consumer)
    : consumerReference_(consumer),
      client_(client),
      timeoutMs_(timeoutMs),
      tickDurationInMs_(timeoutMs >= tickDurationInMs ? tickDurationInMs : timeoutMs) {
    const int blankPartitions =
        static_cast<int>(std::ceil(static_cast<double>(timeoutMs_) / tickDurationInMs_)) + 1;

    for (int i = 0; i < blankPartitions; i++) {
        std::set<MessageId> msgIds;
        timePartitions.push_back(msgIds);
    }
}

void UnAckedMessageTrackerEnabled::start() { timeoutHandler(); }

bool UnAckedMessageTrackerEnabled::add(const MessageId& msgId) {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    // For ChunkMessageId, skip discardBatch to preserve the original ChunkMessageIdImpl.
    //
    // ChunkMessageIdImpl stores all chunk entries internally (chunkedMessageIds_), and its
    // ledgerId/entryId are set to the last chunk's position. If discardBatch is applied, a
    // new plain MessageIdImpl would be created, losing all chunk entries information.
    //
    // Although the set/map key only reflects the last chunk's position, the MessageId object
    // itself retains the full ChunkMessageIdImpl via its impl_ pointer. So when ackTimeout
    // triggers and the MessageId is passed to redeliverMessages(), it can be expanded into
    // all chunk entries for redelivery, ensuring the broker redelivers the complete chunked
    // message.
    auto id = std::dynamic_pointer_cast<ChunkMessageIdImpl>(msgId.impl_) ? msgId : discardBatch(msgId);
    if (messageIdPartitionMap.count(id) == 0) {
        std::set<MessageId>& partition = timePartitions.back();
        bool emplace = messageIdPartitionMap.emplace(id, partition).second;
        bool insert = partition.insert(id).second;
        return emplace && insert;
    }
    return false;
}

bool UnAckedMessageTrackerEnabled::isEmpty() {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    return messageIdPartitionMap.empty();
}

bool UnAckedMessageTrackerEnabled::remove(const MessageId& msgId) {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    // Keep consistent with add(): skip discardBatch for ChunkMessageId to ensure
    // the same key is used for lookup and removal.
    auto id = std::dynamic_pointer_cast<ChunkMessageIdImpl>(msgId.impl_) ? msgId : discardBatch(msgId);
    bool removed = false;

    std::map<MessageId, std::set<MessageId>&>::iterator exist = messageIdPartitionMap.find(id);
    if (exist != messageIdPartitionMap.end()) {
        removed = exist->second.erase(id);
        messageIdPartitionMap.erase(exist);
    }
    return removed;
}

void UnAckedMessageTrackerEnabled::remove(const MessageIdList& msgIds) {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    for (const auto& msgId : msgIds) {
        remove(msgId);
    }
}

long UnAckedMessageTrackerEnabled::size() {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    return messageIdPartitionMap.size();
}

void UnAckedMessageTrackerEnabled::removeMessagesTill(const MessageId& msgId) {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    for (auto it = messageIdPartitionMap.begin(); it != messageIdPartitionMap.end();) {
        MessageId msgIdInMap = it->first;
        if (msgIdInMap <= msgId) {
            it->second.erase(msgIdInMap);
            messageIdPartitionMap.erase(it++);
        } else {
            it++;
        }
    }
}

// this is only for MultiTopicsConsumerImpl, when un-subscribe a single topic, should remove all it's message.
void UnAckedMessageTrackerEnabled::removeTopicMessage(const std::string& topic) {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    for (auto it = messageIdPartitionMap.begin(); it != messageIdPartitionMap.end();) {
        MessageId msgIdInMap = it->first;
        if (msgIdInMap.getTopicName().compare(topic) == 0) {
            it->second.erase(msgIdInMap);
            messageIdPartitionMap.erase(it++);
        } else {
            it++;
        }
    }
}

void UnAckedMessageTrackerEnabled::clear() {
    std::lock_guard<std::recursive_mutex> acquire(lock_);
    messageIdPartitionMap.clear();
    for (auto it = timePartitions.begin(); it != timePartitions.end(); it++) {
        it->clear();
    }
}

void UnAckedMessageTrackerEnabled::stop() {
    if (timer_) {
        cancelTimer(*timer_);
    }
}
} /* namespace pulsar */

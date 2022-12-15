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

#include "TableViewImpl.h"

#include "LogUtils.h"
#include "ReaderImpl.h"
#include "TimeUtils.h"

namespace pulsar {

DECLARE_LOG_OBJECT()

TableViewImpl::TableViewImpl(const ClientImplPtr client, const std::string& topic,
                             const TableViewConfiguration& conf)
    : client_(client), topic_(topic), conf_(conf) {}

Future<Result, TableViewImplPtr> TableViewImpl::start() {
    Promise<Result, TableViewImplPtr> promise;
    TableViewImplPtr self = shared_from_this();

    ReaderConfiguration readerConfiguration;
    readerConfiguration.setSchema(conf_.getSchemaInfo());
    readerConfiguration.setReadCompacted(true);
    readerConfiguration.setInternalSubscriptionName(conf_.getSubscriptionName());

    ReaderCallback readerCallback = [self, promise](Result res, Reader reader) {
        if (res == ResultOk) {
            self->reader_ = reader.impl_;
            self->readAllExistingMessages(promise, TimeUtils::currentTimeMillis(), 0);
        } else {
            promise.setFailed(res);
        }
    };
    client_->createReaderAsync(topic_, MessageId::earliest(), readerConfiguration, readerCallback);
    return promise.getFuture();
}

bool TableViewImpl::retrieveValue(const std::string& key, std::string& value) {
    auto optValue = data_.remove(key);
    if (optValue) {
        value = optValue.value();
        return true;
    }
    return false;
}

bool TableViewImpl::getValue(const std::string& key, std::string& value) const {
    auto optValue = data_.find(key);
    if (optValue) {
        value = optValue.value();
        return true;
    }
    return false;
}

bool TableViewImpl::containsKey(const std::string& key) const { return data_.find(key) != boost::none; }

std::unordered_map<std::string, std::string> TableViewImpl::snapshot() { return data_.move(); }

std::size_t TableViewImpl::size() const { return data_.size(); }

void TableViewImpl::forEach(TableViewAction action) { data_.forEach(action); }

void TableViewImpl::forEachAndListen(TableViewAction action) {
    Lock lock(listenersMutex_);
    data_.forEach(action);
    listeners_.emplace_back(action);
}

void TableViewImpl::closeAsync(ResultCallback callback) {
    if (reader_) {
        reader_->closeAsync([callback, this](Result result) {
            reader_.reset();
            data_.clear();
            callback(result);
        });
    } else {
        callback(ResultConsumerNotInitialized);
    }
}

void TableViewImpl::handleMessage(const Message& msg) {
    if (msg.hasPartitionKey()) {
        LOG_DEBUG("Applying message from " << topic_ << " key=" << msg.getPartitionKey()
                                           << " value=" << msg.getDataAsString())

        Lock lock(listenersMutex_);
        if (msg.getDataAsString().empty()) {
            data_.remove(msg.getPartitionKey());
        } else {
            data_.emplace(msg.getPartitionKey(), msg.getDataAsString());
        }

        for (const auto& listener : listeners_) {
            try {
                listener(msg.getPartitionKey(), msg.getDataAsString());
            } catch (const std::exception& exc) {
                LOG_ERROR("Table view listener raised an exception: " << exc.what());
            }
        }
    }
}

void TableViewImpl::readAllExistingMessages(Promise<Result, TableViewImplPtr> promise, long startTime,
                                            long messagesRead) {
    auto self = shared_from_this();
    reader_->hasMessageAvailableAsync(
        [self, promise, startTime, messagesRead](Result result, bool hasMessage) {
            if (result != ResultOk) {
                promise.setFailed(result);
            }
            if (hasMessage) {
                Message msg;
                self->reader_->readNextAsync(
                    [self, promise, startTime, messagesRead](Result res, const Message& msg) {
                        if (res != ResultOk) {
                            promise.setFailed(res);
                        } else {
                            self->handleMessage(msg);
                            auto tmpMessagesRead = messagesRead + 1;
                            self->readAllExistingMessages(promise, startTime, tmpMessagesRead);
                        }
                    });
            } else {
                long endTime = TimeUtils::currentTimeMillis();
                long durationMillis = endTime - startTime;
                LOG_INFO("Started table view for " << self->topic_ << "Replayed: " << messagesRead
                                                   << " message in " << durationMillis << " millis");
                promise.setValue(self);
                self->readTailMessage();
            }
        });
}

void TableViewImpl::readTailMessage() {
    auto self = shared_from_this();
    reader_->readNextAsync([self](Result result, const Message& msg) {
        if (result == ResultOk) {
            self->handleMessage(msg);
            self->readTailMessage();
        } else {
            LOG_INFO("Reader " << self->topic_ << " was interrupted: " << result);
        }
    });
}

}  // namespace pulsar
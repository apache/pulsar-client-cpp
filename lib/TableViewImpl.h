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

#ifndef PULSAR_CPP_TABLEVIEW_IMPL_H
#define PULSAR_CPP_TABLEVIEW_IMPL_H

#include <pulsar/Message.h>
#include <pulsar/TableView.h>

#include <map>

#include "Future.h"
#include "SynchronizedHashMap.h"

namespace pulsar {

class ClientImpl;
typedef std::shared_ptr<ClientImpl> ClientImplPtr;

class TableViewImpl;
typedef std::shared_ptr<TableViewImpl> TableViewImplPtr;

class ReaderImpl;
typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;

class TableViewImpl : public std::enable_shared_from_this<TableViewImpl> {
   public:
    TableViewImpl(ClientImplPtr client, const std::string& topic, const TableViewConfiguration& conf);

    ~TableViewImpl(){};

    Future<Result, TableViewImplPtr> start();

    bool retrieveValue(const std::string& key, std::string& value);

    bool getValue(const std::string& key, std::string& value) const;

    bool containsKey(const std::string& key) const;

    std::unordered_map<std::string, std::string> snapshot();

    std::size_t size() const;

    void forEach(TableViewAction action);

    void forEachAndListen(TableViewAction action);

    void closeAsync(ResultCallback callback);

   private:
    using MutexType = std::mutex;
    using Lock = std::lock_guard<MutexType>;

    const ClientImplPtr client_;
    const std::string topic_;
    const TableViewConfiguration conf_;
    ReaderImplPtr reader_;

    MutexType listenersMutex_;
    std::vector<TableViewAction> listeners_;
    SynchronizedHashMap<std::string, std::string> data_;

    void handleMessage(const Message& msg);
    void readAllExistingMessages(Promise<Result, TableViewImplPtr> promise, long startTime,
                                 long messagesRead);
    void readTailMessage();
};
}  // namespace pulsar

#endif  // PULSAR_CPP_TABLEVIEW_IMPL_H
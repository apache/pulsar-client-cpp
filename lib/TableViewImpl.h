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

#include <map>

#include "ClientImpl.h"
#include "SynchronizedHashMap.h"

namespace pulsar {

class TableViewImpl : public std::enable_shared_from_this<TableViewImpl> {
   public:
    TableViewImpl(const ClientImplPtr client, const std::string& topic, const TableViewConfiguration& conf);

    Future<Result, TableViewImplPtr> start();

   private:
    const ClientImplPtr client_;
    const std::string topic_;
    const TableViewConfiguration conf_;
    Reader reader_;

    SynchronizedHashMap<std::string, std::string> data_;
    // todo 只支持持久化topic:
    // https://github.com/apache/pulsar/pull/18375/files#diff-4c15ad9e44a19d82a9c40274cad3950b0af335cb40bd537b130300510a6857d8
};
}  // namespace pulsar

#endif  // PULSAR_CPP_TABLEVIEW_IMPL_H
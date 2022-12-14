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

namespace pulsar {

TableViewImpl::TableViewImpl(const ClientImplPtr client, const std::string& topic,
                             const TableViewConfiguration& conf)
    : client_(client), topic_(topic), conf_(conf) {}

Future<Result, TableViewImplPtr> TableViewImpl::start() {
    Promise<Result, TableViewImplPtr> promise;
    TableViewImplPtr self = shared_from_this();

    ReaderConfiguration readerConfiguration;
    readerConfiguration.setSchema(conf_.getSchemaInfo());
    // TODO readerConfiguration.setAutoUpdatePatition();
    readerConfiguration.setReadCompacted(true);
    readerConfiguration.setInternalSubscriptionName(conf_.getSubscriptionName());

    ReaderCallback readerCallback = [self, &promise](Result res, Reader reader) {
        // todo log
        if (res == ResultOk) {
            self->reader_ = reader;
            promise.setValue(self);
        } else {
            promise.setFailed(res);
        }
    };
    client_->createReaderAsync(topic_, MessageId::earliest(), readerConfiguration, readerCallback);
    return promise.getFuture();
}
}  // namespace pulsar
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
#include <pulsar/TableView.h>

#include "TableViewImpl.h"
#include "Utils.h"

namespace pulsar {

TableView::TableView() {}

TableView::TableView(TableViewImplPtr impl) : impl_(impl) {}

bool TableView::retrieveValue(const std::string& key, std::string& value) {
    if (impl_) {
        return impl_->retrieveValue(key, value);
    }
    return false;
}

bool TableView::getValue(const std::string& key, std::string& value) const {
    if (impl_) {
        return impl_->getValue(key, value);
    }
    return false;
}

bool TableView::containsKey(const std::string& key) const {
    if (impl_) {
        return impl_->containsKey(key);
    }
    return false;
}

std::unordered_map<std::string, std::string> TableView::snapshot() {
    if (impl_) {
        return impl_->snapshot();
    }
    return {};
}

std::size_t TableView::size() const {
    if (impl_) {
        return impl_->size();
    }
    return 0;
}

void TableView::forEach(TableViewAction action) {
    if (impl_) {
        impl_->forEach(action);
    }
}

void TableView::forEachAndListen(TableViewAction action) {
    if (impl_) {
        impl_->forEachAndListen(action);
    }
}

void TableView::closeAsync(ResultCallback callback) {
    if (!impl_) {
        callback(ResultConsumerNotInitialized);
        return;
    }

    impl_->closeAsync(callback);
}

Result TableView::close() {
    if (!impl_) {
        return ResultConsumerNotInitialized;
    }
    Promise<bool, Result> promise;
    impl_->closeAsync(WaitForCallback(promise));
    Result result;
    promise.getFuture().get(result);
    return result;
}

}  // namespace pulsar

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

namespace pulsar {

const static std::string emptyString;

TableView::TableView() {}
bool TableView::retrieveValue(const std::string& key, std::string& value) { return false; }
bool TableView::getValue(const std::string& key, std::string& value) const { return false; }
bool TableView::containsKey(const std::string& key) const { return false; }
std::unordered_map<std::string, std::string> TableView::snapshot() const {
    return std::unordered_map<std::string, std::string>();
}
std::size_t TableView::size() const { return 0; }
void TableView::forEach(TableViewAction action) {}
void TableView::forEachAndListen(TableViewAction action) {}
void TableView::closeAsync() {}
}  // namespace pulsar

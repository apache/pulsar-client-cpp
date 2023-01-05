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
#include <pulsar/TableViewConfiguration.h>

#include "TableViewConfigurationImpl.h"

namespace pulsar {

TableViewConfiguration::TableViewConfiguration() : impl_(std::make_shared<TableViewConfigurationImpl>()) {}

TableViewConfiguration::~TableViewConfiguration() {}

TableViewConfiguration::TableViewConfiguration(const TableViewConfiguration& x) : impl_(x.impl_) {}

TableViewConfiguration& TableViewConfiguration::operator=(const TableViewConfiguration& x) {
    impl_ = x.impl_;
    return *this;
}

TableViewConfiguration TableViewConfiguration::clone() const {
    TableViewConfiguration newConf;
    newConf.impl_.reset(new TableViewConfigurationImpl(*this->impl_));
    return newConf;
}

const SchemaInfo& TableViewConfiguration::getSchemaInfo() const { return impl_->schemaInfo_; }

TableViewConfiguration& TableViewConfiguration::setSchemaInfo(const SchemaInfo& schemaInfo) {
    impl_->schemaInfo_ = schemaInfo;
    return *this;
}

const std::string& TableViewConfiguration::getSubscriptionName() const { return impl_->subscriptionName_; }

TableViewConfiguration& TableViewConfiguration::setSubscriptionName(const std::string subscriptionName) {
    impl_->subscriptionName_ = subscriptionName;
    return *this;
}

}  // namespace pulsar

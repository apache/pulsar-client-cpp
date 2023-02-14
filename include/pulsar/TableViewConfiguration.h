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
#ifndef PULSAR_TABLEVIEW_CONFIGURATION_H_
#define PULSAR_TABLEVIEW_CONFIGURATION_H_

#include <pulsar/Schema.h>
#include <pulsar/defines.h>

namespace pulsar {

struct TableViewConfiguration {
    // Declare the schema of the data that this table view will be accepting.
    // The schema will be checked against the schema of the topic, and the
    // table view creation will fail if it's not compatible.
    SchemaInfo schemaInfo;
    // The name of the subscription to the topic. Default value is reader-{random string}.
    std::string subscriptionName;
};
}  // namespace pulsar
#endif /* PULSAR_TABLEVIEW_CONFIGURATION_H_ */

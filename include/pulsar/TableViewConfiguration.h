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

#include <memory>

namespace pulsar {

struct TableViewConfigurationImpl;

/**
 * Class specifying the configuration of a consumer.
 */
class PULSAR_PUBLIC TableViewConfiguration {
   public:
    TableViewConfiguration();
    ~TableViewConfiguration();
    TableViewConfiguration(const TableViewConfiguration&);
    TableViewConfiguration& operator=(const TableViewConfiguration&);

    /**
     * Create a new instance of TableViewConfiguration with the same
     * initial settings as the current one.
     */
    TableViewConfiguration clone() const;

    /**
     * @return the schema information declared for this consumer
     */
    const SchemaInfo& getSchemaInfo() const;

    /**
     * Declare the schema of the data that this table view will be accepting.
     *
     * The schema will be checked against the schema of the topic, and the
     * table view creation will fail if it's not compatible.
     *
     * @param schemaInfo the schema definition object
     */
    TableViewConfiguration& setSchemaInfo(const SchemaInfo& schemaInfo);

    /**
     * @return subscriptionName
     */
    const std::string& getSubscriptionName() const;

    /**
     * Set the subscription name of the {@link TableView}.
     *
     * @param subscriptionName the name of the subscription to the topic
     */
    TableViewConfiguration& setSubscriptionName(const std::string subscriptionName);

   private:
    std::shared_ptr<TableViewConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_TABLEVIEW_CONFIGURATION_H_ */

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
#ifndef TABEL_VIEW_HPP_
#define TABEL_VIEW_HPP_

#include <pulsar/Result.h>
#include <pulsar/TableViewConfiguration.h>
#include <pulsar/defines.h>

#include <functional>
#include <unordered_map>

namespace pulsar {

class TableViewImpl;

typedef std::function<void(Result result)> ResultCallback;
typedef std::function<void(const std::string& key, const std::string& value)> TableViewAction;
/**
 *
 */
class PULSAR_PUBLIC TableView {
   public:
    /**
     * Construct an uninitialized tableView object
     */
    TableView();

    /**
     * Move the latest value associated with the key.
     *
     * Example:
     *
     * ```c++
     * TableView view;
     * std::string value;
     * while (true) {
     *     if (view.retrieveValue("key", value)) {
     *         std::cout << "value is updated to: " << value;
     *     } else {
     *         // sleep for a while or print the message that value is not updated
     *     }
     * }
     * ```
     *
     * @param key
     * @param value the value associated with the key
     * @return true if there is an associated value of the key, otherwise false
     *
     * NOTE: Once the value has been retrieved successfully, the associated value
     * will be removed from the table view until next time the value is updated.
     */
    bool retrieveValue(const std::string& key, std::string& value);

    /**
     * It's similar with retrievedValue except the value is copied into `value`.
     *
     * @param key
     * @param value the value associated with the key
     * @return Whether the key exists in the table view.
     */
    bool getValue(const std::string& key, std::string& value) const;

    /**
     * Check if the key exists in the table view.
     *
     * @return true if the key exists in the table view
     */
    bool containsKey(const std::string& key) const;

    /**
     * Move the table view data into the unordered map.
     */
    std::unordered_map<std::string, std::string> snapshot();

    /**
     * Get the size of the elements.
     */
    std::size_t size() const;

    /**
     * Performs the given action for each entry in this map until all entries have been processed or the
     * action throws an exception.
     */
    void forEach(TableViewAction action);

    /**
     * Performs the given action for each entry in this map until all entries have been processed and
     * register the callback, which will be called each time a key-value pair is updated.
     */
    void forEachAndListen(TableViewAction action);

    /**
     * Asynchronously close the tableview and stop the broker to push more messages
     */
    void closeAsync(ResultCallback callback);

    /**
     * Close the table view and stop the broker to push more messages
     */
    Result close();

   private:
    typedef std::shared_ptr<TableViewImpl> TableViewImplPtr;
    TableViewImplPtr impl_;
    explicit TableView(TableViewImplPtr);

    friend class PulsarFriend;
    friend class ClientImpl;
};
}  // namespace pulsar

#endif /* TABEL_VIEW_HPP_ */

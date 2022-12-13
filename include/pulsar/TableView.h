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

#include <iostream>
#include <map>

#include "defines.h"

namespace pulsar {

class TableViewImpl;

typedef std::map<std::string, std::string> TableViewMap;
typedef std::function<void(const std::string& key, const std::string& value )> TableViewAction;
typedef std::shared_ptr<TableViewImpl> TableViewImplPtr;
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
     *
     * @return
     */
    int size() const;

    /**
     *
     * @return
     */
    bool empty() const;

    /**
     *
     * @param key
     * @return
     */
    bool containsKey(const std::string& key) const;

    /**
     *
     * @param key
     * @return
     */
    const std::string& get(const std::string& key) const;

    /**
     *
     * @param action
     */
    void forEach(TableViewAction action) const;

    /**
     *
     * @param action
     */
    void forEachAndListener(TableViewAction listener) const;

//    /**
//     * todo 不能把原始map返回给用户，如果用户用引用接收，及时只读取，但是会出现线程安全问题。
//     * @return
//     */
//    const std::map<std::string, std::string>& getMap() const;

    /**
     *
     * @return
     */
    void closeAsync();

   private:
    TableViewImplPtr impl_;

};
}  // namespace pulsar

#endif /* TABEL_VIEW_HPP_ */

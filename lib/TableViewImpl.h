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
#include <unordered_set>

#include "SynchronizedHashMap.h"


namespace pulsar {

typedef std::shared_ptr<std::map<std::string, std::string>> mapPtr;

class TableViewImpl {

   public:
    typedef std::pair<std::string, std::string> Point;
    SynchronizedHashMap<std::string, std::string> data;
    std::map<std::string, std::string> test;

   private:


    mapPtr const ttt() const {
        return std::make_shared<std::map<std::string, std::string>>(test);
    }

    void aa(){
        auto immutableMap = ttt();
        immutableMap = std::make_shared<std::map<std::string, std::string>>(test);
        immutableMap.reset();
        immutableMap->find("test");

        // keys and values todo 还不可变
        std::vector<Point> tempSet2;

        std::transform(test.cbegin(), test.cend(),
                       std::inserter(tempSet2, tempSet2.begin()),
                       [](const std::pair<std::string, std::string>& key_value)
                       { return key_value; });

        // keys and values todo 还不可变
        // todo 不可变，
        // todo 没有数据拷贝, 数据变化可以看得到不用重复获取
        // todo  线程安全, 内存的引用,
        std::unordered_set<std::string> tempSet;
        std::transform(test.cbegin(), test.cend(),
                       std::inserter(tempSet, tempSet.begin()),
                       [](const std::pair<std::string, std::string>& key_value)
                       { return key_value.first; });

        test.find("key");
    }


};
} // namespace pulsar

#endif  // PULSAR_CPP_TABLEVIEW_IMPL_H
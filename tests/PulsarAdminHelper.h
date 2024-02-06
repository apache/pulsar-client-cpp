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
#pragma once

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "HttpHelper.h"

namespace pulsar {

inline std::string getTopicStats(const std::string& topic, boost::property_tree::ptree& root) {
    const auto url = "http://localhost:8080/admin/v2/persistent/public/default/" + topic + "/stats";
    std::string responseData;
    int code = makeGetRequest(url, responseData);
    if (code != 200) {
        return url + " failed: " + std::to_string(code);
    }

    std::stringstream stream;
    stream << responseData;
    boost::property_tree::read_json(stream, root);
    return "";
}

}  // namespace pulsar

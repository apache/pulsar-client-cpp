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
#include <sstream>
#include <string>

namespace pulsar {

template <typename Ptree>
inline std::string toJson(const Ptree& pt) {
    std::ostringstream oss;
    boost::property_tree::write_json(oss, pt, false);
    // For Boost < 1.86, boost::property_tree will write a endline at the end
#if BOOST_VERSION < 108600
    auto s = oss.str();
    s.pop_back();
    return s;
#else
    return oss.str();
#endif
}

}  // namespace pulsar

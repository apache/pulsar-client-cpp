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

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <string>

namespace pulsar {

namespace base64 {

inline std::string encode(const char* data, size_t size) {
    using namespace boost::archive::iterators;
    using Base64Iter = base64_from_binary<transform_width<const char*, 6, 8>>;

    std::string encoded{Base64Iter(data), Base64Iter(data + size)};
    const auto numPaddings = (4 - encoded.size()) % 4;
    encoded.append(numPaddings, '=');
    return encoded;
}

template <typename CharContainer>
inline std::string encode(const CharContainer& container) {
    return encode(container.data(), container.size());
}

inline std::string decode(const std::string& encoded) {
    using namespace boost::archive::iterators;
    using Base64Iter = transform_width<binary_from_base64<std::string::const_iterator>, 8, 6>;

    std::string result{Base64Iter(encoded.cbegin()), Base64Iter(encoded.cend())};
    // There could be '\0's at the tail, it could cause "garbage after data" error when parsing JSON
    while (!result.empty() && result.back() == '\0') {
        result.pop_back();
    }
    return result;
}

}  // namespace base64

}  // namespace pulsar

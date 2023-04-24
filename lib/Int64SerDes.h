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

#include <stdint.h>

#include <boost/asio.hpp>  // for ntohl

namespace pulsar {

inline int64_t fromBigEndianBytes(const std::string& bytes) {
    const auto int32Array = reinterpret_cast<const uint32_t*>(bytes.c_str());
    return (static_cast<int64_t>(ntohl(int32Array[0])) << 32) + static_cast<int64_t>(ntohl(int32Array[1]));
}

inline std::string toBigEndianBytes(int64_t value) {
    union {
        char bytes[8];
        uint32_t int32Array[2];
    } u;
    u.int32Array[0] = htonl(static_cast<int32_t>(value >> 32));
    u.int32Array[1] = htonl(static_cast<int32_t>(value & 0xFFFFFFFF));
    return {u.bytes, 8};
}

}  // namespace pulsar

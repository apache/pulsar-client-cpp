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
#ifndef UTILS_HPP_
#define UTILS_HPP_

#include <pulsar/Result.h>

#include <iostream>
#include <map>

#include "Future.h"

namespace pulsar {

struct WaitForCallback {
    Promise<bool, Result> m_promise;

    WaitForCallback(Promise<bool, Result> promise) : m_promise(promise) {}

    void operator()(Result result) { m_promise.setValue(result); }
};

template <typename T>
struct WaitForCallbackValue {
    Promise<Result, T>& m_promise;

    WaitForCallbackValue(Promise<Result, T>& promise) : m_promise(promise) {}

    void operator()(Result result, const T& value) {
        if (result == ResultOk) {
            m_promise.setValue(value);
        } else {
            m_promise.setFailed(result);
        }
    }
};

template <typename T>
struct WaitForCallbackType {
    Promise<Result, T> m_promise;

    WaitForCallbackType(Promise<Result, T> promise) : m_promise(promise) {}

    void operator()(T result) { m_promise.setValue(result); }
};

inline std::ostream& operator<<(std::ostream& os, const std::map<Result, unsigned long>& m) {
    os << "{";
    for (std::map<Result, unsigned long>::const_iterator it = m.begin(); it != m.end(); it++) {
        os << "[Key: " << strResult(it->first) << ", Value: " << it->second << "], ";
    }
    os << "}";
    return os;
}

}  // namespace pulsar

#endif /* UTILS_HPP_ */

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

#include <pulsar/defines.h>

#include <atomic>
#include <chrono>

namespace pulsar {

using ptime = decltype(std::chrono::high_resolution_clock::now());
using TimeDuration = std::chrono::nanoseconds;

inline decltype(std::chrono::milliseconds(0).count()) toMillis(TimeDuration duration) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

class PULSAR_PUBLIC TimeUtils {
   public:
    static ptime now() { return std::chrono::high_resolution_clock::now(); }

    static int64_t currentTimeMillis() {
        return toMillis(std::chrono::system_clock::now().time_since_epoch());
    }
};

// This class processes a timeout with the following semantics:
//  > 0: wait at most the timeout until a blocking operation completes
//  == 0: do not wait the blocking operation
//  < 0: wait infinitely until a blocking operation completes.
//
// Here is a simple example usage:
//
// ```c++
// // Wait at most 300 milliseconds
// TimeoutProcessor<std::chrono::milliseconds> timeoutProcessor{300};
// while (!allOperationsAreDone()) {
//     timeoutProcessor.tik();
//     // This method may block for some time
//     performBlockingOperation(timeoutProcessor.getLeftTimeout());
//     timeoutProcessor.tok();
// }
// ```
//
// The template argument is the same as std::chrono::duration.
template <typename Duration>
class TimeoutProcessor {
   public:
    using Clock = std::chrono::high_resolution_clock;

    TimeoutProcessor(long timeout) : leftTimeout_(timeout) {}

    long getLeftTimeout() const noexcept { return leftTimeout_; }

    void tik() { before_ = Clock::now(); }

    void tok() {
        if (leftTimeout_ > 0) {
            leftTimeout_ -= std::chrono::duration_cast<Duration>(Clock::now() - before_).count();
            if (leftTimeout_ <= 0) {
                // The timeout exceeds, getLeftTimeout() will return 0 to indicate we should not wait more
                leftTimeout_ = 0;
            }
        }
    }

   private:
    std::atomic_long leftTimeout_;
    std::chrono::time_point<Clock> before_;
};

}  // namespace pulsar

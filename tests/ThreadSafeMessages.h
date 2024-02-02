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

#include <pulsar/Message.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <vector>

namespace pulsar {

// When we receive messages in the message listener or the callback of receiveAsync(), we need to verify the
// received messages in the test thread. This class is a helper class for thread-safe access to the messages.
class ThreadSafeMessages {
   public:
    ThreadSafeMessages(size_t minNumMsgs) : minNumMsgs_(minNumMsgs) {}

    template <typename Duration>
    bool wait(Duration duration) {
        std::unique_lock<std::mutex> lock{mutex_};
        return cond_.wait_for(lock, duration, [this] { return msgs_.size() >= minNumMsgs_; });
    }

    void add(const Message& msg) {
        std::lock_guard<std::mutex> lock{mutex_};
        msgs_.emplace_back(msg);
        if (msgs_.size() >= minNumMsgs_) {
            cond_.notify_all();
        }
    }

    void clear() {
        std::lock_guard<std::mutex> lock{mutex_};
        msgs_.clear();
    }

    std::vector<std::string> getSortedValues() const {
        std::unique_lock<std::mutex> lock{mutex_};
        std::vector<std::string> values(msgs_.size());
        std::transform(msgs_.cbegin(), msgs_.cend(), values.begin(),
                       [](const Message& msg) { return msg.getDataAsString(); });
        lock.unlock();
        std::sort(values.begin(), values.end());
        return values;
    }

    void setMinNumMsgs(size_t minNumMsgs) noexcept { minNumMsgs_ = minNumMsgs; }

   private:
    std::atomic_size_t minNumMsgs_;
    std::vector<Message> msgs_;
    mutable std::mutex mutex_;
    mutable std::condition_variable cond_;
};

}  // namespace pulsar

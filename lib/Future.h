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
#ifndef LIB_FUTURE_H_
#define LIB_FUTURE_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace pulsar {

template <typename Result, typename Type>
class InternalState {
   public:
    using Listener = std::function<void(Result, const Type &)>;
    using Pair = std::pair<Result, Type>;
    using Lock = std::unique_lock<std::mutex>;

    // NOTE: Add the constructor explicitly just to be compatible with GCC 4.8
    InternalState() {}

    void addListener(Listener listener) {
        Lock lock{mutex_};
        listeners_.emplace_back(listener);
        lock.unlock();

        if (completed()) {
            Type value;
            Result result = get(value);
            triggerListeners(result, value);
        }
    }

    bool complete(Result result, const Type &value) {
        bool expected = false;
        if (!completed_.compare_exchange_strong(expected, true)) {
            return false;
        }
        triggerListeners(result, value);
        promise_.set_value(std::make_pair(result, value));
        return true;
    }

    bool completed() const noexcept { return completed_; }

    Result get(Type &result) {
        const auto &pair = future_.get();
        result = pair.second;
        return pair.first;
    }

    // Only public for test
    void triggerListeners(Result result, const Type &value) {
        while (true) {
            Lock lock{mutex_};
            if (listeners_.empty()) {
                return;
            }

            bool expected = false;
            if (!listenerRunning_.compare_exchange_strong(expected, true)) {
                // There is another thread that polled a listener that is running, skip polling and release
                // the lock. Here we wait for some time to avoid busy waiting.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            auto listener = std::move(listeners_.front());
            listeners_.pop_front();
            lock.unlock();

            listener(result, value);
            listenerRunning_ = false;
        }
    }

   private:
    std::atomic_bool completed_{false};
    std::promise<Pair> promise_;
    std::shared_future<Pair> future_{promise_.get_future()};

    std::list<Listener> listeners_;
    mutable std::mutex mutex_;
    std::atomic_bool listenerRunning_{false};
};

template <typename Result, typename Type>
using InternalStatePtr = std::shared_ptr<InternalState<Result, Type>>;

template <typename Result, typename Type>
class Future {
   public:
    using Listener = typename InternalState<Result, Type>::Listener;

    Future &addListener(Listener listener) {
        state_->addListener(listener);
        return *this;
    }

    Result get(Type &result) { return state_->get(result); }

   private:
    InternalStatePtr<Result, Type> state_;

    Future(InternalStatePtr<Result, Type> state) : state_(state) {}

    template <typename U, typename V>
    friend class Promise;
};

template <typename Result, typename Type>
class Promise {
   public:
    Promise() : state_(std::make_shared<InternalState<Result, Type>>()) {}

    bool setValue(const Type &value) const { return state_->complete({}, value); }

    bool setFailed(Result result) const { return state_->complete(result, {}); }

    bool isComplete() const { return state_->completed(); }

    Future<Result, Type> getFuture() const { return Future<Result, Type>{state_}; }

   private:
    const InternalStatePtr<Result, Type> state_;
};

}  // namespace pulsar

#endif

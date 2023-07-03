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
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

namespace pulsar {

template <typename Result, typename Type>
class InternalState {
   public:
    using Listener = std::function<void(Result, const Type &)>;
    using Pair = std::pair<Result, Type>;

    void addListener(Listener listener) {
        if (completed()) {
            // Allow get_future() being called multiple times, only the 1st time will wait() be called to wait
            // until all previous listeners are done.
            try {
                listenersPromise_.get_future().wait();
            } catch (const std::future_error &e) {
                if (e.code() != std::future_errc::future_already_retrieved) {
                    throw e;
                }
            }
            listener(future_.get().first, future_.get().second);
        } else {
            std::lock_guard<std::mutex> lock{mutex_};
            listeners_.emplace_back(listener);
        }
    }

    bool complete(Result result, const Type &value) {
        bool expected = false;
        if (!completed_.compare_exchange_strong(expected, true)) {
            return false;
        }

        std::unique_lock<std::mutex> lock{mutex_};
        decltype(listeners_) listeners;
        listeners.swap(listeners_);
        lock.unlock();

        for (auto &&listener : listeners) {
            listener(result, value);
        }
        // Notify the previous listeners are all done so that any listener added after completing will be
        // called after the previous listeners.
        listenersPromise_.set_value(true);

        promise_.set_value(std::make_pair(result, value));
        return true;
    }

    bool completed() const noexcept { return completed_; }

    Result get(Type &result) {
        auto pair = future_.get();
        result = std::move(pair.second);
        return pair.first;
    }

   private:
    std::atomic_bool completed_{false};
    std::promise<Pair> promise_;
    std::shared_future<Pair> future_{promise_.get_future()};

    std::promise<bool> listenersPromise_;
    std::list<Listener> listeners_;
    mutable std::mutex mutex_;
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

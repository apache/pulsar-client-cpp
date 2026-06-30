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
#include <pulsar/st/Expected.h>
#include <pulsar/st/detail/SharedState.h>

#include <chrono>
#include <coroutine>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace pulsar::st {

namespace detail {
template <typename T>
class Promise;
}

/**
 * The result of an asynchronous operation, available now or later.
 *
 * Unlike `std::future`, this is **continuation-capable**: `addListener()` runs a
 * callback when the operation completes, so you can react without blocking. The
 * callback receives an `Expected<T>` (value or error). `get()` is available when
 * you do want to block. On a C++20 toolchain the future is also `co_await`-able.
 *
 * A Future is cheap to copy (it shares the underlying state). Listeners run on
 * whichever thread completes the operation — do not block inside one.
 *
 * @tparam T the type of the value the operation produces (`void` for value-less
 *         operations, which complete with an `Expected<void>`).
 */
template <typename T>
class Future {
   public:
    /** Callback type accepted by `addListener`, invoked with `const Expected<T>&`. */
    using Listener = typename detail::SharedState<T>::Listener;

    /**
     * Register a continuation to run when the operation completes.
     *
     * @p listener is invoked with the `Expected<T>` result (value or error) on the
     * thread that completes the operation, or synchronously on the calling thread if
     * the operation has already completed. It does not block. Do not block inside
     * the listener. Multiple listeners may be registered.
     *
     * @param listener the callback to invoke on completion.
     * @return `*this`, to allow chaining.
     */
    Future& addListener(Listener listener) {
        state_->addListener(std::move(listener));
        return *this;
    }

    /**
     * Block the calling thread until the operation completes and return its result.
     *
     * @return the `Expected<T>` result, holding either the value or the error.
     */
    Expected<T> get() const { return state_->get(); }

    /**
     * Block until the operation completes or @p timeout elapses, whichever first.
     *
     * @tparam Rep the `std::chrono::duration` representation type.
     * @tparam Period the `std::chrono::duration` period type.
     * @param timeout the maximum time to wait for completion.
     * @return the `Expected<T>` result if it completed in time, or `std::nullopt`
     *         if @p timeout elapsed first.
     */
    template <typename Rep, typename Period>
    std::optional<Expected<T>> get(std::chrono::duration<Rep, Period> timeout) const {
        return state_->get(timeout);
    }

    /**
     * Whether the operation has already completed.
     *
     * @return `true` if the result is available (so `get()` would not block),
     *         `false` otherwise.
     */
    bool isReady() const { return state_->isReady(); }

    /**
     * Return a new Future whose value is `f` applied to this one's value on
     * completion; an error propagates unchanged. Lets a typed facade map a Future
     * of a (non-templated) core into a Future of its typed wrapper.
     *
     * @p f is applied on whichever thread completes this Future. If this Future
     * completes with an error, @p f is not called and the error is forwarded to the
     * returned Future. Only participates in overload resolution when `T` is not
     * `void`.
     *
     * @tparam F a callable taking `const T&` and returning the mapped value.
     * @tparam U defaults to `T`; an implementation detail of the `void` constraint.
     * @param f the mapping function to apply to the value.
     * @return a `Future` of `f`'s return type, completed with the mapped value on
     *         success or the propagated error on failure.
     */
    template <typename F, typename U = T, std::enable_if_t<!std::is_void_v<U>, int> = 0>
    Future<std::invoke_result_t<F, const U&>> thenApply(F f) const {
        using R = std::invoke_result_t<F, const U&>;
        detail::Promise<R> promise;
        state_->addListener([promise, f = std::move(f)](const Expected<T>& result) {
            if (result) {
                promise.setValue(f(*result));
            } else {
                promise.setError(result.error());
            }
        });
        return promise.getFuture();
    }

    /**
     * Coroutine support: whether the awaiting coroutine may skip suspension.
     *
     * Part of the C++20 awaitable interface so a `Future` can be used as
     * `Expected<T> r = co_await someFuture;`. Not called directly.
     *
     * @return `true` if the result is already available, `false` otherwise.
     */
    bool await_ready() const { return state_->isReady(); }

    /**
     * Coroutine support: suspend the awaiting coroutine until completion.
     *
     * Atomically registers a continuation that resumes @p handle on completion, or —
     * if the result is already available — returns `false` so the coroutine resumes
     * immediately rather than being resumed from inside `await_suspend` (which could
     * run and destroy this awaiter before it returns). Part of the C++20 awaitable
     * interface; not called directly.
     *
     * @param handle the suspended coroutine to resume on completion.
     * @return `true` to stay suspended (resumed later on the completing thread),
     *         `false` to resume immediately because the result is already available.
     */
    bool await_suspend(std::coroutine_handle<> handle) {
        return state_->addListenerOrReady([handle](const Expected<T>&) { handle.resume(); });
    }

    /**
     * Coroutine support: produce the value of a `co_await` expression.
     *
     * Part of the C++20 awaitable interface; not called directly.
     *
     * @return the `Expected<T>` result of the awaited operation.
     */
    Expected<T> await_resume() const { return state_->get(); }

   private:
    template <typename U>
    friend class detail::Promise;
    explicit Future(std::shared_ptr<detail::SharedState<T>> state) : state_(std::move(state)) {}

    std::shared_ptr<detail::SharedState<T>> state_;
};

namespace detail {

/**
 * INTERNAL producing side of a `Future<T>`. The SDK fulfils these to complete the
 * futures it returns; applications only ever consume `Future<T>` and never build
 * a Promise, so it lives in `detail` rather than on the public surface.
 */
template <typename T>
class Promise {
   public:
    Promise() : state_(std::make_shared<SharedState<T>>()) {}

    Future<T> getFuture() const { return Future<T>(state_); }

    bool complete(Expected<T> result) const { return state_->complete(std::move(result)); }
    bool setError(Error error) const { return state_->complete(Expected<T>(std::move(error))); }

    template <typename U = T, std::enable_if_t<!std::is_void_v<U>, int> = 0>
    bool setValue(U value) const {
        return state_->complete(Expected<T>(std::move(value)));
    }

    template <typename U = T, std::enable_if_t<std::is_void_v<U>, int> = 0>
    bool setSuccess() const {
        return state_->complete(Expected<void>());
    }

   private:
    std::shared_ptr<SharedState<T>> state_;
};

}  // namespace detail
}  // namespace pulsar::st

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
#include <pulsar/st/Error.h>

#include <cstdlib>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

// A C++17 stand-in for std::expected<T, Error> (C++23), used as the single
// synchronous result type of the scalable-topics API. The public surface mirrors
// std::expected so call sites read identically on every toolchain; on compilers
// that ship <expected> we could later alias to std::expected without changing
// user code. The one intentional deviation: `value()` on an error throws our
// `ClientException` (not std::bad_expected_access), keeping one error channel.

#if defined(__cpp_exceptions) || defined(_CPPUNWIND)
#define PULSAR_ST_THROW(ex) throw(ex)
#else
#define PULSAR_ST_THROW(ex) (static_cast<void>(ex), std::abort())
#endif

namespace pulsar::st {

/** Wrap an `Error` to construct an `Expected<T>` in its error state. */
struct Unexpected {
    /** The wrapped error to seed the `Expected`'s error state. */
    Error error;
};

/**
 * Build an `Unexpected` from an existing `Error`.
 *
 * Convenience factory for returning a failure from a function whose return type
 * is an `Expected<T>`, mirroring `std::unexpected`.
 *
 * @param error the error to wrap.
 * @return an `Unexpected` carrying @p error, implicitly convertible to any `Expected<T>`.
 */
inline Unexpected unexpected(Error error) { return Unexpected{std::move(error)}; }

/**
 * Build an `Unexpected` from a result code and an optional detail message.
 *
 * Convenience overload that constructs the `Error` pair in place, so call sites
 * can write `return unexpected(ResultTimeout, "...")` without naming `Error`.
 *
 * @param result the machine-readable result code describing the failure.
 * @param message an optional human-readable detail message (empty by default).
 * @return an `Unexpected` carrying `Error{result, message}`.
 */
inline Unexpected unexpected(Result result, std::string message = {}) {
    return Unexpected{Error{result, std::move(message)}};
}

/**
 * Holds either a value of type `T` or an `Error`. Returned by every synchronous
 * operation. `[[nodiscard]]` so a failure can't be silently dropped — the one
 * weakness of value-based errors, closed at compile time.
 */
template <typename T>
class [[nodiscard]] Expected {
   public:
    /** The contained value type. */
    using value_type = T;
    /** The error type held in the failure state. */
    using error_type = Error;

    /** Construct in the value state by copying @p value. */
    Expected(const T& value) : storage_(std::in_place_index<0>, value) {}
    /** Construct in the value state by moving @p value. */
    Expected(T&& value) : storage_(std::in_place_index<0>, std::move(value)) {}
    /** Construct in the error state by copying @p error. */
    Expected(const Error& error) : storage_(std::in_place_index<1>, error) {}
    /** Construct in the error state by moving @p error. */
    Expected(Error&& error) : storage_(std::in_place_index<1>, std::move(error)) {}
    /** Construct in the error state from an `Unexpected` wrapper. */
    Expected(Unexpected u) : storage_(std::in_place_index<1>, std::move(u.error)) {}

    /**
     * Whether this holds a value (as opposed to an error).
     *
     * @return `true` if a value is present, `false` if it holds an error.
     */
    bool has_value() const noexcept { return storage_.index() == 0; }

    /**
     * Whether this holds a value, for use in a boolean context.
     *
     * Equivalent to `has_value()`. Enables the non-throwing pattern
     * `if (result) { use(*result); } else { handle(result.error()); }`.
     *
     * @return `true` if a value is present, `false` if it holds an error.
     */
    explicit operator bool() const noexcept { return has_value(); }

    /**
     * Returns the value, or throws `ClientException` if this holds an error.
     *
     * This mirrors `std::expected<T, E>::value()`, which throws
     * `std::bad_expected_access<E>` on an error. Modern C++ has no checked
     * exception specifications (dynamic specs were removed in C++17), so the thrown
     * type is documented here, not declared in the signature. Under `-fno-exceptions`
     * this aborts. Use `operator bool` + `operator*` for the non-throwing path.
     */
    const T& value() const& {
        if (!has_value()) PULSAR_ST_THROW(ClientException(std::get<1>(storage_)));
        return std::get<0>(storage_);
    }
    T& value() & {
        if (!has_value()) PULSAR_ST_THROW(ClientException(std::get<1>(storage_)));
        return std::get<0>(storage_);
    }
    T&& value() && {
        if (!has_value()) PULSAR_ST_THROW(ClientException(std::get<1>(storage_)));
        return std::get<0>(std::move(storage_));
    }

    /**
     * Unchecked access to the contained value.
     *
     * Performs no check and is `noexcept`. It does not throw on an error; instead,
     * because it reads the wrong `std::variant` alternative through a `noexcept`
     * boundary, accessing the value when this holds an error terminates the program.
     * Verify with `operator bool` first.
     *
     * @return a reference to the contained value (lvalue or rvalue per ref-qualifier).
     */
    const T& operator*() const& noexcept { return std::get<0>(storage_); }
    /** @copydoc operator*() const& */
    T& operator*() & noexcept { return std::get<0>(storage_); }
    /** @copydoc operator*() const& */
    T&& operator*() && noexcept { return std::get<0>(std::move(storage_)); }

    /**
     * Unchecked member access to the contained value.
     *
     * Returns `nullptr` if this holds an error (so `e->member` would then dereference
     * a null pointer); verify with `operator bool` first.
     *
     * @return a pointer to the contained value, or `nullptr` if this holds an error.
     */
    const T* operator->() const noexcept { return std::get_if<0>(&storage_); }
    /** @copydoc operator->() const */
    T* operator->() noexcept { return std::get_if<0>(&storage_); }

    /**
     * Access the contained error.
     *
     * @pre `!has_value()`. Behaviour is undefined if this holds a value.
     * @return a reference to the contained `Error`.
     */
    const Error& error() const& noexcept { return std::get<1>(storage_); }
    /** @copydoc error() const& */
    Error& error() & noexcept { return std::get<1>(storage_); }

    /**
     * Return the contained value, or @p fallback if this holds an error.
     *
     * @tparam U a type convertible to `T`.
     * @param fallback the value to return when no value is present.
     * @return a copy of the contained value, or `static_cast<T>(fallback)` on error.
     */
    template <typename U>
    T value_or(U&& fallback) const& {
        return has_value() ? std::get<0>(storage_) : static_cast<T>(std::forward<U>(fallback));
    }

    /** Rvalue overload of `value_or()`: moves the contained value out on success. */
    template <typename U>
    T value_or(U&& fallback) && {
        return has_value() ? std::get<0>(std::move(storage_)) : static_cast<T>(std::forward<U>(fallback));
    }

    /**
     * Monadic chaining: invoke @p f on the value, or propagate the error.
     *
     * If this holds a value, returns `f(value)` — which must itself be an
     * `Expected`. If this holds an error, the error is forwarded unchanged into a
     * fresh `Expected` of @p f's return type, and @p f is not called. Mirrors
     * `std::expected::and_then`.
     *
     * @tparam F a callable taking `const T&` and returning some `Expected<U>`.
     * @param f the continuation to invoke on the value.
     * @return `f(value)` on success, or that result type's error state on failure.
     */
    template <typename F>
    auto and_then(F&& f) const& {
        using R = std::remove_cv_t<std::remove_reference_t<std::invoke_result_t<F, const T&>>>;
        return has_value() ? std::forward<F>(f)(std::get<0>(storage_)) : R(error());
    }

    /** Rvalue overload of `and_then()`: invokes @p f with the moved-out value. */
    template <typename F>
    auto and_then(F&& f) && {
        using R = std::remove_cv_t<std::remove_reference_t<std::invoke_result_t<F, T&&>>>;
        return has_value() ? std::forward<F>(f)(std::get<0>(std::move(storage_))) : R(std::move(error()));
    }

    /**
     * Monadic mapping: transform the value through @p f, or propagate the error.
     *
     * If this holds a value, returns `Expected<U>(f(value))` where `U` is @p f's
     * return type. If this holds an error, the error is forwarded unchanged and
     * @p f is not called. Mirrors `std::expected::transform`.
     *
     * @tparam F a callable taking `const T&` and returning a plain value `U`.
     * @param f the mapping function to apply to the value.
     * @return `Expected<U>` holding `f(value)` on success, or the error on failure.
     */
    template <typename F>
    auto transform(F&& f) const& {
        using U = std::remove_cv_t<std::remove_reference_t<std::invoke_result_t<F, const T&>>>;
        return has_value() ? Expected<U>(std::forward<F>(f)(std::get<0>(storage_))) : Expected<U>(error());
    }

    /** Rvalue overload of `transform()`: maps the moved-out value through @p f. */
    template <typename F>
    auto transform(F&& f) && {
        using U = std::remove_cv_t<std::remove_reference_t<std::invoke_result_t<F, T&&>>>;
        return has_value() ? Expected<U>(std::forward<F>(f)(std::get<0>(std::move(storage_))))
                           : Expected<U>(std::move(error()));
    }

    /**
     * Monadic error recovery: invoke @p f on the error, or pass the value through.
     *
     * If this holds an error, returns `f(error)` — used to recover or substitute an
     * alternative result. If this holds a value, `*this` is returned unchanged and
     * @p f is not called. Mirrors `std::expected::or_else`.
     *
     * @tparam F a callable taking `const Error&` and returning an `Expected`.
     * @param f the recovery function to invoke on the error.
     * @return `*this` on success, or `f(error)` on failure.
     */
    template <typename F>
    Expected or_else(F&& f) const& {
        return has_value() ? *this : std::forward<F>(f)(error());
    }

    /** Rvalue overload of `or_else()`: passes the moved value through, or invokes @p f
     *  with the moved-out error. */
    template <typename F>
    Expected or_else(F&& f) && {
        return has_value() ? std::move(*this) : std::forward<F>(f)(std::move(error()));
    }

   private:
    std::variant<T, Error> storage_;
};

/**
 * Specialization for value-less results (`close`, `flush`, `commit`, …).
 *
 * Carries no value: it is either in the success state or holds an `Error`. Used as
 * the synchronous result type of operations that either succeed or fail without
 * producing data.
 */
template <>
class [[nodiscard]] Expected<void> {
   public:
    /** The (absent) value type. */
    using value_type = void;
    /** The error type held in the failure state. */
    using error_type = Error;

    /** Construct in the success state. */
    Expected() noexcept = default;  // success
    /** Construct in the error state by copying @p error. */
    Expected(const Error& error) : error_(error) {}
    /** Construct in the error state by moving @p error. */
    Expected(Error&& error) : error_(std::move(error)) {}
    /** Construct in the error state from an `Unexpected` wrapper. */
    Expected(Unexpected u) : error_(std::move(u.error)) {}

    /**
     * Whether this represents success (as opposed to an error).
     *
     * @return `true` on success, `false` if it holds an error.
     */
    bool has_value() const noexcept { return !error_.has_value(); }

    /**
     * Whether this represents success, for use in a boolean context.
     *
     * @return `true` on success, `false` if it holds an error.
     */
    explicit operator bool() const noexcept { return has_value(); }

    /**
     * Throw `ClientException` if this holds an error; otherwise return normally.
     *
     * The value-less analogue of `Expected<T>::value()`: it has no value to yield,
     * so on success it simply returns. As with the primary template, the thrown
     * type is documented rather than declared in the signature; under
     * `-fno-exceptions` this aborts.
     */
    void value() const {
        if (!has_value()) PULSAR_ST_THROW(ClientException(*error_));
    }

    /**
     * Access the contained error.
     *
     * @pre `!has_value()`. Behaviour is undefined on a success value.
     * @return a reference to the contained `Error`.
     */
    const Error& error() const& noexcept { return *error_; }

   private:
    std::optional<Error> error_;
};

}  // namespace pulsar::st

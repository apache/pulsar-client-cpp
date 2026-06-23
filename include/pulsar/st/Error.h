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

#include <pulsar/Result.h>
#include <pulsar/defines.h>
#include <pulsar/st/detail/Cxx20.h>

#include <exception>
#include <string>
#include <utility>

namespace pulsar::st {

// The scalable-topics SDK reuses the existing `pulsar::Result` code taxonomy and
// the {result, message} `Error` pair rather than introducing a parallel one: the
// underlying core already emits `pulsar::Result`, so reusing it avoids a lossy
// translation layer and keeps `strResult()` / error strings consistent. C++20
// `using enum` re-exports the result codes into `pulsar::st`, so they are usable
// unqualified here (e.g. `ResultTimeout`) — no scoped parallel enum needed.
/** Re-export of `pulsar::Error`: the `{result, message}` pair describing a failure. */
using pulsar::Error;
/** Re-export of `pulsar::Result`: the enumeration of machine-readable result codes. */
using pulsar::Result;
/** Re-export the `Result` enumerators into `pulsar::st` so they are usable unqualified (e.g. `ResultTimeout`). */
using enum pulsar::Result;

/**
 * The exception type of the scalable-topics API, wrapping a {result, message}
 * pair. The API is **non-throwing by default**: synchronous calls return
 * `Expected<T>` and asynchronous calls deliver `Expected<T>` to a `Future<T>`
 * listener. `ClientException` is thrown only when you opt in — by calling
 * `Expected<T>::value()` on a result that holds an error. Code that never calls
 * `value()` never throws (and the C API under `pulsar/c/st/` is fully non-throwing
 * and ABI-stable).
 */
class PULSAR_PUBLIC ClientException : public std::exception {
   public:
    /**
     * Construct from a result code and detail message.
     *
     * The `what()` string is composed from the code's `strResult()` name and, when
     * non-empty, @p message.
     *
     * @param result the machine-readable result code describing the failure.
     * @param message the human-readable detail message (may be empty).
     */
    ClientException(Result result, std::string message)
        : error_{result, std::move(message)},
          what_(error_.message.empty() ? std::string{strResult(result)}
                                       : std::string{strResult(result)} + ": " + error_.message) {}

    /**
     * Construct from an existing `{result, message}` `Error` pair.
     *
     * This is the constructor `Expected<T>::value()` uses to turn a stored error
     * into a thrown exception.
     *
     * @param error the error pair to wrap.
     */
    explicit ClientException(Error error) : ClientException(error.result, std::move(error.message)) {}

    /** The machine-readable result code. */
    Result result() const noexcept { return error_.result; }

    /** The human-readable detail message (may be empty). */
    const std::string& message() const noexcept { return error_.message; }

    /** The full {result, message} pair. */
    const Error& error() const noexcept { return error_; }

    /**
     * The formatted exception description, as required by `std::exception`.
     *
     * Combines the result code's `strResult()` name with the detail message when one
     * is present.
     *
     * @return a null-terminated string owned by this exception.
     */
    const char* what() const noexcept override { return what_.c_str(); }

   private:
    Error error_;
    std::string what_;
};

}  // namespace pulsar::st

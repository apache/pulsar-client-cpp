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

#include <pulsar/MessageBuilder.h>

#include <functional>

namespace pulsar {

template <typename T>
class TypedMessageBuilder : public MessageBuilder {
   public:
    using Encoder = std::function<std::string(const T&)>;
    using Validator = std::function<void(const char* data, size_t)>;

    TypedMessageBuilder(
        Encoder encoder, Validator validator = [](const char*, std::size_t) {})
        : encoder_(encoder), validator_(validator) {}

    TypedMessageBuilder& setValue(const T& value) {
        setContent(encoder_(value));
        if (validator_) {
            validator_(data(), size());
        }
        return *this;
    }

   private:
    const Encoder encoder_;
    const Validator validator_;
};

template <>
class TypedMessageBuilder<std::string> : public MessageBuilder {
   public:
    // The validator should throw an exception to indicate the message is corrupted.
    using Validator = std::function<void(const char* data, size_t)>;

    TypedMessageBuilder(Validator validator = nullptr) : validator_(validator) {}

    TypedMessageBuilder& setValue(const std::string& value) {
        if (validator_) {
            validator_(value.data(), value.size());
        }
        setContent(value);
        return *this;
    }

    TypedMessageBuilder& setValue(std::string&& value) {
        if (validator_) {
            validator_(value.data(), value.size());
        }
        setContent(std::move(value));
        return *this;
    }

   private:
    Validator validator_;
};
using BytesMessageBuilder = TypedMessageBuilder<std::string>;

}  // namespace pulsar

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

#include <functional>

namespace pulsar {

template <typename T>
class TypedMessage : public Message {
   public:
    using Decoder = std::function<T(const char*, std::size_t)>;

    TypedMessage() = default;

    TypedMessage(
        const Message& message, Decoder decoder = [](const char*, std::size_t) { return T{}; })
        : Message(message), decoder_(decoder) {}

    T getValue() const { return decoder_(static_cast<const char*>(getData()), getLength()); }

    TypedMessage& setDecoder(Decoder decoder) {
        decoder_ = decoder;
        return *this;
    }

   private:
    Decoder decoder_;
};

}  // namespace pulsar

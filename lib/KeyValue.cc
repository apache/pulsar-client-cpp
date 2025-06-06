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
#include <pulsar/KeyValue.h>

#include "KeyValueImpl.h"

namespace pulsar {

KeyValue::KeyValue(KeyValueImplPtr impl) : impl_(std::move(impl)) {}

KeyValue::KeyValue(std::string &&key, std::string &&value)
    : impl_(std::make_shared<KeyValueImpl>(std::move(key), std::move(value))) {}

std::string KeyValue::getKey() const { return impl_->getKey(); }

const void *KeyValue::getValue() const { return impl_->getValue(); }

size_t KeyValue::getValueLength() const { return impl_->getValueLength(); }

std::string KeyValue::getValueAsString() const { return impl_->getValueAsString(); }

}  // namespace pulsar

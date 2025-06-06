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

#include <pulsar/EncryptionKeyInfo.h>

#include "EncryptionKeyInfoImpl.h"

namespace pulsar {

EncryptionKeyInfo::EncryptionKeyInfo() : impl_(new EncryptionKeyInfoImpl()) {}

EncryptionKeyInfo::EncryptionKeyInfo(const EncryptionKeyInfoImplPtr& impl) : impl_(impl) {}

std::string& EncryptionKeyInfo::getKey() { return impl_->getKey(); }

void EncryptionKeyInfo::setKey(const std::string& key) { impl_->setKey(key); }

EncryptionKeyInfo::StringMap& EncryptionKeyInfo::getMetadata() { return impl_->getMetadata(); }

void EncryptionKeyInfo::setMetadata(StringMap& metadata) { impl_->setMetadata(metadata); }

}; /* namespace pulsar */

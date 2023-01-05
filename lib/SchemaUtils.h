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
#ifndef SCHEMA_UTILS_HPP_
#define SCHEMA_UTILS_HPP_

#include "SharedBuffer.h"

namespace pulsar {

static constexpr uint32_t INVALID_SIZE = 0xFFFFFFFF;
static const std::string KEY_SCHEMA_NAME = "key.schema.name";
static const std::string KEY_SCHEMA_TYPE = "key.schema.type";
static const std::string KEY_SCHEMA_PROPS = "key.schema.properties";
static const std::string VALUE_SCHEMA_NAME = "value.schema.name";
static const std::string VALUE_SCHEMA_TYPE = "value.schema.type";
static const std::string VALUE_SCHEMA_PROPS = "value.schema.properties";
static const std::string KV_ENCODING_TYPE = "kv.encoding.type";

/**
 * Merge keySchemaData and valueSchemaData.
 * @return
 */
static std::string mergeKeyValueSchema(const std::string& keySchemaData, const std::string& valueSchemaData) {
    uint32_t keySize = keySchemaData.size();
    uint32_t valueSize = valueSchemaData.size();

    auto buffSize = sizeof keySize + keySize + sizeof valueSize + valueSize;
    SharedBuffer buffer = SharedBuffer::allocate(buffSize);
    buffer.writeUnsignedInt(keySize == 0 ? INVALID_SIZE : static_cast<uint32_t>(keySize));
    buffer.write(keySchemaData.c_str(), static_cast<uint32_t>(keySize));
    buffer.writeUnsignedInt(valueSize == 0 ? INVALID_SIZE : static_cast<uint32_t>(valueSize));
    buffer.write(valueSchemaData.c_str(), static_cast<uint32_t>(valueSize));

    return std::string(buffer.data(), buffSize);
}

}  // namespace pulsar

#endif /* SCHEMA_UTILS_HPP_ */

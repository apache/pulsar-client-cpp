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

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "CompressionType.h"
#include "defines.h"

namespace pulsar {

namespace proto {
class MessageMetadata;
}

struct PULSAR_PUBLIC EncryptionKey {
    std::string key;
    std::string value;
    std::unordered_map<std::string, std::string> metadata;

    EncryptionKey(const std::string& key, const std::string& value,
                  const decltype(EncryptionKey::metadata)& metadata)
        : key(key), value(value), metadata(metadata) {}
};

/**
 * It contains encryption and compression information in it using which application can decrypt consumed
 * message with encrypted-payload.
 */
class PULSAR_PUBLIC EncryptionContext {
   public:
    using KeysType = std::vector<EncryptionKey>;

    /**
     * @return the map of encryption keys used for the message
     */
    const KeysType& keys() const noexcept { return keys_; }

    /**
     * @return the encryption parameter used for the message
     */
    const std::string& param() const noexcept { return param_; }

    /**
     * @return the encryption algorithm used for the message
     */
    const std::string& algorithm() const noexcept { return algorithm_; }

    /**
     * @return the compression type used for the message
     */
    CompressionType compressionType() const noexcept { return compressionType_; }

    /**
     * @return the uncompressed message size if the message is compressed, 0 otherwise
     */
    uint32_t uncompressedMessageSize() const noexcept { return uncompressedMessageSize_; }

    /**
     * @return the batch size if the message is part of a batch, -1 otherwise
     */
    int32_t batchSize() const noexcept { return batchSize_; }

    /**
     * When the `ConsumerConfiguration#getCryptoFailureAction` is set to `CONSUME`, the message will still be
     * returned even if the decryption failed. This method is provided to let users know whether the decryption
     * failed.
     *
     * @return whether the decryption failed
     */
    bool isDecryptionFailed() const noexcept { return isDecryptionFailed_; }

    /**
     * This constructor is public to allow in-place construction via std::optional
     * (e.g., `std::optional<EncryptionContext>(std::in_place, metadata, false)`),
     * but should not be used directly in application code.
     */
    EncryptionContext(const proto::MessageMetadata&, bool);

   private:
    KeysType keys_;
    std::string param_;
    std::string algorithm_;
    CompressionType compressionType_{CompressionNone};
    uint32_t uncompressedMessageSize_{0};
    int32_t batchSize_{-1};
    bool isDecryptionFailed_{false};

    void setDecryptionFailed(bool failed) noexcept { isDecryptionFailed_ = failed; }

    friend class ConsumerImpl;
};

}  // namespace pulsar

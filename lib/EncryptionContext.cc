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
#include <pulsar/EncryptionContext.h>

#include "PulsarApi.pb.h"

namespace pulsar {

static EncryptionContext::KeysType encryptedKeysFromMetadata(const proto::MessageMetadata& msgMetadata) {
    EncryptionContext::KeysType keys;
    for (auto&& key : msgMetadata.encryption_keys()) {
        decltype(EncryptionKey::metadata) metadata;
        for (int i = 0; i < key.metadata_size(); i++) {
            const auto& entry = key.metadata(i);
            metadata[entry.key()] = entry.value();
        }
        keys.emplace_back(key.key(), key.value(), std::move(metadata));
    }
    return keys;
}

EncryptionContext::EncryptionContext(const proto::MessageMetadata& msgMetadata, bool isDecryptionFailed)

    : keys_(encryptedKeysFromMetadata(msgMetadata)),
      param_(msgMetadata.encryption_param()),
      algorithm_(msgMetadata.encryption_algo()),
      compressionType_(static_cast<CompressionType>(msgMetadata.compression())),
      uncompressedMessageSize_(msgMetadata.uncompressed_size()),
      batchSize_(msgMetadata.has_num_messages_in_batch() ? msgMetadata.num_messages_in_batch() : -1),
      isDecryptionFailed_(isDecryptionFailed) {}

}  // namespace pulsar

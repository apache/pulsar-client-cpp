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
#include "CompressionCodec.h"

#include "CompressionCodecLZ4.h"
#include "CompressionCodecSnappy.h"
#include "CompressionCodecZLib.h"
#include "CompressionCodecZstd.h"

using namespace pulsar;
namespace pulsar {

CompressionCodecNone CompressionCodecProvider::compressionCodecNone_;
CompressionCodecLZ4 CompressionCodecProvider::compressionCodecLZ4_;
CompressionCodecZLib CompressionCodecProvider::compressionCodecZLib_;
CompressionCodecZstd CompressionCodecProvider::compressionCodecZstd_;
CompressionCodecSnappy CompressionCodecProvider::compressionCodecSnappy_;

CompressionCodec& CompressionCodecProvider::getCodec(CompressionType compressionType) {
    switch (compressionType) {
        case CompressionLZ4:
            return compressionCodecLZ4_;
        case CompressionZLib:
            return compressionCodecZLib_;
        case CompressionZSTD:
            return compressionCodecZstd_;
        case CompressionSNAPPY:
            return compressionCodecSnappy_;
        default:
            return compressionCodecNone_;
    }
}

SharedBuffer CompressionCodecNone::encode(const SharedBuffer& raw) { return raw; }

bool CompressionCodecNone::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                  SharedBuffer& decoded) {
    decoded = encoded;
    return true;
}
}  // namespace pulsar

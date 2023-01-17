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

#include "BitSet.h"

namespace pulsar {

class MessageIdImpl {
   public:
    MessageIdImpl() = default;
    MessageIdImpl(int32_t partition, int64_t ledgerId, int64_t entryId, int32_t batchIndex)
        : ledgerId_(ledgerId),
          entryId_(entryId),
          partition_(partition),
          batchIndex_(batchIndex),
          topicName_() {}
    virtual ~MessageIdImpl() {}

    int64_t ledgerId_ = -1;
    int64_t entryId_ = -1;
    int32_t partition_ = -1;
    int32_t batchIndex_ = -1;
    int32_t batchSize_ = 0;

    const std::string& getTopicName() { return *topicName_; }
    void setTopicName(const std::string& topicName) { topicName_ = &topicName; }

    virtual const BitSet& getBitSet(bool individual) const noexcept {
        static const BitSet emptyBitSet;
        return emptyBitSet;
    }

   private:
    const std::string* topicName_ = nullptr;
    friend class MessageImpl;
    friend class MultiTopicsConsumerImpl;
    friend class UnAckedMessageTrackerEnabled;
};
}  // namespace pulsar

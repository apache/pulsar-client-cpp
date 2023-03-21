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

#include <boost/functional/hash.hpp>
#include <cstdint>
#include <string>

#include "BitSet.h"

namespace std {

template <>
struct hash<pulsar::MessageId> {
    std::size_t operator()(const pulsar::MessageId& msgId) const {
        using boost::hash_combine;
        using boost::hash_value;

        // Start with a hash value of 0    .
        std::size_t seed = 0;

        // Modify 'seed' by XORing and bit-shifting in
        // one member of 'Key' after the other:
        hash_combine(seed, hash_value(msgId.ledgerId()));
        hash_combine(seed, hash_value(msgId.entryId()));
        hash_combine(seed, hash_value(msgId.batchIndex()));
        hash_combine(seed, hash_value(msgId.partition()));

        // Return the result.
        return seed;
    }
};
}  // namespace std

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

    const std::string& getTopicName() {
        static const std::string EMPTY_TOPIC = "";
        return topicName_ ? *topicName_ : EMPTY_TOPIC;
    }
    void setTopicName(const std::shared_ptr<std::string>& topicName) { topicName_ = topicName; }

    virtual const BitSet& getBitSet() const noexcept {
        static const BitSet emptyBitSet;
        return emptyBitSet;
    }

   private:
    std::shared_ptr<std::string> topicName_;
    friend class MessageImpl;
    friend class MultiTopicsConsumerImpl;
    friend class UnAckedMessageTrackerEnabled;
};
}  // namespace pulsar

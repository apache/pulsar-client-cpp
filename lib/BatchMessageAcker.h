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

#include <atomic>
#include <memory>
#include <mutex>

#include "BitSet.h"
#include "ProtoApiEnums.h"

namespace pulsar {

class BatchMessageAcker;
using BatchMessageAckerPtr = std::shared_ptr<BatchMessageAcker>;

class BatchMessageAcker {
   public:
    virtual ~BatchMessageAcker() {}
    // Return false for these methods so that batch index ACK will be falled back to if the acker is created
    // by deserializing from raw bytes.
    virtual bool ackIndividual(int32_t) { return false; }
    virtual bool ackCumulative(int32_t) { return false; }

    bool shouldAckPreviousMessageId() noexcept {
        bool expectedValue = false;
        return prevBatchCumulativelyAcked_.compare_exchange_strong(expectedValue, true);
    }

    virtual const BitSet& getBitSet() const noexcept {
        static BitSet emptyBitSet;
        return emptyBitSet;
    }

   private:
    // When a batched message is acknowledged cumulatively, the previous message id will be acknowledged
    // without batch index ACK enabled. However, it should be acknowledged only once. Use this flag to
    // determine whether to acknowledge the previous message id.
    std::atomic_bool prevBatchCumulativelyAcked_{false};
};

class BatchMessageAckerImpl : public BatchMessageAcker {
   public:
    using Lock = std::lock_guard<std::mutex>;

    static BatchMessageAckerPtr create(int32_t batchSize) {
        if (batchSize > 0) {
            return std::make_shared<BatchMessageAckerImpl>(batchSize);
        } else {
            return std::make_shared<BatchMessageAcker>();
        }
    }

    BatchMessageAckerImpl(int32_t batchSize) : bitSet_(batchSize) { bitSet_.set(0, batchSize); }

    bool ackIndividual(int32_t batchIndex) override {
        Lock lock{mutex_};
        bitSet_.clear(batchIndex);
        return bitSet_.isEmpty();
    }

    bool ackCumulative(int32_t batchIndex) override {
        Lock lock{mutex_};
        // The range of cumulative acknowledgment is closed while BitSet::clear accepts a left-closed
        // right-open range.
        bitSet_.clear(0, batchIndex + 1);
        return bitSet_.isEmpty();
    }

    const BitSet& getBitSet() const noexcept override { return bitSet_; }

   private:
    BitSet bitSet_;
    mutable std::mutex mutex_;
};

}  // namespace pulsar

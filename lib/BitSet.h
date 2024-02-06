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

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <vector>

// This class migrates the BitSet class from Java to have essential methods.
// Here `boost::dynamic_bitset` is not used because we need to convert the dynamic bit set
// to a long array because the Pulsar API uses a long array to represent the bit set.
namespace pulsar {

class BitSet {
   public:
    // We must store the unsigned integer to make operator >> equivalent to Java's >>>
    using Data = std::vector<uint64_t>;
    using const_iterator = typename Data::const_iterator;

    BitSet() {}

    BitSet(int32_t numBits) : words_((numBits / 64) + ((numBits % 64 == 0) ? 0 : 1)) { assert(numBits > 0); }

    BitSet(Data&& words) : words_(std::move(words)), wordsInUse_(words_.size()) {}

    // Support range loop like:
    // ```c++
    // BitSet bitSet(129);
    // for (auto x : bitSet) { /* ... */ }
    // ```
    const_iterator begin() const noexcept { return words_.begin(); }
    const_iterator end() const noexcept { return words_.begin() + wordsInUse_; }

    /**
     * Returns true if this {@code BitSet} contains no bits that are set
     * to {@code true}.
     *
     * @return boolean indicating whether this {@code BitSet} is empty
     */
    bool isEmpty() const noexcept { return wordsInUse_ == 0; }

    /**
     * Returns the value of the bit with the specific index. The value is {@code true} if the bit with the
     * index {@code bitIndex} is currently set in this {@code BitSet}; otherwise, the result is {@code false}.
     *
     * @param  bitIndex   the bit index
     * @return the value of the bit with the specified index
     */
    bool get(int32_t bitIndex) const;

    /**
     * Sets the bits from the specified {@code fromIndex} (inclusive) to the
     * specified {@code toIndex} (exclusive) to {@code true}.
     *
     * @param  fromIndex index of the first bit to be set
     * @param  toIndex index after the last bit to be set
     */
    void set(int32_t fromIndex, int32_t toIndex);

    /**
     * Sets the bits from the specified {@code fromIndex} (inclusive) to the
     * specified {@code toIndex} (exclusive) to {@code false}.
     *
     * @param  fromIndex index of the first bit to be cleared
     * @param  toIndex index after the last bit to be cleared
     */
    void clear(int32_t fromIndex, int32_t toIndex);

    /**
     * Sets the bit specified by the index to {@code false}.
     *
     * @param  bitIndex the index of the bit to be cleared
     */
    void clear(int32_t bitIndex);

   private:
    Data words_;
    int32_t wordsInUse_ = 0;

    static constexpr int32_t ADDRESS_BITS_PER_WORD = 6;
    static constexpr int32_t BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    static constexpr uint64_t WORD_MASK = 0xffffffffffffffffL;

    static constexpr int32_t wordIndex(int32_t bitIndex) { return bitIndex >> ADDRESS_BITS_PER_WORD; }

    void expandTo(int32_t wordIndex) {
        auto wordsRequired = wordIndex + 1;
        if (wordsInUse_ < wordsRequired) {
            words_.resize(wordsRequired);
            wordsInUse_ = wordsRequired;
        }
    }

    int32_t length() {
        if (wordsInUse_ == 0) {
            return 0;
        }
        return BITS_PER_WORD * (wordsInUse_ - 1) +
               (BITS_PER_WORD - numberOfLeadingZeros(words_[wordsInUse_ - 1]));
    }

    void recalculateWordsInUse() {
        // Traverse the bitset until a used word is found
        int32_t i;
        for (i = wordsInUse_ - 1; i >= 0; i--) {
            if (words_[i] != 0) {
                break;
            }
        }
        wordsInUse_ = i + 1;
    }

    static int32_t numberOfLeadingZeros(uint64_t i) {
        auto x = static_cast<uint32_t>(i >> 32);
        return x == 0 ? 32 + numberOfLeadingZeros(static_cast<uint32_t>(i)) : numberOfLeadingZeros(x);
    }

    static int32_t numberOfLeadingZeros(uint32_t i) {
        if (i <= 0) {
            return i == 0 ? 32 : 0;
        }
        int32_t n = 31;
        if (i >= (1 << 16)) {
            n -= 16;
            i >>= 16;
        }
        if (i >= (1 << 8)) {
            n -= 8;
            i >>= 8;
        }
        if (i >= (1 << 4)) {
            n -= 4;
            i >>= 4;
        }
        if (i >= (1 << 2)) {
            n -= 2;
            i >>= 2;
        }
        return n - (i >> 1);
    }

    // In C++, when shift count is negative or >= width of type, the behavior is undefined. We should
    // convert n to the valid range [0, sizeof(T)) first.
    static constexpr int32_t safeShiftCount(int32_t width, int32_t n) {
        return (n < 0) ? safeShiftCount(width, n + width)
                       : ((n >= width) ? safeShiftCount(width, n - width) : n);
    }

    template <typename T>
    static T safeLeftShift(T x, int32_t n) {
        return (x << safeShiftCount(sizeof(x) * 8, n));
    }

    template <typename T>
    static T safeRightShift(T x, int32_t n) {
        return (x >> safeShiftCount(sizeof(x) * 8, n));
    }
};

inline bool BitSet::get(int32_t bitIndex) const {
    assert(bitIndex >= 0);
    auto wordIndex_ = wordIndex(bitIndex);
    return (wordIndex_ < wordsInUse_) && ((words_[wordIndex_] & (1L << bitIndex)) != 0);
}

inline void BitSet::set(int32_t fromIndex, int32_t toIndex) {
    assert(fromIndex < toIndex && fromIndex >= 0 && toIndex >= 0);
    if (fromIndex == toIndex) {
        return;
    }

    auto startWordIndex = wordIndex(fromIndex);
    auto endWordIndex = wordIndex(toIndex - 1);
    expandTo(endWordIndex);

    auto firstWordMask = safeLeftShift(WORD_MASK, fromIndex);
    auto lastWordMask = safeRightShift(WORD_MASK, -toIndex);
    if (startWordIndex == endWordIndex) {
        // Case 1: One word
        words_[startWordIndex] |= (firstWordMask & lastWordMask);
    } else {
        // Case 2: Multiple words
        // Handle first word
        words_[startWordIndex] |= firstWordMask;

        // Handle intermediate words, if any
        for (int32_t i = startWordIndex + 1; i < endWordIndex; i++) {
            words_[i] = WORD_MASK;
        }

        // Handle last word (restores invariants)
        words_[endWordIndex] |= lastWordMask;
    }
}

inline void BitSet::clear(int32_t fromIndex, int32_t toIndex) {
    assert(fromIndex < toIndex && fromIndex >= 0 && toIndex >= 0);
    if (fromIndex == toIndex) {
        return;
    }

    auto startWordIndex = wordIndex(fromIndex);
    if (startWordIndex >= wordsInUse_) {
        return;
    }

    auto endWordIndex = wordIndex(toIndex - 1);
    if (endWordIndex >= wordsInUse_) {
        toIndex = length();
        endWordIndex = wordsInUse_ - 1;
    }

    auto firstWordMask = safeLeftShift(WORD_MASK, fromIndex);
    auto lastWordMask = safeRightShift(WORD_MASK, -toIndex);

    if (startWordIndex == endWordIndex) {
        // Case 1: One word
        words_[startWordIndex] &= ~(firstWordMask & lastWordMask);
    } else {
        // Case 2: Multiple words
        // Handle first word
        words_[startWordIndex] &= ~firstWordMask;

        // Handle intermediate words, if any
        for (int32_t i = startWordIndex + 1; i < endWordIndex; i++) {
            words_[i] = 0;
        }

        // Handle last word
        words_[endWordIndex] &= ~lastWordMask;
    }

    recalculateWordsInUse();
}

inline void BitSet::clear(int32_t bitIndex) {
    assert(bitIndex >= 0);

    auto i = wordIndex(bitIndex);
    if (i >= wordsInUse_) {
        return;
    }

    words_[i] &= ~(safeLeftShift(1L, bitIndex));

    recalculateWordsInUse();
}

}  // namespace pulsar

#pragma once

#include <cstdint>
#include <iostream>

namespace pulsar {

class TxnID {
    int64_t msb_;
    int64_t lsb_;

public:
    TxnID() : msb_(0), lsb_(0) {}
    TxnID(int64_t msb, int64_t lsb) : msb_(msb), lsb_(lsb) {}

    // Getters (Java-style, for compatibility with your test code)
    int64_t mostSigBits() const { return msb_; }
    int64_t leastSigBits() const { return lsb_; }

    // Equality check
    bool operator==(const TxnID& other) const {
        return msb_ == other.msb_ && lsb_ == other.lsb_;
    }

    // For debugging
    friend std::ostream& operator<<(std::ostream& os, const TxnID& txnId) {
        os << txnId.msb_ << ":" << txnId.lsb_;
        return os;
    }
};

} // namespace pulsar


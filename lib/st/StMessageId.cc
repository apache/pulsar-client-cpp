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
#include <pulsar/st/MessageId.h>

#include <cstdint>
#include <exception>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "MessageIdImpl.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar::st {

namespace {

// The serialized form is the Java client's MessageIdV5 layout (big-endian):
//   int64 segmentId
//   int32 v4Length, v4 bytes            (classic MessageId serialization)
//   int32 positionCount, {int64 segmentId, int32 length, bytes} entries
//   int32 parentTopicLength (-1 = absent), utf-8 bytes
//   int32 multiTopicCount   (-1 = absent),
//        {int32 topicLength, bytes, int32 positionCount, entries} entries
// The trailing sections are optional on read, so shorter (older) forms parse.

void putInt32(std::vector<std::byte>& out, std::int32_t value) {
    auto u = static_cast<std::uint32_t>(value);
    for (int shift = 24; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::byte>((u >> shift) & 0xFF));
    }
}

void putInt64(std::vector<std::byte>& out, std::int64_t value) {
    auto u = static_cast<std::uint64_t>(value);
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::byte>((u >> shift) & 0xFF));
    }
}

void putBytes(std::vector<std::byte>& out, const std::string& bytes) {
    const auto* p = reinterpret_cast<const std::byte*>(bytes.data());
    out.insert(out.end(), p, p + bytes.size());
}

class Reader {
   public:
    explicit Reader(std::span<const std::byte> data) : data_(data) {}

    bool hasRemaining() const { return pos_ < data_.size(); }

    bool readInt32(std::int32_t& value) {
        if (data_.size() - pos_ < 4) return false;
        std::uint32_t u = 0;
        for (int i = 0; i < 4; i++) {
            u = (u << 8) | std::to_integer<std::uint32_t>(data_[pos_++]);
        }
        value = static_cast<std::int32_t>(u);
        return true;
    }

    bool readInt64(std::int64_t& value) {
        if (data_.size() - pos_ < 8) return false;
        std::uint64_t u = 0;
        for (int i = 0; i < 8; i++) {
            u = (u << 8) | std::to_integer<std::uint64_t>(data_[pos_++]);
        }
        value = static_cast<std::int64_t>(u);
        return true;
    }

    bool readBytes(std::int32_t length, std::string& out) {
        if (length < 0 || static_cast<std::size_t>(length) > data_.size() - pos_) return false;
        out.assign(reinterpret_cast<const char*>(data_.data() + pos_), static_cast<std::size_t>(length));
        pos_ += static_cast<std::size_t>(length);
        return true;
    }

   private:
    std::span<const std::byte> data_;
    std::size_t pos_ = 0;
};

void writePositionVector(std::vector<std::byte>& out,
                         const std::map<std::int64_t, pulsar::MessageId>& vector) {
    putInt32(out, static_cast<std::int32_t>(vector.size()));
    for (const auto& entry : vector) {
        putInt64(out, entry.first);
        std::string serialized;
        entry.second.serialize(serialized);
        putInt32(out, static_cast<std::int32_t>(serialized.size()));
        putBytes(out, serialized);
    }
}

bool readPositionVector(Reader& reader, std::map<std::int64_t, pulsar::MessageId>& out) {
    std::int32_t count;
    if (!reader.readInt32(count) || count < 0) return false;
    for (std::int32_t i = 0; i < count; i++) {
        std::int64_t segmentId;
        std::int32_t length;
        std::string bytes;
        if (!reader.readInt64(segmentId) || !reader.readInt32(length) || !reader.readBytes(length, bytes)) {
            return false;
        }
        out.emplace(segmentId, pulsar::MessageId::deserialize(bytes));
    }
    return true;
}

}  // namespace

// An empty id: impl_ stays null, so the handle is falsy under operator bool and
// compares equal only to other empty ids.
MessageId::MessageId() = default;

MessageId::MessageId(std::shared_ptr<MessageIdImpl> impl) : impl_(std::move(impl)) {}

const MessageId& MessageId::earliest() {
    static const MessageId id =
        MessageIdFactory::create(pulsar::MessageId::earliest(), MessageIdImpl::kNoSegment);
    return id;
}

const MessageId& MessageId::latest() {
    static const MessageId id =
        MessageIdFactory::create(pulsar::MessageId::latest(), MessageIdImpl::kNoSegment);
    return id;
}

std::vector<std::byte> MessageId::toByteArray() const {
    if (!impl_) {
        return {};
    }
    std::vector<std::byte> out;
    putInt64(out, impl_->segmentId);

    std::string v4Bytes;
    impl_->v4MessageId.serialize(v4Bytes);
    putInt32(out, static_cast<std::int32_t>(v4Bytes.size()));
    putBytes(out, v4Bytes);

    writePositionVector(out, impl_->positionVector);

    if (impl_->parentTopic) {
        putInt32(out, static_cast<std::int32_t>(impl_->parentTopic->size()));
        putBytes(out, *impl_->parentTopic);
    } else {
        putInt32(out, -1);
    }

    if (impl_->multiTopicVector) {
        putInt32(out, static_cast<std::int32_t>(impl_->multiTopicVector->size()));
        for (const auto& entry : *impl_->multiTopicVector) {
            putInt32(out, static_cast<std::int32_t>(entry.first.size()));
            putBytes(out, entry.first);
            writePositionVector(out, entry.second);
        }
    } else {
        putInt32(out, -1);
    }
    return out;
}

MessageId MessageId::fromByteArray(std::span<const std::byte> data) {
    try {
        Reader reader(data);
        auto impl = std::make_shared<MessageIdImpl>();

        std::int32_t v4Length;
        std::string v4Bytes;
        if (!reader.readInt64(impl->segmentId) || !reader.readInt32(v4Length) ||
            !reader.readBytes(v4Length, v4Bytes)) {
            LOG_WARN("Discarding malformed scalable-topic MessageId: " << data.size() << " bytes");
            return MessageId();
        }
        impl->v4MessageId = pulsar::MessageId::deserialize(v4Bytes);

        if (reader.hasRemaining() && !readPositionVector(reader, impl->positionVector)) {
            LOG_WARN("Discarding scalable-topic MessageId with a malformed position vector");
            return MessageId();
        }

        if (reader.hasRemaining()) {
            std::int32_t parentLength;
            if (!reader.readInt32(parentLength)) {
                return MessageId();
            }
            if (parentLength >= 0) {
                std::string parent;
                if (!reader.readBytes(parentLength, parent)) {
                    return MessageId();
                }
                impl->parentTopic = std::move(parent);
            }
        }

        if (reader.hasRemaining()) {
            std::int32_t topicCount;
            if (!reader.readInt32(topicCount)) {
                return MessageId();
            }
            if (topicCount >= 0) {
                std::map<std::string, std::map<std::int64_t, pulsar::MessageId>> multi;
                for (std::int32_t i = 0; i < topicCount; i++) {
                    std::int32_t topicLength;
                    std::string topic;
                    std::map<std::int64_t, pulsar::MessageId> inner;
                    if (!reader.readInt32(topicLength) || !reader.readBytes(topicLength, topic) ||
                        !readPositionVector(reader, inner)) {
                        return MessageId();
                    }
                    multi.emplace(std::move(topic), std::move(inner));
                }
                impl->multiTopicVector = std::move(multi);
            }
        }
        return MessageIdFactory::create(std::move(impl));
    } catch (const std::exception& e) {
        // The classic MessageId deserialization throws on malformed proto bytes.
        LOG_WARN("Discarding malformed scalable-topic MessageId: " << e.what());
        return MessageId();
    }
}

std::strong_ordering MessageId::operator<=>(const MessageId& other) const {
    // Empty ids sort before every real id and tie with each other.
    if (!impl_ || !other.impl_) {
        return static_cast<bool>(impl_) <=> static_cast<bool>(other.impl_);
    }
    if (auto cmp = impl_->segmentId <=> other.impl_->segmentId; cmp != 0) {
        return cmp;
    }
    if (impl_->v4MessageId == other.impl_->v4MessageId) {
        return std::strong_ordering::equal;
    }
    return impl_->v4MessageId < other.impl_->v4MessageId ? std::strong_ordering::less
                                                         : std::strong_ordering::greater;
}

bool MessageId::operator==(const MessageId& other) const {
    if (!impl_ || !other.impl_) {
        return static_cast<bool>(impl_) == static_cast<bool>(other.impl_);
    }
    return impl_->segmentId == other.impl_->segmentId && impl_->v4MessageId == other.impl_->v4MessageId;
}

std::ostream& operator<<(std::ostream& s, const MessageId& messageId) {
    if (!messageId.impl_) {
        return s << "{empty}";
    }
    return s << "{segment=" << messageId.impl_->segmentId << ", id=" << messageId.impl_->v4MessageId
             << ", positions=" << messageId.impl_->positionVector.size() << "}";
}

}  // namespace pulsar::st

std::size_t std::hash<pulsar::st::MessageId>::operator()(
    const pulsar::st::MessageId& messageId) const noexcept {
    const auto& impl = pulsar::st::MessageIdFactory::impl(messageId);
    if (!impl) {
        return 0;
    }
    std::size_t seed = std::hash<std::int64_t>()(impl->segmentId);
    auto combine = [&seed](std::size_t h) { seed ^= h + 0x9e3779b9 + (seed << 6) + (seed >> 2); };
    combine(std::hash<std::int64_t>()(impl->v4MessageId.ledgerId()));
    combine(std::hash<std::int64_t>()(impl->v4MessageId.entryId()));
    combine(std::hash<std::int32_t>()(impl->v4MessageId.batchIndex()));
    combine(std::hash<std::int32_t>()(impl->v4MessageId.partition()));
    return seed;
}

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

#include <pulsar/MessageId.h>
#include <pulsar/st/MessageId.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace pulsar::st {

/**
 * The hidden state of a pulsar::st::MessageId, mirroring the Java client's
 * MessageIdV5 field-for-field so the two clients' serialized ids interoperate:
 * the classic per-segment message id plus the segment it belongs to, and the
 * optional position-vector sections used by the consumer side (a cumulative-ack
 * StreamConsumer id also carries the other segments' positions; a namespace
 * consumer's id carries them per topic). The producer path fills only
 * v4MessageId + segmentId.
 */
class MessageIdImpl {
   public:
    /** segmentId of the earliest/latest sentinels, which span all segments. */
    static constexpr std::int64_t kNoSegment = -1;

    pulsar::MessageId v4MessageId;
    std::int64_t segmentId = kNoSegment;
    /** Per-segment positions for cumulative-ack ids. Empty on the producer path. */
    std::map<std::int64_t, pulsar::MessageId> positionVector;
    /** The scalable topic this id belongs to, when carried (topic://...). */
    std::optional<std::string> parentTopic;
    /** Cross-topic position vectors for namespace consumers, when carried. */
    std::optional<std::map<std::string, std::map<std::int64_t, pulsar::MessageId>>> multiTopicVector;
};

/**
 * INTERNAL factory befriended by the public MessageId handle: the only way
 * lib/st mints typed ids and reaches back into their hidden state.
 */
class MessageIdFactory {
   public:
    static MessageId create(std::shared_ptr<MessageIdImpl> impl) { return MessageId(std::move(impl)); }

    /** The producer path: an id for one published message in one segment. */
    static MessageId create(const pulsar::MessageId& v4MessageId, std::int64_t segmentId) {
        auto impl = std::make_shared<MessageIdImpl>();
        impl->v4MessageId = v4MessageId;
        impl->segmentId = segmentId;
        return MessageId(std::move(impl));
    }

    static const std::shared_ptr<MessageIdImpl>& impl(const MessageId& id) { return id.impl_; }
};

}  // namespace pulsar::st

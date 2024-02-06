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
#ifndef LIB_ACKGROUPINGTRACKERDISABLED_H_
#define LIB_ACKGROUPINGTRACKERDISABLED_H_

#include <cstdint>

#include "AckGroupingTracker.h"

namespace pulsar {

class HandlerBase;

/**
 * @class AckGroupingTrackerDisabled
 * ACK grouping tracker that does not tracker or group ACK requests. The ACK requests are diretly
 * sent to broker.
 */
class AckGroupingTrackerDisabled : public AckGroupingTracker {
   public:
    using AckGroupingTracker::AckGroupingTracker;
    virtual ~AckGroupingTrackerDisabled() = default;

    void addAcknowledge(const MessageId& msgId, ResultCallback callback) override;
    void addAcknowledgeList(const MessageIdList& msgIds, ResultCallback callback) override;
    void addAcknowledgeCumulative(const MessageId& msgId, ResultCallback callback) override;
};  // class AckGroupingTrackerDisabled

}  // namespace pulsar
#endif /* LIB_ACKGROUPINGTRACKERDISABLED_H_ */

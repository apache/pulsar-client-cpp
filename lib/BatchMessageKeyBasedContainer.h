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
#ifndef LIB_BATCHMESSAGEKEYBASEDCONTAINER_H_
#define LIB_BATCHMESSAGEKEYBASEDCONTAINER_H_

#include <unordered_map>

#include "BatchMessageContainerBase.h"
#include "MessageAndCallbackBatch.h"

namespace pulsar {

class BatchMessageKeyBasedContainer : public BatchMessageContainerBase {
   public:
    BatchMessageKeyBasedContainer(const ProducerImpl& producer);

    ~BatchMessageKeyBasedContainer();

    bool hasMultiOpSendMsgs() const override { return true; }

    bool isFirstMessageToAdd(const Message& msg) const override;

    bool add(const Message& msg, const SendCallback& callback) override;

    std::vector<std::unique_ptr<OpSendMsg>> createOpSendMsgs(const FlushCallback& flushCallback) override;

    void serialize(std::ostream& os) const override;

   private:
    // key: message key, ordering key has higher priority than partitioned key
    std::unordered_map<std::string, MessageAndCallbackBatch> batches_;
    size_t numberOfBatchesSent_ = 0;
    double averageBatchSize_ = 0;

    void clear() override;
};

}  // namespace pulsar

#endif

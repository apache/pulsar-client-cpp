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
#ifndef LIB_CONSUMERCONFIGURATIONIMPL_H_
#define LIB_CONSUMERCONFIGURATIONIMPL_H_

#include <pulsar/ConsumerConfiguration.h>

namespace pulsar {
struct ConsumerConfigurationImpl {
    long unAckedMessagesTimeoutMs{0};
    long tickDurationInMs{1000};
    long negativeAckRedeliveryDelayMs{60000};
    long ackGroupingTimeMs{100};
    long ackGroupingMaxSize{1000};
    long brokerConsumerStatsCacheTimeInMs{30 * 1000L};  // 30 seconds
    long expireTimeOfIncompleteChunkedMessageMs{60000};
    SchemaInfo schemaInfo;
    ConsumerEventListenerPtr eventListener;
    CryptoKeyReaderPtr cryptoKeyReader;
    InitialPosition subscriptionInitialPosition{InitialPosition::InitialPositionLatest};
    int patternAutoDiscoveryPeriod{60};
    RegexSubscriptionMode regexSubscriptionMode{RegexSubscriptionMode::PersistentOnly};
    int priorityLevel{0};
    bool hasMessageListener{false};
    bool hasConsumerEventListener{false};
    bool readCompacted{false};
    bool replicateSubscriptionStateEnabled{false};
    bool autoAckOldestChunkedMessageOnQueueFull{false};
    bool startMessageIdInclusive{false};
    bool batchIndexAckEnabled{false};
    bool ackReceiptEnabled{false};
    bool startPaused{false};

    size_t maxPendingChunkedMessage{10};
    ConsumerType consumerType{ConsumerExclusive};
    MessageListener messageListener;
    int receiverQueueSize{1000};
    int maxTotalReceiverQueueSizeAcrossPartitions{50000};
    std::string consumerName;
    ConsumerCryptoFailureAction cryptoFailureAction{ConsumerCryptoFailureAction::FAIL};
    BatchReceivePolicy batchReceivePolicy{};
    DeadLetterPolicy deadLetterPolicy;
    std::map<std::string, std::string> properties;
    std::map<std::string, std::string> subscriptionProperties;
    KeySharedPolicy keySharedPolicy;
    std::vector<ConsumerInterceptorPtr> interceptors;
};
}  // namespace pulsar
#endif /* LIB_CONSUMERCONFIGURATIONIMPL_H_ */

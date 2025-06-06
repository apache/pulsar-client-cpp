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
#ifndef PULSAR_CONSUMERCONFIGURATION_H_
#define PULSAR_CONSUMERCONFIGURATION_H_

#include <pulsar/ConsumerCryptoFailureAction.h>
#include <pulsar/ConsumerEventListener.h>
#include <pulsar/ConsumerInterceptor.h>
#include <pulsar/ConsumerType.h>
#include <pulsar/CryptoKeyReader.h>
#include <pulsar/InitialPosition.h>
#include <pulsar/KeySharedPolicy.h>
#include <pulsar/Message.h>
#include <pulsar/RegexSubscriptionMode.h>
#include <pulsar/Result.h>
#include <pulsar/Schema.h>
#include <pulsar/TypedMessage.h>
#include <pulsar/defines.h>

#include <functional>
#include <memory>

#include "BatchReceivePolicy.h"
#include "DeadLetterPolicy.h"

namespace pulsar {

class Consumer;
class PulsarWrapper;
class PulsarFriend;

/// Callback definition for non-data operation
typedef std::vector<Message> Messages;
typedef std::function<void(Result result)> ResultCallback;
typedef std::function<void(Result, const Message& msg)> ReceiveCallback;
typedef std::function<void(Result, const Messages& msgs)> BatchReceiveCallback;
typedef std::function<void(Result result, MessageId messageId)> GetLastMessageIdCallback;

/// Callback definition for MessageListener
typedef std::function<void(Consumer& consumer, const Message& msg)> MessageListener;

typedef std::shared_ptr<ConsumerEventListener> ConsumerEventListenerPtr;

struct ConsumerConfigurationImpl;

/**
 * Class specifying the configuration of a consumer.
 */
class PULSAR_PUBLIC ConsumerConfiguration {
   public:
    ConsumerConfiguration();
    ~ConsumerConfiguration();
    ConsumerConfiguration(const ConsumerConfiguration&);
    ConsumerConfiguration& operator=(const ConsumerConfiguration&);

    /**
     * Create a new instance of ConsumerConfiguration with the same
     * initial settings as the current one.
     */
    ConsumerConfiguration clone() const;

    /**
     * Declare the schema of the data that this consumer will be accepting.
     *
     * The schema will be checked against the schema of the topic, and the
     * consumer creation will fail if it's not compatible.
     *
     * @param schemaInfo the schema definition object
     */
    ConsumerConfiguration& setSchema(const SchemaInfo& schemaInfo);

    /**
     * @return the schema information declared for this consumer
     */
    const SchemaInfo& getSchema() const;

    /**
     * Specify the consumer type. The consumer type enables
     * specifying the type of subscription. In Exclusive subscription,
     * only a single consumer is allowed to attach to the subscription. Other consumers
     * will get an error message. In Shared subscription, multiple consumers will be
     * able to use the same subscription name and the messages will be dispatched in a
     * round robin fashion. In Failover subscription, a primary-failover subscription model
     * allows for multiple consumers to attach to a single subscription, though only one
     * of them will be “master” at a given time. Only the primary consumer will receive
     * messages. When the primary consumer gets disconnected, one among the failover
     * consumers will be promoted to primary and will start getting messages.
     */
    ConsumerConfiguration& setConsumerType(ConsumerType consumerType);

    /**
     * @return the consumer type
     */
    ConsumerType getConsumerType() const;

    /**
     * Set KeyShared subscription policy for consumer.
     *
     * By default, KeyShared subscription use auto split hash range to maintain consumers. If you want to
     * set a different KeyShared policy, you can set by following example:
     *
     * @param keySharedPolicy The {@link KeySharedPolicy} want to specify
     */
    ConsumerConfiguration& setKeySharedPolicy(const KeySharedPolicy& keySharedPolicy);

    /**
     * @return the KeyShared subscription policy
     */
    KeySharedPolicy getKeySharedPolicy() const;

    /**
     * A message listener enables your application to configure how to process
     * and acknowledge messages delivered. A listener will be called in order
     * for every message received.
     */
    ConsumerConfiguration& setMessageListener(MessageListener messageListener);

    template <typename T>
    ConsumerConfiguration& setTypedMessageListener(
        std::function<void(Consumer&, const TypedMessage<T>&)> listener,
        typename TypedMessage<T>::Decoder decoder) {
        return setMessageListener([listener, decoder](Consumer& consumer, const Message& msg) {
            listener(consumer, TypedMessage<T>{msg, decoder});
        });
    }

    /**
     * @return the message listener
     */
    MessageListener getMessageListener() const;

    /**
     * @return true if the message listener has been set
     */
    bool hasMessageListener() const;

    /**
     * A event listener enables your application to react the consumer state
     * change event (active or inactive).
     */
    ConsumerConfiguration& setConsumerEventListener(ConsumerEventListenerPtr eventListener);

    /**
     * @return the consumer event listener
     */
    ConsumerEventListenerPtr getConsumerEventListener() const;

    /**
     * @return true if the consumer event listener has been set
     */
    bool hasConsumerEventListener() const;

    /**
     * Sets the size of the consumer receive queue.
     *
     * The consumer receive queue controls how many messages can be accumulated by the consumer before the
     * application calls receive(). Using a higher value may potentially increase the consumer throughput
     * at the expense of bigger memory utilization.
     *
     * Setting the consumer queue size to 0 decreases the throughput of the consumer by disabling
     * pre-fetching of
     * messages. This approach improves the message distribution on shared subscription by pushing messages
     * only to
     * the consumers that are ready to process them. Neither receive with timeout nor partitioned topics can
     * be
     * used if the consumer queue size is 0. The receive() function call should not be interrupted when
     * the consumer queue size is 0.
     *
     * The default value is 1000 messages and it is appropriate for the most use cases.
     *
     * @param size the new receiver queue size value
     *
     */
    void setReceiverQueueSize(int size);

    /**
     * @return the receiver queue size
     */
    int getReceiverQueueSize() const;

    /**
     * Set the max total receiver queue size across partitons.
     *
     * This setting is used to reduce the receiver queue size for individual partitions
     * {@link #setReceiverQueueSize(int)} if the total exceeds this value (default: 50000).
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     */
    void setMaxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * @return the configured max total receiver queue size across partitions
     */
    int getMaxTotalReceiverQueueSizeAcrossPartitions() const;

    /**
     * Set the consumer name.
     *
     * @param consumerName
     */
    void setConsumerName(const std::string& consumerName);

    /**
     * @return the consumer name
     */
    const std::string& getConsumerName() const;

    /**
     * Set the timeout in milliseconds for unacknowledged messages, the timeout needs to be greater than
     * 10 seconds. An Exception is thrown if the given value is less than 10000 (10 seconds).
     * If a successful acknowledgement is not sent within the timeout all the unacknowledged messages are
     * redelivered.
     *
     * Default: 0, which means the the tracker for unacknowledged messages is disabled.
     *
     * @param timeout in milliseconds
     */
    void setUnAckedMessagesTimeoutMs(const uint64_t milliSeconds);

    /**
     * @return the configured timeout in milliseconds for unacked messages.
     */
    long getUnAckedMessagesTimeoutMs() const;

    /**
     * Set the tick duration time that defines the granularity of the ack-timeout redelivery (in
     * milliseconds).
     *
     * The default value is 1000, which means 1 second.
     *
     * Using a higher tick time reduces
     * the memory overhead to track messages when the ack-timeout is set to a bigger value.
     *
     * @param milliSeconds the tick duration time (in milliseconds)
     */
    void setTickDurationInMs(const uint64_t milliSeconds);

    /**
     * @return the tick duration time (in milliseconds)
     */
    long getTickDurationInMs() const;

    /**
     * Set the delay to wait before re-delivering messages that have failed to be process.
     *
     * When application uses {@link Consumer#negativeAcknowledge(Message)}, the failed message
     * will be redelivered after a fixed timeout. The default is 1 min.
     *
     * @param redeliveryDelay
     *            redelivery delay for failed messages
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    void setNegativeAckRedeliveryDelayMs(long redeliveryDelayMillis);

    /**
     * Get the configured delay to wait before re-delivering messages that have failed to be process.
     *
     * @return redelivery delay for failed messages
     */
    long getNegativeAckRedeliveryDelayMs() const;

    /**
     * Set time window in milliseconds for grouping message ACK requests. An ACK request is not sent
     * to broker until the time window reaches its end, or the number of grouped messages reaches
     * limit. Default is 100 milliseconds. If it's set to a non-positive value, ACK requests will be
     * directly sent to broker without grouping.
     *
     * @param ackGroupMillis time of ACK grouping window in milliseconds.
     */
    void setAckGroupingTimeMs(long ackGroupingMillis);

    /**
     * Get grouping time window in milliseconds.
     *
     * @return grouping time window in milliseconds.
     */
    long getAckGroupingTimeMs() const;

    /**
     * Set max number of grouped messages within one grouping time window. If it's set to a
     * non-positive value, number of grouped messages is not limited. Default is 1000.
     *
     * @param maxGroupingSize max number of grouped messages with in one grouping time window.
     */
    void setAckGroupingMaxSize(long maxGroupingSize);

    /**
     * Get max number of grouped messages within one grouping time window.
     *
     * @return max number of grouped messages within one grouping time window.
     */
    long getAckGroupingMaxSize() const;

    /**
     * Set the time duration for which the broker side consumer stats will be cached in the client.
     *
     * Default: 30000, which means 30 seconds.
     *
     * @param cacheTimeInMs in milliseconds
     */
    void setBrokerConsumerStatsCacheTimeInMs(const long cacheTimeInMs);

    /**
     * @return the configured timeout in milliseconds caching BrokerConsumerStats.
     */
    long getBrokerConsumerStatsCacheTimeInMs() const;

    /**
     * @return true if encryption keys are added
     */
    bool isEncryptionEnabled() const;

    /**
     * @return the shared pointer to CryptoKeyReader.
     */
    const CryptoKeyReaderPtr getCryptoKeyReader() const;

    /**
     * Set the shared pointer to CryptoKeyReader.
     *
     * @param the shared pointer to CryptoKeyReader
     */
    ConsumerConfiguration& setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);

    /**
     * @return the ConsumerCryptoFailureAction
     */
    ConsumerCryptoFailureAction getCryptoFailureAction() const;

    /**
     * Set the ConsumerCryptoFailureAction.
     */
    ConsumerConfiguration& setCryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * @return true if readCompacted is enabled
     */
    bool isReadCompacted() const;

    /**
     * If enabled, the consumer reads messages from the compacted topics rather than reading the full message
     * backlog of the topic. This means that if the topic has been compacted, the consumer only sees the
     * latest value for each key in the topic, up until the point in the topic message backlog that has been
     * compacted. Beyond that point, message is sent as normal.
     *
     * `readCompacted` can only be enabled subscriptions to persistent topics, which have a single active
     * consumer (for example, failure or exclusive subscriptions). Attempting to enable it on subscriptions to
     * a non-persistent topics or on a shared subscription leads to the subscription call failure.
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     */
    void setReadCompacted(bool compacted);

    /**
     * Set the time duration in minutes, for which the PatternMultiTopicsConsumer will do a pattern auto
     * discovery.
     * The default value is 60 seconds. less than 0 will disable auto discovery.
     *
     * @param periodInSeconds       period in seconds to do an auto discovery
     */
    void setPatternAutoDiscoveryPeriod(int periodInSeconds);

    /**
     * @return the time duration for the PatternMultiTopicsConsumer performs a pattern auto discovery
     */
    int getPatternAutoDiscoveryPeriod() const;

    /**
     * Determines which topics this consumer should be subscribed to - Persistent, Non-Persistent, or
     * AllTopics. Only used with pattern subscriptions.
     *
     * @param regexSubscriptionMode The default value is `PersistentOnly`.
     */
    ConsumerConfiguration& setRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode);

    /**
     * @return the regex subscription mode for the pattern consumer.
     */
    RegexSubscriptionMode getRegexSubscriptionMode() const;

    /**
     * The default value is `InitialPositionLatest`.
     *
     * @param subscriptionInitialPosition the initial position at which to set
     * the cursor when subscribing to the topic for the first time
     */
    void setSubscriptionInitialPosition(InitialPosition subscriptionInitialPosition);

    /**
     * @return the configured `InitialPosition` for the consumer
     */
    InitialPosition getSubscriptionInitialPosition() const;

    /**
     * Set batch receive policy.
     *
     * @param batchReceivePolicy the default is
     * {maxNumMessage: -1, maxNumBytes: 10 * 1024 * 1024, timeoutMs: 100}
     */
    void setBatchReceivePolicy(const BatchReceivePolicy& batchReceivePolicy);

    /**
     * Get batch receive policy.
     *
     * @return batch receive policy
     */
    const BatchReceivePolicy& getBatchReceivePolicy() const;

    /**
     * Set dead letter policy for consumer
     *
     * By default, some messages are redelivered many times, even to the extent that they can never be
     * stopped. By using the dead letter mechanism, messages have the max redelivery count, when they
     * exceeding the maximum number of redeliveries. Messages are sent to dead letter topics and acknowledged
     * automatically.
     *
     * You can enable the dead letter mechanism by setting the dead letter policy.
     * Example:
     *
     * <pre>
     * * DeadLetterPolicy dlqPolicy = DeadLetterPolicyBuilder()
     *                       .maxRedeliverCount(10)
     *                       .build();
     * </pre>
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ.
     * To set a custom dead letter topic name
     * <pre>
     * DeadLetterPolicy dlqPolicy = DeadLetterPolicyBuilder()
     *                       .deadLetterTopic("dlq-topic")
     *                       .maxRedeliverCount(10)
     *                       .initialSubscriptionName("init-sub-name")
     *                       .build();
     * </pre>
     * @param deadLetterPolicy Default value is empty
     */
    void setDeadLetterPolicy(const DeadLetterPolicy& deadLetterPolicy);

    /**
     * Get dead letter policy.
     *
     * @return dead letter policy
     */
    const DeadLetterPolicy& getDeadLetterPolicy() const;

    /**
     * Set whether the subscription status should be replicated.
     * The default value is `false`.
     *
     * @param replicateSubscriptionState whether the subscription status should be replicated
     */
    void setReplicateSubscriptionStateEnabled(bool enabled);

    /**
     * @return whether the subscription status should be replicated
     */
    bool isReplicateSubscriptionStateEnabled() const;

    /**
     * Check whether the message has a specific property attached.
     *
     * @param name the name of the property to check
     * @return true if the message has the specified property
     * @return false if the property is not defined
     */
    bool hasProperty(const std::string& name) const;

    /**
     * Get the value of a specific property
     *
     * @param name the name of the property
     * @return the value of the property or null if the property was not defined
     */
    const std::string& getProperty(const std::string& name) const;

    /**
     * Get all the properties attached to this producer.
     */
    std::map<std::string, std::string>& getProperties() const;

    /**
     * Sets a new property on a message.
     * @param name   the name of the property
     * @param value  the associated value
     */
    ConsumerConfiguration& setProperty(const std::string& name, const std::string& value);

    /**
     * Add all the properties in the provided map
     */
    ConsumerConfiguration& setProperties(const std::map<std::string, std::string>& properties);

    /**
     * Get all the subscription properties attached to this subscription.
     */
    std::map<std::string, std::string>& getSubscriptionProperties() const;

    /**
     * Sets a new subscription properties for this subscription.
     * Notice: SubscriptionProperties are immutable, and consumers under the same subscription will fail to
     * create a subscription if they use different properties.
     *
     * @param subscriptionProperties all the subscription properties in the provided map
     */
    ConsumerConfiguration& setSubscriptionProperties(
        const std::map<std::string, std::string>& subscriptionProperties);

    /**
     * Set the Priority Level for consumer (0 is the default value and means the highest priority).
     *
     * @param priorityLevel the priority of this consumer
     * @return the ConsumerConfiguration instance
     */
    ConsumerConfiguration& setPriorityLevel(int priorityLevel);

    /**
     * @return the configured priority for the consumer
     */
    int getPriorityLevel() const;

    /**
     * Consumer buffers chunk messages into memory until it receives all the chunks of the original message.
     * While consuming chunk-messages, chunks from same message might not be contiguous in the stream and they
     * might be mixed with other messages' chunks. so, consumer has to maintain multiple buffers to manage
     * chunks coming from different messages. This mainly happens when multiple publishers are publishing
     * messages on the topic concurrently or publisher failed to publish all chunks of the messages.
     *
     * eg: M1-C1, M2-C1, M1-C2, M2-C2
     * Here, Messages M1-C1 and M1-C2 belong to original message M1, M2-C1 and M2-C2 belong to M2 message.
     *
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it
     * can be guarded by providing this maxPendingChunkedMessage threshold. Once, consumer reaches this
     * threshold, it drops the outstanding unchunked-messages by silently acking or asking broker to redeliver
     * later by marking it unacked. See setAutoAckOldestChunkedMessageOnQueueFull.
     *
     * If it's zero, the pending chunked messages will not be limited.
     *
     * Default: 10
     *
     * @param maxPendingChunkedMessage the number of max pending chunked messages
     */
    ConsumerConfiguration& setMaxPendingChunkedMessage(size_t maxPendingChunkedMessage);

    /**
     * The associated getter of setMaxPendingChunkedMessage
     */
    size_t getMaxPendingChunkedMessage() const;

    /**
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it
     * can be guarded by providing the maxPendingChunkedMessage threshold. See setMaxPendingChunkedMessage.
     * Once, consumer reaches this threshold, it drops the outstanding unchunked-messages by silently acking
     * if autoAckOldestChunkedMessageOnQueueFull is true else it marks them for redelivery.
     *
     * Default: false
     *
     * @param autoAckOldestChunkedMessageOnQueueFull whether to ack the discarded chunked message
     */
    ConsumerConfiguration& setAutoAckOldestChunkedMessageOnQueueFull(
        bool autoAckOldestChunkedMessageOnQueueFull);

    /**
     * The associated getter of setAutoAckOldestChunkedMessageOnQueueFull
     */
    bool isAutoAckOldestChunkedMessageOnQueueFull() const;

    /**
     * If producer fails to publish all the chunks of a message then consumer can expire incomplete chunks if
     * consumer won't be able to receive all chunks in expire times. Use value 0 to disable this feature.
     *
     * Default: 60000, which means 1 minutes
     *
     * @param expireTimeOfIncompleteChunkedMessageMs expire time in milliseconds
     * @return Consumer Configuration
     */
    ConsumerConfiguration& setExpireTimeOfIncompleteChunkedMessageMs(
        long expireTimeOfIncompleteChunkedMessageMs);

    /**
     *
     * Get the expire time of incomplete chunked message in milliseconds
     *
     * @return the expire time of incomplete chunked message in milliseconds
     */
    long getExpireTimeOfIncompleteChunkedMessageMs() const;

    /**
     * Set the consumer to include the given position of any reset operation like Consumer::seek.
     *
     * Default: false
     *
     * @param startMessageIdInclusive whether to include the reset position
     */
    ConsumerConfiguration& setStartMessageIdInclusive(bool startMessageIdInclusive);

    /**
     * The associated getter of setStartMessageIdInclusive
     */
    bool isStartMessageIdInclusive() const;

    /**
     * Enable the batch index acknowledgment.
     *
     * It should be noted that this option can only work when the broker side also enables the batch index
     * acknowledgment. See the `acknowledgmentAtBatchIndexLevelEnabled` config in `broker.conf`.
     *
     * Default: false
     *
     * @param enabled whether to enable the batch index acknowledgment
     */
    ConsumerConfiguration& setBatchIndexAckEnabled(bool enabled);

    /**
     * The associated getter of setBatchingEnabled
     */
    bool isBatchIndexAckEnabled() const;

    /**
     * Intercept the consumer
     *
     * @param interceptors the list of interceptors to intercept the consumer
     * @return Consumer Configuration
     */
    ConsumerConfiguration& intercept(const std::vector<ConsumerInterceptorPtr>& interceptors);

    const std::vector<ConsumerInterceptorPtr>& getInterceptors() const;

    /**
     * Whether to receive the ACK receipt from broker.
     *
     * By default, when Consumer::acknowledge is called, it won't wait until the corresponding response from
     * broker. After it's enabled, the `acknowledge` method will return a Result that indicates if the
     * acknowledgment succeeded.
     *
     * Default: false
     */
    ConsumerConfiguration& setAckReceiptEnabled(bool ackReceiptEnabled);

    /**
     * The associated getter of setAckReceiptEnabled.
     */
    bool isAckReceiptEnabled() const;

    /**
     * Starts the consumer in a paused state.
     *
     * When enabled, the consumer does not immediately fetch messages when the consumer is created.
     * Instead, the consumer waits to fetch messages until Consumer::resumeMessageListener is called.
     *
     * Default: false
     */
    ConsumerConfiguration& setStartPaused(bool startPaused);

    /**
     * The associated getter of setStartPaused.
     */
    bool isStartPaused() const;

    friend class PulsarWrapper;
    friend class PulsarFriend;

   private:
    std::shared_ptr<ConsumerConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_CONSUMERCONFIGURATION_H_ */

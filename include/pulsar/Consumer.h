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
#ifndef CONSUMER_HPP_
#define CONSUMER_HPP_

#include <pulsar/BrokerConsumerStats.h>
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/TypedMessage.h>
#include <pulsar/defines.h>

#include <iostream>

namespace pulsar {
class PulsarWrapper;
class ConsumerImplBase;
class PulsarFriend;
typedef std::shared_ptr<ConsumerImplBase> ConsumerImplBasePtr;
/**
 *
 */
class PULSAR_PUBLIC Consumer {
   public:
    /**
     * Construct an uninitialized consumer object
     */
    Consumer();
    virtual ~Consumer() = default;

    /**
     * @return the topic this consumer is subscribed to
     */
    const std::string& getTopic() const;

    /**
     * @return the subscription name
     */
    const std::string& getSubscriptionName() const;

    /**
     * @return the consumer name
     */
    const std::string& getConsumerName() const;

    /**
     * Unsubscribe the current consumer from the topic.
     *
     * This method will block until the operation is completed. Once the consumer is
     * unsubscribed, no more messages will be received and subsequent new messages
     * will not be retained for this consumer.
     *
     * This consumer object cannot be reused.
     *
     * @see asyncUnsubscribe
     * @return Result::ResultOk if the unsubscribe operation completed successfully
     * @return Result::ResultError if the unsubscribe operation failed
     */
    Result unsubscribe();

    /**
     * Asynchronously unsubscribe the current consumer from the topic.
     *
     * This method will block until the operation is completed. Once the consumer is
     * unsubscribed, no more messages will be received and subsequent new messages
     * will not be retained for this consumer.
     *
     * This consumer object cannot be reused.
     *
     * @param callback the callback to get notified when the operation is complete
     */
    void unsubscribeAsync(const ResultCallback& callback);

    /**
     * Receive a single message.
     *
     * If a message is not immediately available, this method will block until a new
     * message is available.
     *
     * @param msg a non-const reference where the received message will be copied
     * @return ResultOk when a message is received
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result receive(Message& msg);

    template <typename T>
    Result receive(TypedMessage<T>& msg, typename TypedMessage<T>::Decoder decoder) {
        Message rawMsg;
        auto result = receive(rawMsg);
        msg = TypedMessage<T>{rawMsg, decoder};
        return result;
    }

    /**
     *
     * @param msg a non-const reference where the received message will be copied
     * @param timeoutMs the receive timeout in milliseconds
     * @return ResultOk if a message was received
     * @return ResultTimeout if the receive timeout was triggered
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result receive(Message& msg, int timeoutMs);

    template <typename T>
    Result receive(TypedMessage<T>& msg, int timeoutMs, typename TypedMessage<T>::Decoder decoder) {
        Message rawMsg;
        auto result = receive(rawMsg, timeoutMs);
        msg = TypedMessage<T>{rawMsg, decoder};
        return result;
    }

    /**
     * Receive a single message
     * <p>
     * Retrieves a message when it will be available and completes callback with received message.
     * </p>
     * <p>
     * receiveAsync() should be called subsequently once callback gets completed with received message.
     * Else it creates <i> backlog of receive requests </i> in the application.
     * </p>
     * @param ReceiveCallback will be completed when message is available
     */
    void receiveAsync(const ReceiveCallback& callback);

    template <typename T>
    void receiveAsync(const std::function<void(Result result, const TypedMessage<T>&)>& callback,
                      typename TypedMessage<T>::Decoder decoder) {
        receiveAsync([callback, decoder](Result result, const Message& msg) {
            callback(result, TypedMessage<T>{msg, decoder});
        });
    }

    /**
     * Batch receiving messages.
     *
     * <p>This calls blocks until has enough messages or wait timeout, more details to see {@link
     * BatchReceivePolicy}.
     *
     * @param msgs a non-const reference where the received messages will be copied
     * @return ResultOk when a message is received
     * @return ResultInvalidConfiguration if a message listener had been set in the configuration
     */
    Result batchReceive(Messages& msgs);

    /**
     * Async Batch receiving messages.
     * <p>
     * Retrieves a message when it will be available and completes callback with received message.
     * </p>
     * <p>
     * batchReceiveAsync() should be called subsequently once callback gets completed with received message.
     * Else it creates <i> backlog of receive requests </i> in the application.
     * </p>
     * @param BatchReceiveCallback will be completed when messages are available.
     */
    void batchReceiveAsync(const BatchReceiveCallback& callback);

    /**
     * Acknowledge the reception of a single message.
     *
     * This method will block until an acknowledgement is sent to the broker. After
     * that, the message will not be re-delivered to this consumer.
     *
     * @see asyncAcknowledge
     * @param message the message to acknowledge
     * @return ResultOk if the message was successfully acknowledged
     * @return ResultError if there was a failure
     */
    Result acknowledge(const Message& message);

    /**
     * Acknowledge the reception of a single message.
     *
     * This method is blocked until an acknowledgement is sent to the broker. After that, the message is not
     * re-delivered to the consumer.
     *
     * @see asyncAcknowledge
     * @param messageId the MessageId to acknowledge
     * @return ResultOk if the messageId is successfully acknowledged
     */
    Result acknowledge(const MessageId& messageId);

    /**
     * Acknowledge the consumption of a list of message.
     * @param messageIdList
     */
    Result acknowledge(const MessageIdList& messageIdList);

    /**
     * Asynchronously acknowledge the reception of a single message.
     *
     * This method will initiate the operation and return immediately. The provided callback
     * will be triggered when the operation is complete.
     *
     * @param message the message to acknowledge
     * @param callback callback that will be triggered when the message has been acknowledged
     */
    void acknowledgeAsync(const Message& message, const ResultCallback& callback);

    /**
     * Asynchronously acknowledge the reception of a single message.
     *
     * This method initiates the operation and returns the result immediately. The provided callback
     * is triggered when the operation is completed.
     *
     * @param messageId the messageId to acknowledge
     * @param callback the callback that is triggered when the message has been acknowledged or not
     */
    void acknowledgeAsync(const MessageId& messageId, const ResultCallback& callback);

    /**
     * Asynchronously acknowledge the consumption of a list of message.
     * @param messageIdList
     * @param callback the callback that is triggered when the message has been acknowledged or not
     * @return
     */
    void acknowledgeAsync(const MessageIdList& messageIdList, const ResultCallback& callback);

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including)
     * the provided message.
     *
     * This method will block until an acknowledgement is sent to the broker. After
     * that, the messages will not be re-delivered to this consumer.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * It's equivalent to calling asyncAcknowledgeCumulative(const Message&, const ResultCallback&) and
     * waiting for the callback to be triggered.
     *
     * @param message the last message in the stream to acknowledge
     * @return ResultOk if the message was successfully acknowledged. All previously delivered messages for
     * this topic are also acknowledged.
     * @return ResultError if there was a failure
     */
    Result acknowledgeCumulative(const Message& message);

    /**
     * Acknowledge the reception of all the messages in the stream up to (and including)
     * the provided message.
     *
     * This method is blocked until an acknowledgement is sent to the broker. After
     * that, the message is not re-delivered to this consumer.
     *
     * Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
     *
     * It is equivalent to calling the asyncAcknowledgeCumulative(const Message&, const ResultCallback&)
     * method and waiting for the callback to be triggered.
     *
     * @param messageId the last messageId in the stream to acknowledge
     * @return ResultOk if the message is successfully acknowledged. All previously delivered messages for
     * this topic are also acknowledged.
     */
    Result acknowledgeCumulative(const MessageId& messageId);

    /**
     * Asynchronously acknowledge the reception of all the messages in the stream up to (and
     * including) the provided message.
     *
     * This method will initiate the operation and return immediately. The provided callback
     * will be triggered when the operation is complete.
     *
     * @param message the message to acknowledge
     * @param callback callback that will be triggered when the message has been acknowledged
     */
    void acknowledgeCumulativeAsync(const Message& message, const ResultCallback& callback);

    /**
     * Asynchronously acknowledge the reception of all the messages in the stream up to (and
     * including) the provided message.
     *
     * This method initiates the operation and returns the result immediately. The provided callback
     * is triggered when the operation is completed.
     *
     * @param messageId the messageId to acknowledge
     * @param callback the callback that is triggered when the message has been acknowledged or not
     */
    void acknowledgeCumulativeAsync(const MessageId& messageId, const ResultCallback& callback);

    /**
     * Acknowledge the failure to process a single message.
     * <p>
     * When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.
     * <p>
     * This call is not blocking.
     *
     * <p>
     * Example of usage:
     * <pre><code>
     * while (true) {
     *     Message msg;
     *     consumer.receive(msg);
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msg);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.negativeAcknowledge(msg);
     *     }
     * }
     * </code></pre>
     *
     * @param message
     *            The {@code Message} to be acknowledged
     */
    void negativeAcknowledge(const Message& message);

    /**
     * Acknowledge the failure to process a single message.
     * <p>
     * When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.
     * <p>
     * This call is not blocking.
     *
     * <p>
     * Example of usage:
     * <pre><code>
     * while (true) {
     *     Message msg;
     *     consumer.receive(msg);
     *
     *     try {
     *          // Process message...
     *
     *          consumer.acknowledge(msg);
     *     } catch (Throwable t) {
     *          log.warn("Failed to process message");
     *          consumer.negativeAcknowledge(msg);
     *     }
     * }
     * </code></pre>
     *
     * @param messageId
     *            The {@code MessageId} to be acknowledged
     */
    void negativeAcknowledge(const MessageId& messageId);

    /**
     * Close the consumer and stop the broker to push more messages
     */
    Result close();

    /**
     * Asynchronously close the consumer and stop the broker to push more messages
     *
     */
    void closeAsync(const ResultCallback& callback);

    /**
     * Pause receiving messages via the messageListener, till resumeMessageListener() is called.
     */
    Result pauseMessageListener();

    /**
     * Resume receiving the messages via the messageListener.
     * Asynchronously receive all the messages enqueued from time pauseMessageListener() was called.
     */
    Result resumeMessageListener();

    /**
     * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is
     * not
     * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed
     * across all
     * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the
     * connection
     * breaks, the messages are redelivered after reconnect.
     */
    void redeliverUnacknowledgedMessages();

    /**
     * Gets Consumer Stats from broker.
     * The stats are cached for 30 seconds, if a call is made before the stats returned by the previous call
     * expires
     * then cached data will be returned. BrokerConsumerStats::isValid() function can be used to check if the
     * stats are
     * still valid.
     *
     * @param brokerConsumerStats - if the function returns ResultOk, this object will contain consumer stats
     *
     * @note This is a blocking call with timeout of thirty seconds.
     */
    Result getBrokerConsumerStats(BrokerConsumerStats& brokerConsumerStats);

    /**
     * Asynchronous call to gets Consumer Stats from broker.
     * The stats are cached for 30 seconds, if a call is made before the stats returned by the previous call
     * expires
     * then cached data will be returned. BrokerConsumerStats::isValid() function can be used to check if the
     * stats are
     * still valid.
     *
     * @param callback - callback function to get the brokerConsumerStats,
     *                   if result is ResultOk then the brokerConsumerStats will be populated
     */
    void getBrokerConsumerStatsAsync(const BrokerConsumerStatsCallback& callback);

    /**
     * Reset the subscription associated with this consumer to a specific message id.
     * The message id can either be a specific message or represent the first or last messages in the topic.
     *
     * Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
     * seek() on the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     */
    Result seek(const MessageId& messageId);

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     */
    Result seek(uint64_t timestamp);

    /**
     * Asynchronously reset the subscription associated with this consumer to a specific message id.
     * The message id can either be a specific message or represent the first or last messages in the topic.
     *
     * Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
     * seek() on the individual partitions.
     *
     * @param messageId
     *            the message id where to reposition the subscription
     */
    virtual void seekAsync(const MessageId& messageId, const ResultCallback& callback);

    /**
     * Asynchronously reset the subscription associated with this consumer to a specific message publish time.
     *
     * @param timestamp
     *            the message publish time where to reposition the subscription
     */
    virtual void seekAsync(uint64_t timestamp, const ResultCallback& callback);

    /**
     * @return Whether the consumer is currently connected to the broker
     */
    bool isConnected() const;

    /**
     * Asynchronously get an ID of the last available message or a message ID with -1 as an entryId if the
     * topic is empty.
     */
    void getLastMessageIdAsync(const GetLastMessageIdCallback& callback);

    /**
     * Get an ID of the last available message or a message ID with -1 as an entryId if the topic is empty.
     */
    Result getLastMessageId(MessageId& messageId);

   private:
    ConsumerImplBasePtr impl_;
    explicit Consumer(ConsumerImplBasePtr);

    friend class PulsarFriend;
    friend class PulsarWrapper;
    friend class MultiTopicsConsumerImpl;
    friend class ConsumerImpl;
    friend class ClientImpl;
    friend class ConsumerTest;
};
}  // namespace pulsar

#endif /* CONSUMER_HPP_ */

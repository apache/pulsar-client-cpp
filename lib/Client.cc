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
#include <pulsar/Client.h>

#include <iostream>
#include <memory>
#include <utility>

#include "ClientImpl.h"
#include "Int64SerDes.h"
#include "LogUtils.h"
#include "LookupService.h"
#include "TopicName.h"
#include "Utils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

Client::Client(const std::shared_ptr<ClientImpl>& impl) : impl_(impl) {}

Client::Client(const std::string& serviceUrl)
    : impl_(std::make_shared<ClientImpl>(serviceUrl, ClientConfiguration())) {}

Client::Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration)
    : impl_(std::make_shared<ClientImpl>(serviceUrl, clientConfiguration)) {}

Result Client::createProducer(const std::string& topic, Producer& producer) {
    return createProducer(topic, ProducerConfiguration(), producer);
}

Result Client::createProducer(const std::string& topic, const ProducerConfiguration& conf,
                              Producer& producer) {
    Promise<Result, Producer> promise;
    createProducerAsync(topic, conf, WaitForCallbackValue<Producer>(promise));
    Future<Result, Producer> future = promise.getFuture();

    return future.get(producer);
}

void Client::createProducerAsync(const std::string& topic, const CreateProducerCallback& callback) {
    createProducerAsync(topic, ProducerConfiguration(), callback);
}

void Client::createProducerAsync(const std::string& topic, const ProducerConfiguration& conf,
                                 const CreateProducerCallback& callback) {
    impl_->createProducerAsync(topic, conf, callback);
}

Result Client::subscribe(const std::string& topic, const std::string& subscriptionName, Consumer& consumer) {
    return subscribe(topic, subscriptionName, ConsumerConfiguration(), consumer);
}

Result Client::subscribe(const std::string& topic, const std::string& subscriptionName,
                         const ConsumerConfiguration& conf, Consumer& consumer) {
    Promise<Result, Consumer> promise;
    subscribeAsync(topic, subscriptionName, conf, WaitForCallbackValue<Consumer>(promise));
    Future<Result, Consumer> future = promise.getFuture();

    return future.get(consumer);
}

void Client::subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                            const SubscribeCallback& callback) {
    subscribeAsync(topic, subscriptionName, ConsumerConfiguration(), callback);
}

void Client::subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                            const ConsumerConfiguration& conf, const SubscribeCallback& callback) {
    LOG_INFO("Subscribing on Topic :" << topic);
    impl_->subscribeAsync(topic, subscriptionName, conf, callback);
}

Result Client::subscribe(const std::vector<std::string>& topics, const std::string& subscriptionName,
                         Consumer& consumer) {
    return subscribe(topics, subscriptionName, ConsumerConfiguration(), consumer);
}

Result Client::subscribe(const std::vector<std::string>& topics, const std::string& subscriptionName,
                         const ConsumerConfiguration& conf, Consumer& consumer) {
    Promise<Result, Consumer> promise;
    subscribeAsync(topics, subscriptionName, conf, WaitForCallbackValue<Consumer>(promise));
    Future<Result, Consumer> future = promise.getFuture();

    return future.get(consumer);
}

void Client::subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                            const SubscribeCallback& callback) {
    subscribeAsync(topics, subscriptionName, ConsumerConfiguration(), callback);
}

void Client::subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                            const ConsumerConfiguration& conf, const SubscribeCallback& callback) {
    impl_->subscribeAsync(topics, subscriptionName, conf, callback);
}

Result Client::subscribeWithRegex(const std::string& regexPattern, const std::string& subscriptionName,
                                  Consumer& consumer) {
    return subscribeWithRegex(regexPattern, subscriptionName, ConsumerConfiguration(), consumer);
}

Result Client::subscribeWithRegex(const std::string& regexPattern, const std::string& subscriptionName,
                                  const ConsumerConfiguration& conf, Consumer& consumer) {
    Promise<Result, Consumer> promise;
    subscribeWithRegexAsync(regexPattern, subscriptionName, conf, WaitForCallbackValue<Consumer>(promise));
    Future<Result, Consumer> future = promise.getFuture();

    return future.get(consumer);
}

void Client::subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                     const SubscribeCallback& callback) {
    subscribeWithRegexAsync(regexPattern, subscriptionName, ConsumerConfiguration(), callback);
}

void Client::subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                     const ConsumerConfiguration& conf, const SubscribeCallback& callback) {
    impl_->subscribeWithRegexAsync(regexPattern, subscriptionName, conf, callback);
}

Result Client::createReader(const std::string& topic, const MessageId& startMessageId,
                            const ReaderConfiguration& conf, Reader& reader) {
    Promise<Result, Reader> promise;
    createReaderAsync(topic, startMessageId, conf, WaitForCallbackValue<Reader>(promise));
    Future<Result, Reader> future = promise.getFuture();

    return future.get(reader);
}

void Client::createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                               const ReaderConfiguration& conf, const ReaderCallback& callback) {
    impl_->createReaderAsync(topic, startMessageId, conf, callback);
}

Result Client::createTableView(const std::string& topic, const TableViewConfiguration& conf,
                               TableView& tableView) {
    Promise<Result, TableView> promise;
    createTableViewAsync(topic, conf, WaitForCallbackValue<TableView>(promise));
    Future<Result, TableView> future = promise.getFuture();

    return future.get(tableView);
}

void Client::createTableViewAsync(const std::string& topic, const TableViewConfiguration& conf,
                                  const TableViewCallback& callback) {
    impl_->createTableViewAsync(topic, conf, callback);
}

Result Client::getPartitionsForTopic(const std::string& topic, std::vector<std::string>& partitions) {
    Promise<Result, std::vector<std::string> > promise;
    getPartitionsForTopicAsync(topic, WaitForCallbackValue<std::vector<std::string> >(promise));
    Future<Result, std::vector<std::string> > future = promise.getFuture();

    return future.get(partitions);
}

void Client::getPartitionsForTopicAsync(const std::string& topic, const GetPartitionsCallback& callback) {
    impl_->getPartitionsForTopicAsync(topic, callback);
}

Result Client::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Client::closeAsync(const CloseCallback& callback) { impl_->closeAsync(callback); }

void Client::shutdown() { impl_->shutdown(); }

uint64_t Client::getNumberOfProducers() { return impl_->getNumberOfProducers(); }
uint64_t Client::getNumberOfConsumers() { return impl_->getNumberOfConsumers(); }

void Client::getSchemaInfoAsync(const std::string& topic, int64_t version,
                                std::function<void(Result, const SchemaInfo&)> callback) {
    impl_->getLookup()
        ->getSchema(TopicName::get(topic), (version >= 0) ? toBigEndianBytes(version) : "")
        .addListener(std::move(callback));
}
}  // namespace pulsar

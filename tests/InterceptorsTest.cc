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
#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <pulsar/ConsumerInterceptor.h>
#include <pulsar/ProducerInterceptor.h>

#include <utility>

#include "HttpHelper.h"
#include "Latch.h"
#include "lib/LogUtils.h"

DECLARE_LOG_OBJECT()

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

using namespace pulsar;

class ProducerTestInterceptor : public ProducerInterceptor {
   public:
    ProducerTestInterceptor(Latch& latch, Latch& closeLatch, std::string key)
        : latch_(latch), closeLatch_(closeLatch), key_(std::move(key)) {}

    Message beforeSend(const Producer& producer, const Message& message) override {
        return MessageBuilder()
            .setProperties(message.getProperties())
            .setProperty(key_, "set")
            .setContent(message.getDataAsString())
            .build();
    }

    void onSendAcknowledgement(const Producer& producer, Result result, const Message& message,
                               const MessageId& messageID) override {
        ASSERT_EQ(result, ResultOk);
        auto properties = message.getProperties();
        ASSERT_TRUE(properties.find("key1") != properties.end() && properties["key1"] == "set");
        ASSERT_TRUE(properties.find("key2") != properties.end() && properties["key2"] == "set");
        latch_.countdown();
    }

    void close() override { closeLatch_.countdown(); }

   private:
    Latch latch_;
    Latch closeLatch_;
    std::string key_;
};

class ProducerExceptionInterceptor : public ProducerInterceptor {
   public:
    explicit ProducerExceptionInterceptor(Latch& latch) : latch_(latch) {}

    Message beforeSend(const Producer& producer, const Message& message) override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

    void onSendAcknowledgement(const Producer& producer, Result result, const Message& message,
                               const MessageId& messageID) override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

    void close() override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

   private:
    Latch latch_;
};

class ProducerPartitionsChangeInterceptor : public ProducerInterceptor {
   public:
    explicit ProducerPartitionsChangeInterceptor(Latch& latch) : latch_(latch) {}

    Message beforeSend(const Producer& producer, const Message& message) override { return message; }

    void onSendAcknowledgement(const Producer& producer, Result result, const Message& message,
                               const MessageId& messageID) override {}

    void onPartitionsChange(const std::string& topicName, int partitions) override {
        ASSERT_EQ(partitions, 3);
        latch_.countdown();
    }

   private:
    Latch latch_;
};

void createPartitionedTopic(std::string topic) {
    std::string topicOperateUrl = adminUrl + "admin/v2/persistent/public/default/" + topic + "/partitions";

    int res = makePutRequest(topicOperateUrl, "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
}

class ProducerInterceptorsTest : public ::testing::TestWithParam<bool> {};

TEST_P(ProducerInterceptorsTest, testProducerInterceptor) {
    const std::string topic = "InterceptorsTest-testProducerInterceptor-" + std::to_string(time(nullptr));

    if (GetParam()) {
        createPartitionedTopic(topic);
    }

    Latch latch(2);
    Latch closeLatch(2);

    Client client(serviceUrl);
    ProducerConfiguration conf;
    conf.intercept({std::make_shared<ProducerTestInterceptor>(latch, closeLatch, "key1"),
                    std::make_shared<ProducerTestInterceptor>(latch, closeLatch, "key2")});
    Producer producer;
    client.createProducer(topic, conf, producer);

    Message msg = MessageBuilder().setContent("content").build();
    Result result = producer.send(msg);
    ASSERT_EQ(result, ResultOk);

    ASSERT_TRUE(latch.wait(std::chrono::seconds(5)));

    producer.close();
    ASSERT_TRUE(closeLatch.wait(std::chrono::seconds(5)));
    client.close();
}

TEST_P(ProducerInterceptorsTest, testProducerInterceptorWithException) {
    const std::string topic =
        "InterceptorsTest-testProducerInterceptorWithException-" + std::to_string(time(nullptr));

    if (GetParam()) {
        createPartitionedTopic(topic);
    }

    Latch latch(3);

    Client client(serviceUrl);
    ProducerConfiguration conf;
    conf.intercept({std::make_shared<ProducerExceptionInterceptor>(latch)});
    Producer producer;
    client.createProducer(topic, conf, producer);

    Message msg = MessageBuilder().setContent("content").build();
    Result result = producer.send(msg);
    ASSERT_EQ(result, ResultOk);

    producer.close();
    ASSERT_TRUE(latch.wait(std::chrono::seconds(5)));
    client.close();
}

TEST(ProducerInterceptorsTest, testProducerInterceptorOnPartitionsChange) {
    const std::string topic = "public/default/InterceptorsTest-testProducerInterceptorOnPartitionsChange-" +
                              std::to_string(time(nullptr));
    std::string topicOperateUrl = adminUrl + "admin/v2/persistent/" + topic + "/partitions";

    int res = makePutRequest(topicOperateUrl, "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Latch latch(1);

    ClientConfiguration clientConf;
    clientConf.setPartititionsUpdateInterval(1);
    Client client(serviceUrl, clientConf);
    ProducerConfiguration conf;
    conf.intercept({std::make_shared<ProducerPartitionsChangeInterceptor>(latch)});
    Producer producer;
    client.createProducer(topic, conf, producer);

    res = makePostRequest(topicOperateUrl, "3");  // update partitions to 3
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    ASSERT_TRUE(latch.wait(std::chrono::seconds(5)));

    producer.close();
    client.close();
}

class ConsumerExceptionInterceptor : public ConsumerInterceptor {
   public:
    explicit ConsumerExceptionInterceptor(Latch& latch) : latch_(latch) {}

    void close() override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

    Message beforeConsume(const Consumer& consumer, const Message& message) override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

    void onAcknowledge(const Consumer& consumer, Result result, const MessageId& messageID) override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

    void onAcknowledgeCumulative(const Consumer& consumer, Result result,
                                 const MessageId& messageID) override {
        latch_.countdown();
        throw std::runtime_error("expected exception");
    }

   private:
    Latch latch_;
};

enum TopicType
{
    Single,
    Partitioned,
    Pattern
};

class ConsumerTestInterceptor : public ConsumerInterceptor {
   public:
    ConsumerTestInterceptor(Latch& latch, std::string key) : latch_(latch), key_(std::move(key)) {}

    void close() override { latch_.countdown(); }

    Message beforeConsume(const Consumer& consumer, const Message& message) override {
        latch_.countdown();
        LOG_INFO("Received msg from: " << consumer.getTopic());
        return MessageBuilder()
            .setProperties(message.getProperties())
            .setProperty(key_, "set")
            .setContent(message.getDataAsString())
            .build();
    }

    void onAcknowledge(const Consumer& consumer, Result result, const MessageId& messageID) override {
        LOG_INFO("Ack msg from: " << consumer.getTopic());
        ASSERT_EQ(result, ResultOk);
        latch_.countdown();
    }

    void onAcknowledgeCumulative(const Consumer& consumer, Result result,
                                 const MessageId& messageID) override {
        LOG_INFO("Ack cumulative msg from: " << consumer.getTopic());
        ASSERT_EQ(result, ResultOk);
        latch_.countdown();
    }

   private:
    Latch latch_;
    std::string key_;
};

class ConsumerInterceptorsTest : public ::testing::TestWithParam<std::tuple<TopicType, int>> {
   public:
    void SetUp() override {
        topic_ = "persistent://public/default/InterceptorsTest-ConsumerInterceptors-" +
                 std::to_string(time(nullptr));

        switch (std::get<0>(GetParam())) {
            case Partitioned:
                this->createPartitionedTopic(topic_);
            case Single:
                client_.createProducer(topic_, producer1_);
                client_.createProducer(topic_, producer2_);
                break;
            case Pattern:
                client_.createProducer(topic_ + "-p1", producer1_);
                client_.createProducer(topic_ + "-p2", producer2_);
                topic_ += "-.*";
                break;
        }

        consumerConf_.setReceiverQueueSize(std::get<1>(GetParam()));
    }

    void createPartitionedTopic(std::string topic) {
        std::string topicOperateUrl = adminUrl + "admin/v2/persistent/" +
                                      topic.substr(std::string("persistent://").length()) + "/partitions";

        int res = makePutRequest(topicOperateUrl, "2");
        ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    }

    void TearDown() override {
        producer1_.close();
        producer2_.close();
        client_.close();
    }

   protected:
    Client client_{serviceUrl};
    std::string topic_;
    ConsumerConfiguration consumerConf_;
    Producer producer1_;
    Producer producer2_;
};

TEST_P(ConsumerInterceptorsTest, testConsumerInterceptor) {
    Latch latch(
        10);  // (2 beforeConsume + 1 onAcknowledge + 1 onAcknowledgeCumulative + 1 close) * 2 interceptors

    Consumer consumer;
    consumerConf_.intercept({std::make_shared<ConsumerTestInterceptor>(latch, "key1"),
                             std::make_shared<ConsumerTestInterceptor>(latch, "key2")});
    Result result;

    if (std::get<0>(GetParam()) == Pattern) {
        result = client_.subscribeWithRegex(topic_, "sub", consumerConf_, consumer);
    } else {
        result = client_.subscribe(topic_, "sub", consumerConf_, consumer);
    }

    ASSERT_EQ(result, ResultOk);

    Message msg = MessageBuilder().setContent("content").build();
    result = producer1_.send(msg);
    ASSERT_EQ(result, ResultOk);

    Message recvMsg;
    result = consumer.receive(recvMsg);
    ASSERT_EQ(result, ResultOk);
    auto properties = recvMsg.getProperties();
    ASSERT_TRUE(properties.find("key1") != properties.end() && properties["key1"] == "set");
    ASSERT_TRUE(properties.find("key2") != properties.end() && properties["key2"] == "set");
    consumer.acknowledge(recvMsg);

    msg = MessageBuilder().setContent("content").build();
    result = producer2_.send(msg);
    ASSERT_EQ(result, ResultOk);

    consumer.receive(recvMsg);
    consumer.acknowledgeCumulative(recvMsg);

    consumer.close();
    ASSERT_TRUE(latch.wait(std::chrono::seconds(5)));
}

TEST_P(ConsumerInterceptorsTest, testConsumerInterceptorWithExceptions) {
    Latch latch(5);  // 2 beforeConsume + 1 onAcknowledge + 1 onAcknowledgeCumulative + 1 close

    Consumer consumer;
    consumerConf_.intercept({std::make_shared<ConsumerExceptionInterceptor>(latch)});
    client_.subscribe(topic_, "sub", consumerConf_, consumer);

    Producer producer;
    client_.createProducer(topic_, producer);

    Message msg = MessageBuilder().setContent("content").build();
    Result result = producer.send(msg);
    ASSERT_EQ(result, ResultOk);

    Message recvMsg;
    consumer.receive(recvMsg);
    consumer.acknowledge(recvMsg);

    msg = MessageBuilder().setContent("content").build();
    result = producer.send(msg);
    ASSERT_EQ(result, ResultOk);

    consumer.receive(recvMsg);
    consumer.acknowledgeCumulative(recvMsg);

    producer.close();
    consumer.close();
    ASSERT_TRUE(latch.wait(std::chrono::seconds(5)));
}

INSTANTIATE_TEST_CASE_P(Pulsar, ProducerInterceptorsTest, ::testing::Values(true, false));
INSTANTIATE_TEST_CASE_P(Pulsar, ConsumerInterceptorsTest,
                        testing::Values(
                            // Can't use zero queue on multi topics consumer
                            std::make_tuple(Single, 0), std::make_tuple(Single, 1000),
                            std::make_tuple(Partitioned, 1000), std::make_tuple(Pattern, 1000)));

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
#include <pulsar/ConsumerCryptoFailureAction.h>

#include <stdexcept>

#include "PulsarApi.pb.h"
#include "lib/CompressionCodec.h"
#include "lib/MessageCrypto.h"
#include "lib/SharedBuffer.h"
#include "tests/PulsarFriend.h"

static std::string lookupUrl = "pulsar://localhost:6650";

using namespace pulsar;

static CryptoKeyReaderPtr getDefaultCryptoKeyReader() {
    return std::make_shared<DefaultCryptoKeyReader>(TEST_CONF_DIR "/public-key.client-rsa.pem",
                                                    TEST_CONF_DIR "/private-key.client-rsa.pem");
}

static std::vector<std::string> decryptValue(const Message& message) {
    if (!message.getEncryptionContext().has_value()) {
        return {message.getDataAsString()};
    }
    auto context = message.getEncryptionContext().value();
    if (!context->isDecryptionFailed()) {
        return {message.getDataAsString()};
    }

    MessageCrypto crypto{"test", false};
    auto msgImpl = PulsarFriend::getMessageImplPtr(message);
    SharedBuffer decryptedPayload;
    auto originalPayload =
        SharedBuffer::copy(static_cast<const char*>(message.getData()), message.getLength());
    if (!crypto.decrypt(*context, originalPayload, getDefaultCryptoKeyReader(), decryptedPayload)) {
        throw std::runtime_error("Decryption failed");
    }

    SharedBuffer uncompressedPayload;
    if (!CompressionCodecProvider::getCodec(context->compressionType())
             .decode(decryptedPayload, context->uncompressedMessageSize(), uncompressedPayload)) {
        throw std::runtime_error("Decompression failed");
    }

    std::vector<std::string> values;
    if (auto batchSize = message.getEncryptionContext().value()->batchSize(); batchSize > 0) {
        for (decltype(batchSize) i = 0; i < batchSize; i++) {
            auto singleMetaSize = uncompressedPayload.readUnsignedInt();
            proto::SingleMessageMetadata singleMeta;
            singleMeta.ParseFromArray(uncompressedPayload.data(), singleMetaSize);
            uncompressedPayload.consume(singleMetaSize);

            auto payload = uncompressedPayload.slice(0, singleMeta.payload_size());
            uncompressedPayload.consume(payload.readableBytes());
            values.emplace_back(payload.data(), payload.readableBytes());
        }
    } else {
        // non-batched message
        values.emplace_back(uncompressedPayload.data(), uncompressedPayload.readableBytes());
    }
    return values;
}

static void testDecryption(Client& client, const std::string& topic, bool decryptionSucceed,
                           int numMessageReceived) {
    ProducerConfiguration producerConf;
    producerConf.setCompressionType(CompressionLZ4);
    producerConf.addEncryptionKey("client-rsa.pem");
    producerConf.setCryptoKeyReader(getDefaultCryptoKeyReader());

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConf, producer));

    std::vector<std::string> sentValues;
    auto send = [&producer, &sentValues](const std::string& value) {
        Message msg = MessageBuilder().setContent(value).build();
        producer.sendAsync(msg, nullptr);
        sentValues.emplace_back(value);
    };

    for (int i = 0; i < 5; i++) {
        send("msg-" + std::to_string(i));
    }
    producer.flush();
    send("last-msg");
    producer.flush();

    ASSERT_EQ(ResultOk, client.createProducer(topic, producer));
    send("unencrypted-msg");
    producer.flush();
    producer.close();

    ConsumerConfiguration consumerConf;
    consumerConf.setSubscriptionInitialPosition(InitialPositionEarliest);
    if (decryptionSucceed) {
        consumerConf.setCryptoKeyReader(getDefaultCryptoKeyReader());
    } else {
        consumerConf.setCryptoFailureAction(ConsumerCryptoFailureAction::CONSUME);
    }
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic, "sub", consumerConf, consumer));

    std::vector<std::string> values;
    for (int i = 0; i < numMessageReceived; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        if (i < numMessageReceived - 1) {
            ASSERT_TRUE(msg.getEncryptionContext().has_value());
        }
        for (auto&& value : decryptValue(msg)) {
            values.emplace_back(value);
        }
    }
    ASSERT_EQ(values, sentValues);
    consumer.close();
}

TEST(EncryptionTests, testDecryptionSuccess) {
    Client client{lookupUrl};
    std::string topic = "test-decryption-success-" + std::to_string(time(nullptr));
    testDecryption(client, topic, true, 7);
    client.close();
}

TEST(EncryptionTests, testDecryptionFailure) {
    Client client{lookupUrl};
    std::string topic = "test-decryption-failure-" + std::to_string(time(nullptr));
    // The 1st batch that has 5 messages cannot be decrypted, so they can be received only once
    testDecryption(client, topic, false, 3);
    client.close();
}

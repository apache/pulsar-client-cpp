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

// Scalable-topics producer: blocking and asynchronous publishing.

#include <pulsar/st/Client.h>

#include <iostream>

using namespace pulsar::st;

int main() {
    // One client per application; keep it for the whole lifetime.
    auto clientResult = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    if (!clientResult) {
        std::cerr << "failed to build client: " << clientResult.error() << "\n";
        return 1;
    }
    PulsarClient client = std::move(clientResult).value();

    auto producerResult = client.newProducer(Schema<std::string>{})
                              .topic("topic://public/default/orders")
                              .sendTimeout(std::chrono::seconds(30))
                              .create();
    if (!producerResult) {
        std::cerr << "failed to create producer: " << producerResult.error() << "\n";
        return 1;
    }
    Producer<std::string> producer = std::move(producerResult).value();

    // Blocking send: returns Expected<MessageId> (must be checked — [[nodiscard]]).
    for (int i = 0; i < 10; i++) {
        auto sent = producer.newMessage()
                        .key("order-" + std::to_string(i % 4))  // per-key ordering
                        .value("payload-" + std::to_string(i))
                        .property("attempt", "1")
                        .send();
        if (sent) {
            std::cout << "sent " << *sent << "\n";
        } else {
            std::cerr << "send failed: " << sent.error() << "\n";
        }
    }

    // Asynchronous send: react on completion without blocking.
    producer.newMessage()
        .key("order-async")
        .value("async-payload")
        .sendAsync()
        .addListener([](const Expected<MessageId>& result) {
            if (result) {
                std::cout << "async sent " << *result << "\n";
            } else {
                std::cerr << "async send failed: " << result.error() << "\n";
            }
        });

    // Transaction: produced messages become visible atomically on commit.
    if (auto txnResult = client.newTransaction()) {
        Transaction txn = *txnResult;
        auto a = producer.newMessage().value("tx-a").transaction(txn).send();
        auto b = producer.newMessage().value("tx-b").transaction(txn).send();
        if (a && b) {
            if (auto committed = txn.commit(); !committed) {
                std::cerr << "commit failed: " << committed.error() << "\n";
            }
        } else {
            (void)txn.abort();
        }
    }

    (void)producer.flush();  // await all sends issued before this call
    if (auto closed = producer.close(); !closed) {
        std::cerr << "close failed: " << closed.error() << "\n";
    }
    (void)client.close();
    return 0;
}

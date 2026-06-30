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

// Scalable-topics QueueConsumer: parallel consumption, individual ack + nack,
// with a dead-letter policy.

#include <pulsar/st/Client.h>

#include <iostream>

using namespace pulsar::st;

int main() {
    auto clientResult = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    if (!clientResult) {
        std::cerr << "failed to build client: " << clientResult.error() << "\n";
        return 1;
    }
    PulsarClient client = std::move(clientResult).value();

    auto consumerResult = client.newQueueConsumer(Schema<std::string>{})
                              .topic("topic://public/default/orders")
                              .subscriptionName("shared-sub")
                              .deadLetterPolicy({.maxRedeliverCount = 5})  // DLQ after 5 redeliveries
                              .subscribe();
    if (!consumerResult) {
        std::cerr << "failed to subscribe: " << consumerResult.error() << "\n";
        return 1;
    }
    QueueConsumer<std::string> consumer = std::move(consumerResult).value();

    // No ordering guarantee; ack each message individually, or nack to redeliver.
    for (;;) {
        auto msg = consumer.receive(std::chrono::seconds(5));
        if (!msg) {
            if (msg.error().result == ResultTimeout) break;
            std::cerr << "receive failed: " << msg.error() << "\n";
            break;
        }

        auto value = msg->value();
        if (!value) {
            std::cerr << "decode failed: " << value.error() << "\n";
            consumer.negativeAcknowledge(msg->id());  // bad payload; redeliver
            continue;
        }
        const bool processed = !value->empty();
        if (processed) {
            consumer.acknowledge(msg->id());  // fire-and-forget; never blocks or errors
        } else {
            consumer.negativeAcknowledge(msg->id());  // schedule redelivery
        }
    }

    (void)consumer.close();
    (void)client.close();
    return 0;
}

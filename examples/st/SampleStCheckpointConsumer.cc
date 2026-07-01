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

// Scalable-topics CheckpointConsumer: the application owns the position. Read,
// snapshot a Checkpoint, persist it externally, and later resume from it.

#include <pulsar/st/Client.h>

#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

using namespace pulsar::st;

int main() {
    auto clientResult = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    if (!clientResult) {
        std::cerr << "failed to build client: " << clientResult.error() << "\n";
        return 1;
    }
    PulsarClient client = std::move(clientResult).value();

    // Restore from a previously stored checkpoint if you have one; else start at
    // the earliest message. (Checkpoint::fromByteArray(savedBytes) to resume.)
    auto consumerResult = client.newCheckpointConsumer(Schema<std::string>{})
                              .topic("topic://public/default/orders")
                              .startPosition(Checkpoint::earliest())
                              .create();  // NOTE: create(), not subscribe()
    if (!consumerResult) {
        std::cerr << "failed to create consumer: " << consumerResult.error() << "\n";
        return 1;
    }
    CheckpointConsumer<std::string> consumer = std::move(consumerResult).value();

    for (int i = 0; i < 5; i++) {
        auto msg = consumer.receive(std::chrono::seconds(5));
        if (!msg) {
            if (msg.error().result == ResultTimeout) break;
            std::cerr << "receive failed: " << msg.error() << "\n";
            break;
        }
        std::cout << "read: " << msg->value() << "\n";
    }

    // Atomic position snapshot across all segments. Store the bytes yourself
    // (Flink/Spark state backend, a file, etc.) — there is no broker-side cursor.
    Checkpoint checkpoint = consumer.checkpoint();
    std::vector<std::byte> persisted = checkpoint.toByteArray();  // store these bytes yourself
    std::cout << "checkpoint is " << persisted.size() << " bytes\n";

    (void)consumer.close();
    (void)client.close();
    return 0;
}

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

// Passing a struct as JSON: `jsonSchema<T>()` derives both the SerDe and the
// declared schema from the struct's fields (via reflect-cpp) — NO macros, NO base
// class, NO schema string, NO serializer. Nested structs and containers included.
// `avroSchema<T>()` is identical for Avro.

#include <pulsar/st/Client.h>
#include <pulsar/st/JsonSchema.h>

#include <iostream>
#include <string>
#include <vector>

// Plain value types — that is the entire schema "declaration".
struct Address {
    std::string street;
    std::string city;
};
struct Order {
    std::string orderId;
    int quantity;
    double unitPrice;
    Address shipTo;                 // nested struct — handled automatically
    std::vector<std::string> tags;  // container — handled automatically
};

using namespace pulsar::st;

int main() {
    auto clientResult = PulsarClient::builder().serviceUrl("pulsar://localhost:6650").build();
    if (!clientResult) {
        std::cerr << clientResult.error() << "\n";
        return 1;
    }
    PulsarClient client = std::move(clientResult).value();

    auto producerResult =
        client.newProducer(jsonSchema<Order>()).topic("topic://public/default/orders").create();
    if (!producerResult) {
        std::cerr << producerResult.error() << "\n";
        return 1;
    }
    Producer<Order> producer = std::move(producerResult).value();

    Order order{"ord-1", 3, 9.99, {"1 Main St", "Springfield"}, {"priority", "gift"}};
    if (auto sent = producer.send(order); sent) {
        std::cout << "sent " << *sent << "\n";
    }

    auto consumerResult = client.newStreamConsumer(jsonSchema<Order>())
                              .topic("topic://public/default/orders")
                              .subscriptionName("orders-sub")
                              .subscribe();
    if (consumerResult) {
        StreamConsumer<Order> consumer = std::move(consumerResult).value();
        if (auto msg = consumer.receive(std::chrono::seconds(5))) {
            Order received = msg->value();  // decoded straight back into the struct
            std::cout << received.orderId << " -> " << received.shipTo.city << "\n";
            consumer.acknowledgeCumulative(msg->id());
        }
        (void)consumer.close();
    }

    (void)producer.close();
    (void)client.close();
    return 0;
}

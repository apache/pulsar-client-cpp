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
#pragma once

#include <gmock/gmock.h>
#include <sys/stat.h>

#include <chrono>
#include <future>
#include <memory>

#include "lib/ClientImpl.h"

namespace pulsar {

class MockClientImpl : public ClientImpl {
   public:
    struct SyncOpResult {
        Result result;
        long timeMs;
    };
    using PromisePtr = std::shared_ptr<std::promise<SyncOpResult>>;
    MockClientImpl(const std::string& serviceUrl, ClientConfiguration conf = {})
        : ClientImpl(serviceUrl, conf) {}

    MOCK_METHOD((Future<Result, ClientConnectionPtr>), getConnection,
                (const std::string&, const std::string&, size_t), (override));

    SyncOpResult createProducer(const std::string& topic) {
        using namespace std::chrono;
        auto start = high_resolution_clock::now();
        auto promise = createPromise();
        createProducerAsync(topic, {}, [&start, promise](Result result, Producer) {
            auto timeMs = duration_cast<milliseconds>(high_resolution_clock::now() - start).count();
            promise->set_value({result, timeMs});
        });
        return wait(promise);
    }

    SyncOpResult subscribe(const std::string& topic) {
        using namespace std::chrono;
        auto start = std::chrono::high_resolution_clock::now();
        auto promise = createPromise();
        subscribeAsync(topic, "sub", {}, [&start, &promise](Result result, Consumer) {
            auto timeMs = duration_cast<milliseconds>(high_resolution_clock::now() - start).count();
            promise->set_value({result, timeMs});
        });
        return wait(promise);
    }

    GetConnectionFuture getConnectionReal(const std::string& topic, size_t key) {
        return ClientImpl::getConnection("", topic, key);
    }

    Result close() {
        auto promise = createPromise();
        closeAsync([promise](Result result) { promise->set_value({result, 0L}); });
        return wait(promise).result;
    }

   private:
    static PromisePtr createPromise() { return std::make_shared<std::promise<SyncOpResult>>(); }

    static SyncOpResult wait(const PromisePtr& promise) {
        using namespace std::chrono;
        auto future = promise->get_future();
        auto status = future.wait_for(std::chrono::seconds(10));
        if (status == std::future_status::ready) {
            return future.get();
        } else {
            return {ResultUnknownError, -1L};
        }
    }
};

}  // namespace pulsar

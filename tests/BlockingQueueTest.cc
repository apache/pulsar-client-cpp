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

#include <future>
#include <iostream>
#include <thread>

#include "lib/BlockingQueue.h"
#include "lib/Latch.h"

class ProducerWorker {
   private:
    std::thread producerThread_;
    BlockingQueue<int>& queue_;

   public:
    ProducerWorker(BlockingQueue<int>& queue) : queue_(queue) {}

    void produce(int number) { producerThread_ = std::thread(&ProducerWorker::pushNumbers, this, number); }

    void pushNumbers(int number) {
        for (int i = 1; i <= number; i++) {
            queue_.push(i);
        }
    }

    void join() { producerThread_.join(); }
};

class ConsumerWorker {
   private:
    std::thread consumerThread_;
    BlockingQueue<int>& queue_;

   public:
    ConsumerWorker(BlockingQueue<int>& queue) : queue_(queue) {}

    void consume(int number) { consumerThread_ = std::thread(&ConsumerWorker::popNumbers, this, number); }

    void popNumbers(int number) {
        for (int i = 1; i <= number; i++) {
            int poppedElement;
            queue_.pop(poppedElement);
        }
    }

    void join() { consumerThread_.join(); }
};

TEST(BlockingQueueTest, testBasic) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    ProducerWorker producerWorker(queue);
    producerWorker.produce(5);

    ConsumerWorker consumerWorker(queue);
    consumerWorker.consume(5);

    producerWorker.join();
    consumerWorker.join();

    size_t zero = 0;
    ASSERT_EQ(zero, queue.size());
}

TEST(BlockingQueueTest, testQueueOperations) {
    size_t size = 5;
    BlockingQueue<int> queue(size);
    for (size_t i = 1; i <= size; i++) {
        queue.push(i);
    }
    ASSERT_EQ(queue.size(), size);

    int cnt = 1;
    for (BlockingQueue<int>::const_iterator it = queue.begin(); it != queue.end(); it++) {
        ASSERT_EQ(cnt, *it);
        ++cnt;
    }

    cnt = 1;
    for (BlockingQueue<int>::iterator it = queue.begin(); it != queue.end(); it++) {
        ASSERT_EQ(cnt, *it);
        ++cnt;
    }

    int poppedElement;
    for (size_t i = 1; i <= size; i++) {
        queue.pop(poppedElement);
    }

    ASSERT_FALSE(queue.peek(poppedElement));
}

TEST(BlockingQueueTest, testBlockingProducer) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    ProducerWorker producerWorker(queue);
    producerWorker.produce(8);

    ConsumerWorker consumerWorker(queue);
    consumerWorker.consume(5);

    producerWorker.join();
    consumerWorker.join();

    size_t three = 3;
    ASSERT_EQ(three, queue.size());
}

TEST(BlockingQueueTest, testPopIf) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    for (int i = 1; i <= size; ++i) {
        queue.push(i);
    }

    // Use producer worker to assert popIf will notify queueFull thread.
    ProducerWorker producerWorker(queue);
    producerWorker.produce(3);

    int value;
    for (int i = 1; i <= size; ++i) {
        ASSERT_TRUE(queue.popIf(value, [&i](const int& peekValue) { return peekValue == i; }));
    }

    producerWorker.join();
    ASSERT_EQ(3, queue.size());
}

TEST(BlockingQueueTest, testBlockingConsumer) {
    size_t size = 5;
    BlockingQueue<int> queue(size);

    ProducerWorker producerWorker(queue);
    producerWorker.produce(5);

    ConsumerWorker consumerWorker(queue);
    consumerWorker.consume(8);

    producerWorker.pushNumbers(3);

    producerWorker.join();
    consumerWorker.join();

    size_t zero = 0;
    ASSERT_EQ(zero, queue.size());
}

TEST(BlockingQueueTest, testTimeout) {
    size_t size = 5;
    BlockingQueue<int> queue(size);
    int value;
    bool popReturn = queue.pop(value, std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_FALSE(popReturn);
}

TEST(BlockingQueueTest, testPushPopRace) {
    auto test_logic = []() {
        size_t size = 5;
        BlockingQueue<int> queue(size);

        std::vector<std::unique_ptr<ProducerWorker>> producers;
        for (int i = 0; i < 5; ++i) {
            producers.emplace_back(new ProducerWorker{queue});
            producers.back()->produce(1000);
        }

        // wait for queue full
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::vector<std::unique_ptr<ConsumerWorker>> consumers;
        for (int i = 0; i < 5; ++i) {
            consumers.emplace_back(new ConsumerWorker{queue});
            consumers.back()->consume(1000);
        }

        auto future = std::async(std::launch::async, [&]() {
            for (auto& p : producers) p->join();
            for (auto& c : consumers) c->join();
        });
        auto ret = future.wait_for(std::chrono::seconds(5));
        if (ret == std::future_status::ready) {
            std::cerr << "Exiting";
            exit(0);
        } else {
            std::cerr << "Threads are not exited in time";
            exit(1);
        }
    };

    ASSERT_EXIT(test_logic(), ::testing::ExitedWithCode(0), "Exiting");
}

TEST(BlockingQueueTest, testCloseInterruptOnEmpty) {
    BlockingQueue<int> queue(10);
    pulsar::Latch latch(1);

    auto thread = std::thread([&]() {
        int v;
        bool res = queue.pop(v);
        ASSERT_FALSE(res);
        latch.countdown();
    });

    // Sleep to allow for background thread to call pop and be blocked there
    std::this_thread::sleep_for(std::chrono::seconds(1));

    queue.close();
    bool wasUnblocked = latch.wait(std::chrono::seconds(5));

    ASSERT_TRUE(wasUnblocked);
    thread.join();
}

TEST(BlockingQueueTest, testCloseInterruptOnFull) {
    BlockingQueue<int> queue(10);
    pulsar::Latch latch(1);

    auto thread = std::thread([&]() {
        int i = 0;
        while (true) {
            bool res = queue.push(i++);
            if (!res) {
                latch.countdown();
                return;
            }
        }
    });

    // Sleep to allow for background thread to fill the queue and be blocked there
    std::this_thread::sleep_for(std::chrono::seconds(1));

    queue.close();
    bool wasUnblocked = latch.wait(std::chrono::seconds(5));

    ASSERT_TRUE(wasUnblocked);
    thread.join();
}

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
#ifndef LIB_BLOCKINGQUEUE_H_
#define LIB_BLOCKINGQUEUE_H_

#include <boost/circular_buffer.hpp>
#include <condition_variable>
#include <mutex>

/**
 * Following structs are defined for holding a predicate in wait() call on condition variables.
 * This is done in order to avoid spurious wake up problem.
 * Details: https://www.justsoftwaresolutions.co.uk/threading/condition-variable-spurious-wakes.html
 */
template <typename Container>
struct QueueNotEmpty {
    const Container& queue_;
    QueueNotEmpty(const Container& queue) : queue_(queue) {}
    bool operator()() const { return !queue_.isEmptyNoMutex() || queue_.isClosedNoMutex(); }
};

template <typename Container>
struct QueueNotFull {
    const Container& queue_;
    QueueNotFull(const Container& queue) : queue_(queue) {}
    bool operator()() const { return !queue_.isFullNoMutex() || queue_.isClosedNoMutex(); }
};

template <typename T>
class BlockingQueue {
   public:
    typedef typename boost::circular_buffer<T> Container;
    typedef typename Container::iterator iterator;
    typedef typename Container::const_iterator const_iterator;

    BlockingQueue(size_t maxSize) : maxSize_(maxSize), mutex_(), queue_(maxSize) {}

    bool push(const T& value) {
        Lock lock(mutex_);

        // If the queue is full, wait for space to be available
        queueFullCondition.wait(lock, QueueNotFull<BlockingQueue<T> >(*this));

        if (isClosedNoMutex()) {
            return false;
        }

        bool wasEmpty = queue_.empty();
        queue_.push_back(value);
        lock.unlock();
        if (wasEmpty) {
            // Notify that an element is pushed
            queueEmptyCondition.notify_all();
        }

        return true;
    }

    bool pop(T& value) {
        Lock lock(mutex_);

        // If the queue is empty, wait until an element is available to be popped
        queueEmptyCondition.wait(lock, QueueNotEmpty<BlockingQueue<T> >(*this));

        if (isClosedNoMutex()) {
            return false;
        }

        value = queue_.front();
        bool wasFull = isFullNoMutex();
        queue_.pop_front();

        lock.unlock();

        if (wasFull) {
            // Notify that an element is popped
            queueFullCondition.notify_all();
        }

        return true;
    }

    template <typename Duration>
    bool pop(T& value, const Duration& timeout) {
        Lock lock(mutex_);
        if (!queueEmptyCondition.wait_for(lock, timeout, QueueNotEmpty<BlockingQueue<T> >(*this))) {
            return false;
        }

        if (isClosedNoMutex()) {
            return false;
        }

        bool wasFull = isFullNoMutex();
        value = queue_.front();
        queue_.pop_front();
        lock.unlock();

        if (wasFull) {
            // Notify that an element is popped
            queueFullCondition.notify_all();
        }

        return true;
    }

    /**
     * First peek data to the condition judgment, if true then pop it.
     *
     * @param value A reference to the value assigned after pop
     * @param condition A function that returns true if the value should be popped
     * @return true if the value was popped, false otherwise.
     */
    bool popIf(T& value, std::function<bool(const T& peekValue)> condition) {
        Lock lock(mutex_);

        if (isEmptyNoMutex() || isClosedNoMutex()) {
            return false;
        }

        bool wasFull = isFullNoMutex();

        auto peekValue = queue_.front();
        if (condition(peekValue)) {
            value = peekValue;
            queue_.pop_front();
            lock.unlock();
            if (wasFull) {
                // Notify that an element is popped
                queueFullCondition.notify_all();
            }
            return true;
        } else {
            return false;
        }
    }

    // Check the 1st element of the queue
    bool peek(T& value) {
        Lock lock(mutex_);
        if (queue_.empty()) {
            return false;
        }

        value = queue_.front();
        return true;
    }

    // Remove all elements from the queue
    void clear() {
        Lock lock(mutex_);
        queue_.clear();
        queueFullCondition.notify_all();
    }

    size_t size() const {
        Lock lock(mutex_);
        return queue_.size();
    }

    size_t maxSize() const { return maxSize_; }

    bool empty() const {
        Lock lock(mutex_);
        return isEmptyNoMutex();
    }

    bool full() const {
        Lock lock(mutex_);
        return isFullNoMutex();
    }

    const_iterator begin() const { return queue_.begin(); }

    const_iterator end() const { return queue_.end(); }

    iterator begin() { return queue_.begin(); }

    iterator end() { return queue_.end(); }

    void close() {
        Lock lock(mutex_);
        isClosed_ = true;
        queueEmptyCondition.notify_all();
        queueFullCondition.notify_all();
    }

   private:
    bool isEmptyNoMutex() const { return queue_.empty(); }

    bool isFullNoMutex() const { return queue_.size() == maxSize_; }

    bool isClosedNoMutex() const { return isClosed_; }

    const size_t maxSize_;
    mutable std::mutex mutex_;
    std::condition_variable queueFullCondition;
    std::condition_variable queueEmptyCondition;
    Container queue_;
    bool isClosed_ = false;

    typedef std::unique_lock<std::mutex> Lock;
    friend struct QueueNotEmpty<BlockingQueue<T> >;
    friend struct QueueNotFull<BlockingQueue<T> >;
};

#endif /* LIB_BLOCKINGQUEUE_H_ */

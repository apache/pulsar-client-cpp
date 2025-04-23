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
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>

#include "lib/ClientConnection.h"
#include "lib/ConsumerImpl.h"
#include "lib/ExecutorService.h"

using std::string;

namespace pulsar {
class ConsumerTest {
   public:
    static int getNumOfMessagesInQueue(const Consumer& consumer) {
        return consumer.impl_->getNumOfPrefetchedMessages();
    }

    template <typename T>
    static DeadlineTimerPtr scheduleCloseConnection(const Consumer& consumer, T delaySinceStartGrabCnx) {
        auto impl = std::dynamic_pointer_cast<ConsumerImpl>(consumer.impl_);
        if (!impl) {
            throw std::runtime_error("scheduleCloseConnection can only be called on ConsumerImpl");
        }

        auto cnx = impl->getCnx().lock();
        if (!cnx) {
            return nullptr;
        }
        auto timer = cnx->executor_->createDeadlineTimer();
        timer->expires_after(delaySinceStartGrabCnx -
                             std::chrono::milliseconds(impl->connectionTimeMs_ + 50));
        timer->async_wait([cnx](const ASIO_ERROR&) { cnx->close(); });
        return timer;
    }
};
}  // namespace pulsar

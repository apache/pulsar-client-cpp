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
#include "LogUtils.h"

#include <pulsar/ConsoleLoggerFactory.h>

#include <atomic>
#include <iostream>

namespace pulsar {

static std::atomic<LoggerFactory*> s_defaultLoggerFactory(new ConsoleLoggerFactory());
static std::atomic<LoggerFactory*> s_loggerFactory(nullptr);

void LogUtils::setLoggerFactory(std::unique_ptr<LoggerFactory> loggerFactory) {
    LoggerFactory* oldFactory = nullptr;
    LoggerFactory* newFactory = loggerFactory.release();
    if (!s_loggerFactory.compare_exchange_strong(oldFactory, newFactory)) {
        delete newFactory;  // there's already a factory set
    }
}

LoggerFactory* LogUtils::getLoggerFactory() {
    if (s_loggerFactory.load() == nullptr) {
        return s_defaultLoggerFactory.load();
    } else {
        return s_loggerFactory.load();
    }
}

std::string LogUtils::getLoggerName(const std::string& path) {
    // Remove all directories from filename
    int startIdx = path.find_last_of("/");
    int endIdx = path.find_last_of(".");
    return path.substr(startIdx + 1, endIdx - startIdx - 1);
}

void LogUtils::resetLoggerFactory() { s_loggerFactory.exchange(nullptr, std::memory_order_release); }

}  // namespace pulsar

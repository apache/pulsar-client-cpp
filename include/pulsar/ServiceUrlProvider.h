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

#include <pulsar/defines.h>

#include <string>

namespace pulsar {

class Client;

class PULSAR_PUBLIC ServiceUrlProvider {
   public:
    virtual ~ServiceUrlProvider() {}

    /**
     * Initialize the service url provider with Pulsar client instance.
     *
     * <p>This can be used by the provider to force the Pulsar client to reconnect whenever the service url
     * might have changed. See {@link Client#updateServiceUrl(const std::string& serviceUrl)}.
     *
     * @param client
     *            created pulsar client.
     */
    virtual void initialize(const Client& client) = 0;

    /**
     * Log the message with related metadata
     *
     * @param level the Logger::Level
     * @param line the line number of this log
     * @param message the message to log
     */
    virtual const std::string& getServiceUrl() = 0;

    /**
     * Close the resource that the provider allocated.
     *
     */
    virtual void close(){};
};

typedef std::shared_ptr<ServiceUrlProvider> ServiceUrlProviderPtr;

}  // namespace pulsar

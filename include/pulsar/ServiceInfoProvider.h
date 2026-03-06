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
#ifndef PULSAR_SERVICE_INFO_PROVIDER_H_
#define PULSAR_SERVICE_INFO_PROVIDER_H_

#include <pulsar/ServiceInfo.h>

#include <functional>

namespace pulsar {

class Client;

class PULSAR_PUBLIC ServiceInfoProvider {
   public:
    /**
     * The destructor will be called when `Client::close()` is invoked, and the provider should stop any
     * ongoing work and release the resources in the destructor.
     */
    virtual ~ServiceInfoProvider() = default;

    /**
     * Initialize the ServiceInfoProvider.
     *
     * @param client the reference to the client, which could be used by the provider to do some necessary
     * @param onServiceInfoUpdate the callback to update `client` with the new `ServiceInfo`
     *
     * Note: the implementation is responsible to invoke `onServiceInfoUpdate` at least once to provide the
     * initial `ServiceInfo` for the client.
     */
    virtual void initialize(Client& client, std::function<void(ServiceInfo)> onServiceInfoUpdate) = 0;
};

};  // namespace pulsar

#endif

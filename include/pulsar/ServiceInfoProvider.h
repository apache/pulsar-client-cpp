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

class PULSAR_PUBLIC ServiceInfoProvider {
   public:
    /**
     * The destructor will be called when `Client::close()` is invoked, and the provider should stop any
     * ongoing work and release the resources in the destructor.
     */
    virtual ~ServiceInfoProvider() = default;

    /**
     * Get the initial `ServiceInfo` connection for the client.
     * This method is called **only once** internally in `Client::create()` to get the initial `ServiceInfo`
     * for the client to connect to the Pulsar service, typically before {@link initialize} is invoked.
     * Since it's only called once, it's legal to return a moved `ServiceInfo` object to avoid unnecessary
     * copying.
     */
    virtual ServiceInfo initialServiceInfo() = 0;

    /**
     * Initialize the ServiceInfoProvider.
     *
     * After the client has obtained the initial `ServiceInfo` via {@link initialServiceInfo}, this method is
     * called to allow the provider to start any background work (for example, service discovery or watching
     * configuration changes) and to report subsequent updates to the service information.
     *
     * @param onServiceInfoUpdate the callback to deliver updated `ServiceInfo` values to the client after
     *                            the initial connection has been established
     *
     * Implementations may choose not to invoke `onServiceInfoUpdate` if the `ServiceInfo` never changes.
     */
    virtual void initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) = 0;
};

};  // namespace pulsar

#endif

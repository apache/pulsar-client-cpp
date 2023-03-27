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
#ifndef PULSAR_CPP_REGEX_SUB_MODE_H
#define PULSAR_CPP_REGEX_SUB_MODE_H

namespace pulsar {
enum RegexSubscriptionMode
{
    /**
     * Only subscribe to persistent topics.
     */
    PersistentOnly = 0,

    /**
     * Only subscribe to non-persistent topics.
     */
    NonPersistentOnly = 1,

    /**
     * Subscribe to both persistent and non-persistent topics.
     */
    AllTopics = 2
};
}

#endif  // PULSAR_CPP_REGEX_SUB_MODE_H

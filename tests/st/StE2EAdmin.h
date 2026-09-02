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

// Shared harness helpers for the scalable-topics end-to-end tests: the PULSAR_ST_E2E
// gate, broker endpoints, fresh per-run topic names, and the admin REST calls that
// create topics and drive split/merge at the exact point a test needs them.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <string>
#include <thread>

#include "tests/HttpHelper.h"

namespace st_e2e {

inline bool e2eEnabled() { return std::getenv("PULSAR_ST_E2E") != nullptr; }

inline std::string serviceUrl() {
    const char* url = std::getenv("PULSAR_ST_E2E_SERVICE_URL");
    return url != nullptr ? url : "pulsar://localhost:6650";
}

inline std::string adminUrl() {
    const char* url = std::getenv("PULSAR_ST_E2E_ADMIN_URL");
    return url != nullptr ? url : "http://localhost:8080";
}

// A fresh topic name per test run so tests never collide with one another or with a
// topic left on a reused broker (the same convention the classic tests use).
inline std::string uniqueName(const std::string& prefix) {
    static int counter = 0;
    return prefix + "-" + std::to_string(std::time(nullptr)) + "-" + std::to_string(counter++);
}

inline std::string topicUrl(const std::string& name) { return "topic://public/default/" + name; }

// The admin REST base for a scalable topic under public/default.
inline std::string scalablePath(const std::string& name) {
    return adminUrl() + "/admin/v2/scalable/public/default/" + name;
}

// Create a scalable topic with the given number of initial segments. Retries while
// the scalable-topics controller finishes coming up after broker start (only the
// first test waits).
inline bool createScalableTopic(const std::string& name, int numInitialSegments = 1) {
    const std::string url = scalablePath(name) + "?numInitialSegments=" + std::to_string(numInitialSegments);
    for (int attempt = 0; attempt < 30; attempt++) {
        const int code = makePutRequest(url, "");
        if (code >= 200 && code < 300) return true;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return false;
}

// Split a segment into two half-range children (POST .../split/{segmentId}).
inline bool splitSegment(const std::string& name, std::int64_t segmentId) {
    const int code = makePostRequest(scalablePath(name) + "/split/" + std::to_string(segmentId), "");
    return code >= 200 && code < 300;
}

// Merge two segments back into one full-range child (POST .../merge/{segmentId1}/{segmentId2}).
inline bool mergeSegments(const std::string& name, std::int64_t segmentId1, std::int64_t segmentId2) {
    const int code = makePostRequest(
        scalablePath(name) + "/merge/" + std::to_string(segmentId1) + "/" + std::to_string(segmentId2), "");
    return code >= 200 && code < 300;
}

}  // namespace st_e2e

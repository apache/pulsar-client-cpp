#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR

if [[ ! $CMAKE_BUILD_DIRECTORY ]]; then
    CMAKE_BUILD_DIRECTORY=.
fi

# Run ExtensibleLoadManager tests
docker compose -f tests/extensibleLM/docker-compose.yml up -d
docker compose -f tests/blue-green/docker-compose.yml up -d
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
until curl http://localhost:8081/metrics > /dev/null 2>&1 ; do sleep 1; done
sleep 5
$CMAKE_BUILD_DIRECTORY/tests/ExtensibleLoadManagerTest
docker compose -f tests/blue-green/docker-compose.yml down
docker compose -f tests/extensibleLM/docker-compose.yml down
exit(1)

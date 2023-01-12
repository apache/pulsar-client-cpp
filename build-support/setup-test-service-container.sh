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

set -e -x

if [ $# -ne 2 ]; then
    echo "Usage: $0 \$CONTAINER_ID \$START_TEST_SERVICE_INSIDE_CONTAINER"
    exit 1
fi

CONTAINER_ID=$1
START_TEST_SERVICE_INSIDE_CONTAINER=$2

echo $CONTAINER_ID >> .tests-container-id.txt

docker cp test-conf $CONTAINER_ID:/pulsar/test-conf
docker cp build-support/$START_TEST_SERVICE_INSIDE_CONTAINER $CONTAINER_ID:$START_TEST_SERVICE_INSIDE_CONTAINER

docker exec -i $CONTAINER_ID /$START_TEST_SERVICE_INSIDE_CONTAINER

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

export http_proxy=
export https_proxy=

# Run OAuth2 tests
docker compose -f tests/oauth2/docker-compose.yml up -d
# Wait until the namespace is created, currently there is no good way to check it
# because it's hard to configure OAuth2 authentication via CLI.
sleep 15
$CMAKE_BUILD_DIRECTORY/tests/Oauth2Test
docker compose -f tests/oauth2/docker-compose.yml down

# Run BrokerMetadata tests
docker compose -f tests/brokermetadata/docker-compose.yml up -d
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
sleep 5
$CMAKE_BUILD_DIRECTORY/tests/BrokerMetadataTest
docker compose -f tests/brokermetadata/docker-compose.yml down

docker compose -f tests/chunkdedup/docker-compose.yml up -d
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
sleep 5
$CMAKE_BUILD_DIRECTORY/tests/ChunkDedupTest --gtest_repeat=10
docker compose -f tests/chunkdedup/docker-compose.yml down

./pulsar-test-service-start.sh

pushd $CMAKE_BUILD_DIRECTORY/tests

# Avoid this test is still flaky, see https://github.com/apache/pulsar-client-cpp/pull/217
./ConnectionFailTest --gtest_repeat=20

export RETRY_FAILED="${RETRY_FAILED:-1}"

if [ -f /gtest-parallel ]; then
    gtest_workers=10
    # use nproc to set workers to 2 x the number of available cores if nproc is available
    if [ -x "$(command -v nproc)" ]; then
      gtest_workers=$(( $(nproc) * 2 ))
    fi
    # set maximum workers to 10
    gtest_workers=$(( gtest_workers > 10 ? 10 : gtest_workers ))
    echo "---- Run unit tests in parallel (workers=$gtest_workers) (retry_failed=${RETRY_FAILED})"
    tests=""
    if [ $# -eq 1 ]; then
        tests="--gtest_filter=$1"
        echo "Running tests: $1"
    fi
    python3 /gtest-parallel $tests --dump_json_test_results=/tmp/gtest_parallel_results.json \
      --workers=$gtest_workers --retry_failed=$RETRY_FAILED -d /tmp \
      ./pulsar-tests --gtest_filter='-CustomLoggerTest*'
    # The customized logger might affect other tests
    ./pulsar-tests --gtest_filter='CustomLoggerTest*'
    RES=$?
else
    ./pulsar-tests
    RES=$?
fi

popd

./pulsar-test-service-stop.sh

exit $RES

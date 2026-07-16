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


# Run ExtensibleLoadManager tests
docker compose -f tests/extensibleLM/docker-compose.yml up -d
docker compose -f tests/blue-green/docker-compose.yml up -d
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
until curl http://localhost:8081/metrics > /dev/null 2>&1 ; do sleep 1; done
sleep 5
$CMAKE_BUILD_DIRECTORY/tests/ExtensibleLoadManagerTest
docker compose -f tests/blue-green/docker-compose.yml down
docker compose -f tests/extensibleLM/docker-compose.yml down

# Run OAuth2 tests
docker compose -f tests/oauth2/docker-compose.yml up -d
# Wait until the namespace is created, currently there is no good way to check it
# because it's hard to configure OAuth2 authentication via CLI.
sleep 15
$CMAKE_BUILD_DIRECTORY/tests/Oauth2Test --gtest_filter='-*testTlsTrustFilePath'
if [[ -f /etc/ssl/certs/ca-certificates.crt ]]; then
    sudo mv /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/my-cert.crt
fi
$CMAKE_BUILD_DIRECTORY/tests/Oauth2Test --gtest_filter='*testTlsTrustFilePath'
if [[ -f /etc/ssl/certs/my-cert.crt ]]; then
    sudo mv /etc/ssl/certs/my-cert.crt /etc/ssl/certs/ca-certificates.crt
fi
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

# Run scalable-topics tests: the broker-free unit tests plus the producer end-to-end tests,
# which publish to a scalable topic on a standalone broker (PULSAR_ST_E2E gates the e2e cases).
docker compose -f tests/st/docker-compose.yml up -d
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
# The 5.0.0-M1 broker image pinned here does not auto-create a scalable topic on lookup (a bug
# fixed in later releases), so pre-create the one the producer e2e publishes to. This is harmless
# once the image carries the fix: the producer's create_if_missing lookup finds the topic either way.
until curl -sf http://localhost:8080/admin/v2/namespaces/public/default > /dev/null 2>&1 ; do sleep 1; done
# Retry: the scalable-topics controller may not be ready the moment the namespace is.
for i in $(seq 1 30); do
    docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics create persistent://public/default/st-e2e-produce && break
    sleep 2
done
# A topic split into two active segments, so the producer e2e exercises cross-segment routing.
docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics create persistent://public/default/st-e2e-split
docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics split-segment -s=0 persistent://public/default/st-e2e-split
# A topic split then merged back, so the e2e exercises routing over a DAG carrying sealed segments.
docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics create persistent://public/default/st-e2e-merge
docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics split-segment -s=0 persistent://public/default/st-e2e-merge
docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics merge-segments --segment-id-1=1 --segment-id-2=2 persistent://public/default/st-e2e-merge
# A single-segment topic the live-split test seals mid-publish; it triggers the split via this command.
docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics create persistent://public/default/st-e2e-live-split
export PULSAR_ST_E2E_SPLIT_CMD="docker exec pulsar-st-standalone bin/pulsar-admin scalable-topics split-segment -s=0 persistent://public/default/st-e2e-live-split"
PULSAR_ST_E2E=1 $CMAKE_BUILD_DIRECTORY/tests/pulsar-st-tests
docker compose -f tests/st/docker-compose.yml down

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

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

cd `dirname $0`
SRC_DIR=$PWD

./pulsar-test-service-stop.sh

CONTAINER_ID=$(docker run -i --user $(id -u) -p 8080:8080 -p 6650:6650 -p 8443:8443 -p 6651:6651 --rm --detach apachepulsar/pulsar:latest sleep 3600)
build-support/setup-test-service-container.sh $CONTAINER_ID start-test-service-inside-container.sh

docker cp $CONTAINER_ID:/pulsar/data/tokens/token.txt .test-token.txt

CONTAINER_ID=$(docker run -i --user $(id -u) -p 8081:8081 -p 6652:6652 -p 8444:8444 -p 6653:6653 --rm --detach apachepulsar/pulsar:latest sleep 3600)
build-support/setup-test-service-container.sh $CONTAINER_ID start-mim-test-service-inside-container.sh

echo "-- Ready to start tests"

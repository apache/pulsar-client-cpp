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

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/build-support

IMAGE_NAME=apachepulsar/cpp-client-format
docker image inspect apachepulsar/cpp-client-format 1>/dev/null 2>&1
OK=$?
set -e
if [[ $OK -ne 0 ]]; then
    echo "The image $IMAGE_NAME doesn't exist, build it"
    docker build -t $IMAGE_NAME -f ./Dockerfile.format .
fi
docker run -v $ROOT_DIR:/app --rm $IMAGE_NAME

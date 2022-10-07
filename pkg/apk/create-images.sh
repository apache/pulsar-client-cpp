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

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pkg/apk

$ROOT_DIR/build-support/copy-deps-versionfile.sh

# ARM
IMAGE=apachepulsar/pulsar-build:alpine-3.16-arm64
docker build --platform arm64 -t $IMAGE . --build-arg PLATFORM=aarch64

# X86_64
IMAGE=apachepulsar/pulsar-build:alpine-3.16-x86_64
docker build --platform x86_64 -t $IMAGE . --build-arg PLATFORM=x86_64

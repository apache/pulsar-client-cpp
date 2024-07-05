#!/bin/bash
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

set -ex
cd `dirname $0`/../..

if [[ $ARCH == "arm64" ]]; then
    VCPKG_TARGET_TRIPLET=arm64-osx
    CMAKE_OSX_ARCHITECTURES=arm64
else
    VCPKG_TARGET_TRIPLET=x64-osx
    CMAKE_OSX_ARCHITECTURES=x86_64
fi

cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DINTEGRATE_VCPKG=ON -DBUILD_TESTS=OFF -DBUILD_PERF_TOOLS=OFF \
    -DCMAKE_OSX_ARCHITECTURES=$CMAKE_OSX_ARCHITECTURES \
    -DVCPKG_TARGET_TRIPLET=$VCPKG_TARGET_TRIPLET
cmake --build build -j8
TRIPLET=$VCPKG_TARGET_TRIPLET ./build-support/merge_archives_vcpkg.sh $PWD/build
mv build/libpulsarwithdeps.a build/lib/

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

set -e
cd `dirname $0`/../..

if [[ -z $ARCH ]]; then
    ARCH=$(uname -m)
    echo "Use default ARCH: $ARCH"
fi
if [[ $ARCH == "x86_64" ]]; then
    export VCPKG_TRIPLET=x64-osx
elif [[ $ARCH == "arm64" ]]; then
    export VCPKG_TRIPLET=arm64-osx
else
    echo "Invalid ARCH: $ARCH"
    exit 1
fi

INSTALL_DIR=$PWD/pkg/mac/.install
set -x
cmake -B build-osx \
    -DCMAKE_OSX_DEPLOYMENT_TARGET=13.0 \
    -DINTEGRATE_VCPKG=ON \
    -DVCPKG_OVERLAY_TRIPLETS=$PWD/vcpkg-triplets \
    -DVCPKG_TARGET_TRIPLET=$VCPKG_TRIPLET \
    -DCMAKE_OSX_ARCHITECTURES=$ARCH \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTS=OFF \
    -DBUILD_PERF_TOOLS=OFF \
    -DBUILD_DYNAMIC_LIB=ON \
    -DBUILD_STATIC_LIB=ON \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR
cmake --build build-osx -j16 --target install

./build-support/merge_archives_vcpkg.sh $PWD/build-osx
cp ./build-osx/libpulsarwithdeps.a $INSTALL_DIR/lib/

# Test the libraries
clang++ win-examples/example.cc -o dynamic.out -std=c++11 -arch $ARCH -I $INSTALL_DIR/include -L $INSTALL_DIR/lib -Wl,-rpath $INSTALL_DIR/lib -lpulsar
./dynamic.out
clang++ win-examples/example.cc -o static.out -std=c++11 -arch $ARCH -I $INSTALL_DIR/include $INSTALL_DIR/lib/libpulsarwithdeps.a
./static.out

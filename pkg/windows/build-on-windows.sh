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

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <triplet> [Debug|Release]"
    exit 1
fi

set -ex

TRIPLET=$1
MODE=$2
if [[ ! -e $ARCH ]]; then
    if [[ $TRIPLET =~ x64 ]]; then
        ARCH=x64
    elif [[ $TRIPLET = x86-windows ]]; then
        ARCH=Win32
    else
        echo "ARCH environment variable must be set because triplet $TRIPLET is unknown"
        exit 2
    fi
fi

echo "triplet: $TRIPLET, mode: $MODE, architecture: $ARCH"

if [[ ! -e $BUILD_DIR ]]; then
    BUILD_DIR=./build
fi
mkdir -p $BUILD_DIR
vcpkg install --feature-flags=manifests --triplet $TRIPLET
cmake -B $BUILD_DIR \
    -A $ARCH \
    -DVCPKG_TRIPLET=$TRIPLET \
    -DCMAKE_BUILD_TYPE=$MODE \
    -DBUILD_PYTHON_WRAPPER=OFF -DBUILD_TESTS=OFF \
    -S .
cmake --build $BUILD_DIR --config $MODE

if [[ ! -e $PACKAGE_DIR ]]; then
    PACKAGE_DIR=./package
fi
LIB_DIR=$PACKAGE_DIR/lib/$MODE
VCPKG_INSTALLED_DIR=$PACKAGE_DIR/vcpkg_installed
mkdir -p $PACKAGE_DIR
mkdir -p $LIB_DIR
mkdir -p $VCPKG_INSTALLED_DIR/$TRIPLET

cp -r include/ $PACKAGE_DIR
cp -r $BUILD_DIR/include/ $PACKAGE_DIR
cp -r $BUILD_DIR/lib/$MODE/*.lib $LIB_DIR
cp -r $BUILD_DIR/lib/$MODE/*.dll $LIB_DIR
cp -r ./vcpkg_installed/$TRIPLET/* $VCPKG_INSTALLED_DIR/$TRIPLET

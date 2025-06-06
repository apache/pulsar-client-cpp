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

if [ $# -neq 2 ]; then
    echo "Usage: $0 \$DEST_PATH \$WORKFLOW_ID"
    exit 1
fi

DEST_PATH=$(readlink -f $1)
WORKFLOW_ID=$2

pushd $(dirname "$0")
PULSAR_CPP_PATH=$(git rev-parse --show-toplevel)
popd

mkdir -p $DEST_PATH

cd $PULSAR_CPP_PATH

build-support/generate-source-archive.sh $DEST_PATH
build-support/download-release-artifacts.py $WORKFLOW_ID $DEST_PATH

pushd "$DEST_PATH"
if [[ -d x64-windows-static ]]; then
    tar cvzf x64-windows-static.tar.gz x64-windows-static
    rm -rf x64-windows-static/
fi
if [[ -d x86-windows-static ]]; then
    tar cvzf x86-windows-static.tar.gz x86-windows-static
    rm -rf x86-windows-static/
fi
if [[ -d macos-arm64.zip ]]; then
    mv macos-arm64.zip macos-arm64
    mv macos-arm64/* .
    rm -rf macos-arm64
fi
if [[ -d macos-x86_64.zip ]]; then
    mv macos-x86_64.zip macos-x86_64
    mv macos-x86_64/* .
    rm -rf macos-x86_64/
fi
popd

# Sign all files
cd $DEST_PATH
find . -type f | xargs $PULSAR_CPP_PATH/build-support/sign-files.sh

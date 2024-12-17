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

cd `dirname $0`/..
set -ex

COMMIT_ID=$(grep "builtin-baseline" vcpkg.json | sed 's/"//g' | sed 's/,//' | awk '{print $2}')
cd vcpkg
git reset --hard $COMMIT_ID
git apply ../build-support/vcpkg-curl-patch.diff
git add ports/curl
git commit -m "Disable IPv6 for macOS in curl"
./bootstrap-vcpkg.sh
./vcpkg x-add-version --all
git add versions/
git commit -m "Update version"
COMMIT_ID=$(git log --pretty=oneline | head -n 1 | awk '{print $1}')

cd ..
sed -i.bak "s/.*builtin-baseline.*/  \"builtin-baseline\": \"$COMMIT_ID\",/" vcpkg.json
sed -i.bak "s/\"version>=\": \"8\.4\.0\"/\"version>=\": \"8.4.0#1\"/" vcpkg.json

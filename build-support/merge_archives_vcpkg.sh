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

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <cmake-build-directory>"
    exit 1
fi

CMAKE_BUILD_DIRECTORY=$1
./merge_archives.sh $CMAKE_BUILD_DIRECTORY/libpulsarwithdeps.a \
    $CMAKE_BUILD_DIRECTORY/lib/libpulsar.a \
    $(find "$CMAKE_BUILD_DIRECTORY/vcpkg_installed" -name "*.a" | grep -v debug)

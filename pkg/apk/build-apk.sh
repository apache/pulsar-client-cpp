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

cd /pulsar-client-cpp
export ROOT_DIR=$(pwd)

rm -f CMakeCache.txt
rm -rf CMakeFiles

cd $ROOT_DIR/pkg/apk



POM_VERSION=`cat $ROOT_DIR/version.txt | xargs`
# Sanitize the version string
export VERSION=`echo $POM_VERSION | sed -E 's/\-[a-zA-Z]+//'`

echo "VERSION: $VERSION"

abuild-keygen -a -i -n
abuild -F -c -r

cp -r /root/packages/pkg ./build

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

cd /pulsar-client-cpp
ROOT_DIR=$(pwd)
cd $ROOT_DIR/pkg/rpm

if [[ $PLATFORM == "aarch64" ]]; then
    export VCPKG_FORCE_SYSTEM_BINARIES=arm
fi

POM_VERSION=`cat $ROOT_DIR/version.txt | xargs`

# Sanitize VERSION by removing `-incubating` since it's not legal in RPM
VERSION=`echo $POM_VERSION | awk -F-  '{print $1}'`

mkdir -p BUILD RPMS SOURCES SPECS SRPMS

cp $ROOT_DIR/apache-pulsar-client-cpp-$POM_VERSION.tar.gz SOURCES

rpmbuild -v -bb --clean \
        --define "version $VERSION" \
        --define "pom_version $POM_VERSION" \
        --define "_topdir $PWD" \
        SPECS/pulsar-client.spec

cd RPMS/${PLATFORM}
createrepo .

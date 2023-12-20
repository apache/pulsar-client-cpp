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
git config --global --add safe.directory /pulsar-client-cpp
git submodule update --init --recursive
if [[ $ARCH == "aarch64" ]]; then
    export VCPKG_FORCE_SYSTEM_BINARIES=arm
fi
SRC_ROOT_DIR=$(pwd)
CPP_DIR="$SRC_ROOT_DIR/build"
chmod +x $(find . -name "*.sh")
cmake -B $CPP_DIR -DINTEGRATE_VCPKG=ON -DBUILD_TESTS=OFF -DBUILD_DYNAMIC_LIB=ON -DBUILD_STATIC_LIB=ON
cmake --build $CPP_DIR -j8

POM_VERSION=`cat $SRC_ROOT_DIR/version.txt | xargs`
# Sanitize VERSION by removing `SNAPSHOT` if any since it's not legal in DEB
VERSION=`echo $POM_VERSION | awk -F-  '{print $1}'`

if [[ $ARCH == "aarch64" ]]; then
    export PLATFORM="arm64"
else
    export PLATFORM="amd64"
fi

DEST_DIR=apache-pulsar-client
mkdir -p $DEST_DIR/DEBIAN
cat <<EOF > $DEST_DIR/DEBIAN/control
Package: apache-pulsar-client
Version: ${VERSION}
Maintainer: Apache Pulsar <dev@pulsar.apache.org>
Architecture: ${PLATFORM}
Description: The Apache Pulsar client contains a C++ and C APIs to interact with Apache Pulsar brokers.
EOF

DEVEL_DEST_DIR=apache-pulsar-client-dev
mkdir -p $DEVEL_DEST_DIR/DEBIAN
cat <<EOF > $DEVEL_DEST_DIR/DEBIAN/control
Package: apache-pulsar-client-dev
Version: ${VERSION}
Maintainer: Apache Pulsar <dev@pulsar.apache.org>
Architecture: ${PLATFORM}
Depends: apache-pulsar-client
Description: The Apache Pulsar client contains a C++ and C APIs to interact with Apache Pulsar brokers.
EOF

mkdir -p $DEST_DIR/usr/lib
mkdir -p $DEVEL_DEST_DIR/usr/lib
mkdir -p $DEVEL_DEST_DIR/usr/include
mkdir -p $DEST_DIR/usr/share/doc/pulsar-client-$VERSION
mkdir -p $DEVEL_DEST_DIR/usr/share/doc/pulsar-client-dev-$VERSION

ls $CPP_DIR/lib/libpulsar*

mkdir -p $DEVEL_DEST_DIR/usr/include/pulsar
cp -ar $SRC_ROOT_DIR/include/pulsar/* $DEVEL_DEST_DIR/usr/include/pulsar/
cp -ar $CPP_DIR/include/pulsar/* $DEVEL_DEST_DIR/usr/include/pulsar/
cp $CPP_DIR/lib/libpulsar.a $DEVEL_DEST_DIR/usr/lib
# TODO: generate libpulsarwithdeps.a
#cp $CPP_DIR/lib/libpulsarwithdeps.a $DEVEL_DEST_DIR/usr/lib
cp $CPP_DIR/lib/libpulsar.so $DEST_DIR/usr/lib

cp $SRC_ROOT_DIR/NOTICE $DEST_DIR/usr/share/doc/pulsar-client-$VERSION
cp $SRC_ROOT_DIR/pkg/licenses/* $DEST_DIR/usr/share/doc/pulsar-client-$VERSION
cp $SRC_ROOT_DIR/pkg/licenses/LICENSE.txt $DEST_DIR/usr/share/doc/pulsar-client-$VERSION/copyright
cp $SRC_ROOT_DIR/pkg/licenses/LICENSE.txt $DEST_DIR/DEBIAN/copyright
cp $SRC_ROOT_DIR/pkg/licenses/LICENSE.txt $DEVEL_DEST_DIR/DEBIAN/copyright

cp $DEST_DIR/usr/share/doc/pulsar-client-$VERSION/* $DEVEL_DEST_DIR/usr/share/doc/pulsar-client-dev-$VERSION


## Build actual debian packages
dpkg-deb --build $DEST_DIR
dpkg-deb --build $DEVEL_DEST_DIR

mkdir DEB
mv *.deb DEB
cd DEB
dpkg-scanpackages . /dev/null | gzip -9c > Packages.gz

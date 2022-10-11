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

# Build Pulsar C++ client on macOS with Intel-64 architecture
# Required:
# - CMake >= 3.12
# - C++ compiler that supports C++11

# Fail script in case of errors
set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR

python3 -m pip install pyyaml

OS=darwin64-x86_64-cc
export CFLAGS="-fPIC -arch x86_64 -mmacosx-version-min=10.15"
export CXXFLAGS=$CFLAGS

BOOST_VERSION=$(./build-support/dep-version.py boost)
PROTOBUF_VERSION=$(./build-support/dep-version.py protobuf)
ZLIB_VERSION=$(./build-support/dep-version.py zlib)
ZSTD_VERSION=$(./build-support/dep-version.py zstd)
SNAPPY_VERSION=$(./build-support/dep-version.py snappy)
OPENSSL_VERSION=$(./build-support/dep-version.py openssl)
CURL_VERSION=$(./build-support/dep-version.py curl)

CACHE_DIR=~/.pulsar-cpp-mac-build
PREFIX=$CACHE_DIR/install
mkdir -p $CACHE_DIR
cd $CACHE_DIR

download() {
    URL=$1
    BASENAME=$(basename $URL)
    if [[ ! -f $BASENAME ]]; then
        echo "curl -O -L $URL"
        curl -O -L $URL
    fi
    tar xfz $BASENAME
}

install() {
    make -j${NUM_MAKE_THREAD:-8} 2>&1 >/dev/null
    make install 2>&1 >/dev/null
    touch .done
}

# Install Boost
BOOST_VERSION_=${BOOST_VERSION//./_}
DIR=boost_$BOOST_VERSION_
if [[ ! -f $DIR/.done ]]; then
    echo "Building Boost $BOOST_VERSION"
    download https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_}.tar.gz
    mkdir -p $PREFIX/include
    # We only need to copy the headers because Pulsar only uses the header-only part
    cp -rf $DIR/boost $PREFIX/include
    touch $DIR/.done
else
    echo "Using cached Boost $BOOST_VERSION"
fi

# Install Protobuf
if [[ ! -f protobuf-${PROTOBUF_VERSION}/.done ]]; then
    echo "Building Protobuf ${PROTOBUF_VERSION}"
    download https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOBUF_VERSION/protobuf-cpp-$PROTOBUF_VERSION.tar.gz
    pushd protobuf-$PROTOBUF_VERSION
        ./configure --prefix=$PREFIX 2>&1 >/dev/null
        install
    popd
else
    echo "Using cached Protobuf $PROTOBUF_VERSION"
fi

# Install zlib
if [[ ! -f zlib-$ZLIB_VERSION/.done ]]; then
    echo "Building zlib $ZLIB_VERSION"
    download https://zlib.net/zlib-${ZLIB_VERSION}.tar.gz
    pushd zlib-$ZLIB_VERSION
        ./configure --prefix=$PREFIX 2>&1 >/dev/null
        install
    popd
else
    echo "Using cached zlib $ZLIB_VERSION"
fi

# Install zstd
if [[ ! -f zstd-$ZSTD_VERSION/.done ]]; then
    echo "Building zstd $ZSTD_VERSION"
    download https://github.com/facebook/zstd/releases/download/v$ZSTD_VERSION/zstd-$ZSTD_VERSION.tar.gz
    pushd zstd-$ZSTD_VERSION
        PREFIX=$PREFIX install
    popd
else
    echo "Using cached zstd $ZSTD_VERSION"
fi

# Install Snappy
if [[ ! -f snappy-$SNAPPY_VERSION/.done ]]; then
    echo "Building snappy $SNAPPY_VERSION"
    download https://github.com/google/snappy/archive/refs/tags/$SNAPPY_VERSION.tar.gz
    pushd snappy-$SNAPPY_VERSION
        cmake . -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF -DCMAKE_INSTALL_PREFIX=$PREFIX 2>&1 >/dev/null
        install
    popd
else
    echo "Using cached snappy $SNAPPY_VERSION"
fi

# Install OpenSSL
OPENSSL_VERSION_=${OPENSSL_VERSION//./_}
DIR=openssl-OpenSSL_${OPENSSL_VERSION_}
if [[ ! -f $DIR/.done ]]; then
    echo "Building OpenSSL $OPENSSL_VERSION"
    download https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_${OPENSSL_VERSION_}.tar.gz
    pushd $DIR
        # Patch for OpenSSL 1.1.1q, see https://github.com/openssl/openssl/issues/18720
        sed -i.bak 's/#include <stdio.h>/#include <stdio.h>\n#include <string.h>/' test/v3ext.c
        rm -f test/v3ext.c.bak
        ./Configure --prefix=$PREFIX $OS 2>&1 >/dev/null
        install
    popd
else
    echo "Using cached OpenSSL $OPENSSL_VERSION"
fi

# Install libcurl
if [[ ! -f curl-$CURL_VERSION/.done ]]; then
    echo "Building libcurl $CURL_VERSION"
    download https://github.com/curl/curl/releases/download/curl-${CURL_VERSION//./_}/curl-$CURL_VERSION.tar.gz
    pushd curl-${CURL_VERSION}
        ./configure --prefix=$PREFIX \
            --with-ssl=$PREFIX \
            --without-nghttp2 --without-libidn2 \
            --without-librtmp --without-brotli \
            --disable-ldap 2>&1 >/dev/null
        install
    popd
else
    echo "Using cached libcurl $CURL_VERSION"
fi

# Build Pulsar client
cd $ROOT_DIR
BUILD_DIR=_builds
cmake -Wno-dev -B $BUILD_DIR \
    -DCMAKE_CXX_FLAGS="-framework SystemConfiguration -framework CoreFoundation" \
    -DPROTOC_PATH=$PREFIX/bin/protoc \
    -DLINK_STATIC=ON \
    -DBUILD_TESTS=OFF -DBUILD_PERF_TOOLS=OFF \
    -DCMAKE_PREFIX_PATH=$PREFIX
cmake --build $BUILD_DIR -j${NUM_MAKE_THREAD:-8}

# The libraries have been generated under _builds/
ls -lh _builds/lib

# Test example build
cat <<EOF > /tmp/main.cc
#include <pulsar/Client.h>
using namespace pulsar;

int main() {
    Client client("pulsar://localhost:6650");
    client.close();
    return 0;
}
EOF
set -x
clang++ -std=c++11 /tmp/main.cc -I ./include $BUILD_DIR/lib/libpulsarwithdeps.a -o /tmp/main.out \
    -framework SystemConfiguration -framework CoreFoundation
/tmp/main.out

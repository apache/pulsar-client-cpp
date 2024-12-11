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
cd `dirname $0`

python3 -m venv venv
source venv/bin/activate
python3 -m pip install pyyaml

MACOSX_DEPLOYMENT_TARGET=10.15
if [[ -z ${ARCH} ]]; then
    ARCH=`uname -m`
fi

BUILD_DIR=$PWD/.build
INSTALL_DIR=$PWD/.install
PREFIX=$BUILD_DIR/install
mkdir -p $BUILD_DIR
cp -f ../../build-support/dep-version.py $BUILD_DIR/
cp -f ../../dependencies.yaml $BUILD_DIR/

pushd $BUILD_DIR

BOOST_VERSION=$(./dep-version.py boost)
ZLIB_VERSION=$(./dep-version.py zlib)
OPENSSL_VERSION=$(./dep-version.py openssl)
PROTOBUF_VERSION=$(./dep-version.py protobuf)
ZSTD_VERSION=$(./dep-version.py zstd)
SNAPPY_VERSION=$(./dep-version.py snappy)
CURL_VERSION=$(./dep-version.py curl)

if [ ! -f boost/.done ]; then
    echo "Building Boost $BOOST_VERSION"
    curl -O -L https://github.com/boostorg/boost/releases/download/boost-${BOOST_VERSION}/boost-${BOOST_VERSION}.tar.gz
    tar zxf boost-${BOOST_VERSION}.tar.gz
    mkdir -p $PREFIX/include
    pushd boost-${BOOST_VERSION}
      ./bootstrap.sh
      ./b2 headers
      cp -rf boost $PREFIX/include/
    popd
    mkdir -p boost
    touch boost/.done
else
    echo "Using cached Boost $BOOST_VERSION"
fi

if [ ! -f zlib-${ZLIB_VERSION}/.done ]; then
    echo "Building ZLib $ZLIB_VERSION"
    curl -O -L https://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz
    tar zxf zlib-${ZLIB_VERSION}.tar.gz
    pushd zlib-$ZLIB_VERSION
      CFLAGS="-fPIC -O3 -arch ${ARCH} -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" ./configure --prefix=$PREFIX
      make -j16
      make install
      touch .done
    popd
else
    echo "Using cached ZLib $ZLIB_VERSION"
fi

OPENSSL_VERSION_UNDERSCORE=$(echo $OPENSSL_VERSION | sed 's/\./_/g')
if [ ! -f openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.done ]; then
    echo "Building OpenSSL $OPENSSL_VERSION"
    curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_$OPENSSL_VERSION_UNDERSCORE.tar.gz
    tar zxf OpenSSL_$OPENSSL_VERSION_UNDERSCORE.tar.gz

    pushd openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}
      if [[ $ARCH = 'arm64' ]]; then
          PLATFORM=darwin64-arm64-cc
      else
          PLATFORM=darwin64-x86_64-cc
      fi
      CFLAGS="-fPIC -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
          ./Configure --prefix=$PREFIX no-shared no-unit-test $PLATFORM
      make -j8 >/dev/null
      make install_sw >/dev/null
    popd

    touch openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.done
else
    echo "Using cached OpenSSL $OPENSSL_VERSION"
fi

if [ ! -f protobuf-${PROTOBUF_VERSION}/.done ]; then
    echo "Building Protobuf $PROTOBUF_VERSION"
    curl -O -L https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz
    tar zxf protobuf-cpp-${PROTOBUF_VERSION}.tar.gz
    pushd protobuf-${PROTOBUF_VERSION}
      pushd cmake/
        # Build protoc that can run on both x86 and arm architectures
        cmake -B build -DCMAKE_CXX_FLAGS="-fPIC -arch x86_64 -arch arm64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
            -Dprotobuf_BUILD_TESTS=OFF \
            -DCMAKE_INSTALL_PREFIX=$PREFIX
        cmake --build build -j16 --target install
      popd

      # Retain the library for one architecture so that `ar` can work on the library
      pushd $PREFIX/lib
        mv libprotobuf.a libprotobuf_universal.a
        lipo libprotobuf_universal.a -thin ${ARCH} -output libprotobuf.a
      popd
      touch .done
    popd
else
    echo "Using cached Protobuf $PROTOBUF_VERSION"
fi

if [ ! -f zstd-${ZSTD_VERSION}/.done ]; then
    echo "Building ZStd $ZSTD_VERSION"
    curl -O -L https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz
    tar zxf zstd-${ZSTD_VERSION}.tar.gz
    pushd zstd-${ZSTD_VERSION}
      CFLAGS="-fPIC -O3 -arch ${ARCH} -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" PREFIX=$PREFIX \
            make -j16 -C lib install-static install-includes
      touch .done
    popd
else
    echo "Using cached ZStd $ZSTD_VERSION"
fi

if [ ! -f snappy-${SNAPPY_VERSION}/.done ]; then
    echo "Building Snappy $SNAPPY_VERSION"
    curl -O -L https://github.com/google/snappy/archive/refs/tags/${SNAPPY_VERSION}.tar.gz
    tar zxf ${SNAPPY_VERSION}.tar.gz
    pushd snappy-${SNAPPY_VERSION}
      # Without this patch, snappy 1.10 will report a sign-compare error, which cannot be suppressed with the -Wno-sign-compare option in CI
      curl -O -L https://raw.githubusercontent.com/microsoft/vcpkg/2024.02.14/ports/snappy/no-werror.patch
      patch <no-werror.patch
      CXXFLAGS="-fPIC -O3 -arch ${ARCH} -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
          cmake . -DCMAKE_INSTALL_PREFIX=$PREFIX -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF
      make -j16
      make install
      touch .done
    popd
else
    echo "Using cached Snappy $SNAPPY_VERSION"
fi

if [ ! -f curl-${CURL_VERSION}/.done ]; then
    echo "Building LibCurl $CURL_VERSION"
    CURL_VERSION_=${CURL_VERSION//./_}
    curl -O -L https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_}/curl-${CURL_VERSION}.tar.gz
    tar zxf curl-${CURL_VERSION}.tar.gz
    pushd curl-${CURL_VERSION}
      # Force the compiler to find the OpenSSL headers instead of the headers in the system path like /usr/local/include/openssl.
      cp -rf $PREFIX/include/openssl include/
      CFLAGS="-I$PREFIX/include -fPIC -arch ${ARCH} -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
            ./configure --with-ssl=$PREFIX \
              --without-nghttp2 \
              --without-libidn2 \
              --disable-ldap \
              --without-brotli \
              --without-secure-transport \
              --without-librtmp \
              --disable-ipv6 \
              --without-libpsl \
              --host=$ARCH-apple-darwin \
              --prefix=$PREFIX
      make -j16 install
      touch .done
    popd
else
    echo "Using cached LibCurl $CURL_VERSION"
fi

popd # pkg/mac
cd ../../ # project root

cmake -B build-static -DCMAKE_OSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET \
    -DLINK_STATIC=ON \
    -DBUILD_TESTS=OFF \
    -DBUILD_DYNAMIC_LIB=ON \
    -DBUILD_STATIC_LIB=ON \
    -DCMAKE_OSX_ARCHITECTURES=${ARCH} \
    -DCMAKE_PREFIX_PATH=$PREFIX \
    -DOPENSSL_ROOT_DIR=$PREFIX \
    -DPROTOC_PATH=$PREFIX/bin/protoc \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build-static -j16 --target install

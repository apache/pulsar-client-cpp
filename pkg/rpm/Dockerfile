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

# Build pulsar client library in Centos with tools to build static RPM

FROM centos:7

ARG PLATFORM

RUN yum update -y && \
    yum install -y \
        gcc-c++ \
        make \
        rpm-build \
        which \
        createrepo \
        libstdc++-static \
        python3

RUN pip3 install pyyaml

ADD .build/dependencies.yaml /
ADD .build/dep-version.py /usr/local/bin

# Download and compile boost
# GCC 4.8.2 implementation of std::regex is buggy, so we install boost::regex here
RUN BOOST_VERSION=$(dep-version.py boost) && \
    echo "BOOST VERSION: '${BOOST_VERSION}'" && \
    BOOST_VERSION_UNDESRSCORE=$(echo $BOOST_VERSION | sed 's/\./_/g') && \
    curl -O -L https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz && \
    tar xfz boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz && \
    cd boost_${BOOST_VERSION_UNDESRSCORE} && \
    ./bootstrap.sh --with-libraries=regex && \
    ./b2 address-model=64 cxxflags=-fPIC link=static threading=multi variant=release install && \
    rm -rf /boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz /boost_${BOOST_VERSION_UNDESRSCORE}

RUN CMAKE_VERSION=$(dep-version.py cmake) && \
    curl -O -L https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${PLATFORM}.tar.gz && \
    tar xfz cmake-${CMAKE_VERSION}-linux-${PLATFORM}.tar.gz && \
    cp cmake-${CMAKE_VERSION}-linux-${PLATFORM}/bin/* /usr/bin/ && \
    cp -r cmake-${CMAKE_VERSION}-linux-${PLATFORM}/share/cmake-* /usr/share/ && \
    rm -rf cmake-${CMAKE_VERSION}-linux-${PLATFORM} cmake-${CMAKE_VERSION}-linux-${PLATFORM}.tar.gz

# Download and copile protoubf
RUN PROTOBUF_VERSION=$(dep-version.py protobuf) && \
    curl -O -L  https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz && \
    tar xfz protobuf-cpp-${PROTOBUF_VERSION}.tar.gz && \
    cd protobuf-${PROTOBUF_VERSION}/ && \
    CXXFLAGS=-fPIC ./configure && \
    make -j8 && make install && ldconfig && \
    rm -rf /protobuf-cpp-${PROTOBUF_VERSION}.tar.gz /protobuf-${PROTOBUF_VERSION}

# ZLib
RUN ZLIB_VERSION=$(dep-version.py zlib) && \
    curl -O -L https://github.com/madler/zlib/archive/v${ZLIB_VERSION}.tar.gz && \
    tar xfz v${ZLIB_VERSION}.tar.gz && \
    cd zlib-${ZLIB_VERSION} && \
    CFLAGS="-fPIC -O3" ./configure && \
    make -j8 && make install && \
    rm -rf /v${ZLIB_VERSION}.tar.gz /zlib-${ZLIB_VERSION}

# Zstandard
RUN ZSTD_VERSION=$(dep-version.py zstd) && \
    curl -O -L https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz && \
    tar xfz zstd-${ZSTD_VERSION}.tar.gz && \
    cd zstd-${ZSTD_VERSION} && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-${ZSTD_VERSION} /zstd-${ZSTD_VERSION}.tar.gz

# Snappy
RUN SNAPPY_VERSION=$(dep-version.py snappy) && \
    curl -O -L https://github.com/google/snappy/archive/refs/tags/${SNAPPY_VERSION}.tar.gz && \
    tar xfz ${SNAPPY_VERSION}.tar.gz && \
    cd snappy-${SNAPPY_VERSION} && \
    CXXFLAGS="-fPIC -O3" cmake . -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF && \
    make -j8 && make install && \
    rm -rf /snappy-${SNAPPY_VERSION} /${SNAPPY_VERSION}.tar.gz

RUN OPENSSL_VERSION=$(dep-version.py openssl) && \
    OPENSSL_VERSION_UNDERSCORE=$(echo $OPENSSL_VERSION | sed 's/\./_/g') && \
    curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz && \
    tar xfz OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz && \
    cd openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}/ && \
    ./Configure -fPIC --prefix=/usr/local/ssl/ linux-${PLATFORM} && \
    make -j8 && make install && \
    rm -rf /OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz /openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:
ENV OPENSSL_ROOT_DIR /usr/local/ssl/

# LibCurl
RUN CURL_VERSION=$(dep-version.py curl) && \
    CURL_VERSION_UNDERSCORE=$(echo $CURL_VERSION | sed 's/\./_/g') && \
    curl -O -L  https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_UNDERSCORE}/curl-${CURL_VERSION}.tar.gz && \
    tar xfz curl-${CURL_VERSION}.tar.gz && \
    cd curl-${CURL_VERSION} && \
    CFLAGS=-fPIC ./configure --with-ssl=/usr/local/ssl/ --without-zstd && \
    make -j8 && make install && \
    rm -rf /curl-${CURL_VERSION}.tar.gz /curl-${CURL_VERSION}


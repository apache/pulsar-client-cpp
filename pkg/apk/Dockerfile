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

FROM alpine:3.16

ARG PLATFORM

# perl is required to install OpenSSL
RUN apk add \
      build-base \
      abuild \
      bash \
      curl \
      g++ \
      make \
      python3 \
      cmake \
      boost-dev \
      perl \
      sudo

# Download and copile protoubf
RUN curl -O -L  https://github.com/google/protobuf/releases/download/v3.20.0/protobuf-cpp-3.20.0.tar.gz && \
    tar xfz protobuf-cpp-3.20.0.tar.gz && \
    cd protobuf-3.20.0/ && \
    CXXFLAGS=-fPIC ./configure && \
    make -j8 && make install && \
    rm -rf /protobuf-cpp-3.20.0.tar.gz /protobuf-3.20.0

# ZLib
RUN curl -O -L https://github.com/madler/zlib/archive/v1.2.12.tar.gz && \
    tar xfz v1.2.12.tar.gz && \
    cd zlib-1.2.12 && \
    CFLAGS="-fPIC -O3" ./configure && \
    make && make install && \
    rm -rf /v1.2.12.tar.gz /zlib-1.2.12

# Zstandard
RUN curl -O -L https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz && \
    tar xfz zstd-1.5.2.tar.gz && \
    cd zstd-1.5.2 && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-1.5.2 /zstd-1.5.2.tar.gz

# Snappy
RUN curl -O -L https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz && \
    tar xfz snappy-1.1.3.tar.gz && \
    cd snappy-1.1.3 && \
    CXXFLAGS="-fPIC -O3" ./configure && \
    make && make install && \
    rm -rf /snappy-1.1.3 /snappy-1.1.3.tar.gz

RUN curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_1_1_1n.tar.gz && \
    tar xfz OpenSSL_1_1_1n.tar.gz && \
    cd openssl-OpenSSL_1_1_1n/ && \
    ./Configure -fPIC --prefix=/usr/local/ssl/ linux-${PLATFORM} && \
    make -j8 && make install && \
    rm -rf /OpenSSL_1_1_1n.tar.gz /openssl-OpenSSL_1_1_1n

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:

# LibCurl
RUN curl -O -L  https://github.com/curl/curl/releases/download/curl-7_61_0/curl-7.61.0.tar.gz && \
    tar xfz curl-7.61.0.tar.gz && \
    cd curl-7.61.0 && \
    CFLAGS=-fPIC ./configure --with-ssl=/usr/local/ssl/ && \
    make -j8 && make install && \
    rm -rf /curl-7.61.0.tar.gz /curl-7.61.0

ENV OPENSSL_ROOT_DIR /usr/local/ssl/

RUN adduser pulsar -D
RUN addgroup pulsar abuild

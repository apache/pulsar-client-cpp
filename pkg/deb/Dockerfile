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

# Build pulsar client library in Centos with tools to

FROM ubuntu:22.04

ARG PLATFORM

RUN apt-get update -y && \
     apt-get install -y \
        g++ \
        ninja-build \
        python3 \
        git \
        curl \
        zip \
        unzip \
        tar \
        pkg-config \
        dpkg-dev

# CMake >= 3.15 is required for vcpkg
WORKDIR /app
RUN if [[ $ARCH == "arm64" ]]; then SUFFIX="aarch64"; else SUFFIX="x86_64"; fi && \
    curl -O -L https://github.com/Kitware/CMake/releases/download/v3.28.1/cmake-3.28.1-linux-${PLATFORM}.tar.gz && \
    tar zxf cmake-3.28.1-linux-${PLATFORM}.tar.gz && \
    mv cmake-3.28.1-linux-${PLATFORM}/ cmake/ && \
    rm -f cmake-3.28.1-linux-*.tar.gz
ENV PATH="/app/cmake/bin:$PATH"
ENV ARCH=${PLATFORM}

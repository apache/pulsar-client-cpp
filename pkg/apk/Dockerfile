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

FROM alpine:3.12

ARG PLATFORM

RUN apk add build-base \
      linux-headers \
      abuild \
      make \
      curl \
      cmake \
      git \
      ninja \
      zip \
      unzip \
      openssl-dev \
      perl \
      bash

# Build latest CMake for vcpkg
RUN curl -O -L https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0.tar.gz \
    && tar zxf cmake-3.30.0.tar.gz \
    && cd cmake-3.30.0 \
    && ./configure && make -j8 && make install \
    && cd .. && rm -rf cmake-3.30.0 cmake-3.30.0.tar.gz

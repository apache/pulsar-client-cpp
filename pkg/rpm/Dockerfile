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

FROM rockylinux:8

ARG PLATFORM

RUN yum update -y && \
    yum install -y \
        gcc-c++ \
        make \
        rpm-build \
        which \
        createrepo \
        git
RUN dnf --enablerepo=powertools install -y libstdc++-static

# Vcpkg does not provide pre-built binaries for Arm architectures so we need to build vcpkg from source
RUN yum install -y cmake
RUN dnf --enablerepo=devel install -y ninja-build

# Dependencies when building OpenSSL
RUN yum install -y perl-IPC-Cmd

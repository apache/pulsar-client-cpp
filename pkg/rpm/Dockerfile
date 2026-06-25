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

# gcc-toolset-12 supplies GCC 12 for the C++20 scalable-topics code (concepts,
# coroutines, `using enum`) while keeping the el8 / glibc 2.28 ABI, so the produced
# RPM still runs on RHEL/Rocky/CentOS 8. It is enabled in the spec %build via
# `source /opt/rh/gcc-toolset-12/enable`.
RUN yum update -y && \
    yum install -y \
        gcc-c++ \
        gcc-toolset-12 \
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
# OpenSSL 3.6.1 requires the Time::Piece Perl module (packaged separately on EL8)
RUN yum install -y perl-IPC-Cmd perl-Time-Piece

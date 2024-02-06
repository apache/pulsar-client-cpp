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

cmake_minimum_required(VERSION 3.7)
project(pulsar-cpp-wireshark)

if (NOT CMAKE_CXX_STANDARD)
    if (APPLE)
        # The latest Protobuf dependency on macOS requires the C++17 support
        set(CMAKE_CXX_STANDARD 17)
    else ()
        set(CMAKE_CXX_STANDARD 11)
    endif ()
endif ()

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    execute_process(COMMAND sh -c "find $(brew --prefix)/Cellar/wireshark -name 'include' -type d"
            OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE
            EXTRA_INCLUDE_PATH)
endif()
find_path(WIRESHARK_INCLUDE_PATH wireshark/ws_version.h PATHS ${EXTRA_INCLUDE_PATH})
if (WIRESHARK_INCLUDE_PATH)
    add_definitions("-DWITH_WS_VERSION")
else ()
    message(STATUS "Cannot find ws_version.h, fallback to find config.h")
    find_path(WIRESHARK_INCLUDE_PATH wireshark/config.h PATHS ${EXTRA_INCLUDE_PATH})
endif ()
if (NOT WIRESHARK_INCLUDE_PATH)
    message(FATAL_ERROR "Failed to find WIRESHARK_INCLUDE_PATH")
endif ()

include(FindPkgConfig)
pkg_check_modules(GLIB glib-2.0)

MESSAGE(STATUS "Use WIRESHARK_INCLUDE_PATH: ${WIRESHARK_INCLUDE_PATH}")
MESSAGE(STATUS "Use GLIB_INCLUDE_DIRS: ${GLIB_INCLUDE_DIRS}")

set(protobuf_MODULE_COMPATIBLE ON CACHE BOOL "")
if (APPLE)
    find_package(Protobuf REQUIRED CONFIG)
else ()
    find_package(Protobuf REQUIRED)
endif ()

set(PROTO_SOURCES PulsarApi.pb.cc)
protobuf_generate_cpp(${PROTO_SOURCES}
    PulsarApi.pb.h
    ${CMAKE_CURRENT_SOURCE_DIR}/../proto/PulsarApi.proto)

include_directories(${WIRESHARK_INCLUDE_PATH}/wireshark
    ${GLIB_INCLUDE_DIRS}
    ${CMAKE_BINARY_DIR})

# Build wireshark shared lib
add_library(pulsar-dissector SHARED pulsarDissector.cc ${PROTO_SOURCES})
SET(CMAKE_SHARED_LIBRARY_SUFFIX .so)
set_target_properties(pulsar-dissector PROPERTIES PREFIX "" DEFINE_SYMBOL "")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
endif()

target_link_libraries(pulsar-dissector protobuf::libprotobuf-lite) 

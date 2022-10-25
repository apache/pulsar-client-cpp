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

cmake_minimum_required(VERSION 3.4)
project(pulsar-cpp-win-examples)

find_path(PULSAR_INCLUDES NAMES "pulsar/Client.h")
if (PULSAR_INCLUDES)
    message(STATUS "PULSAR_INCLUDES: " ${PULSAR_INCLUDES})
else ()
    message(FATAL_ERROR "Failed to find PULSAR_INCLUDES")
endif ()
option(LINK_STATIC "Link statically to pulsar" ON)
if (LINK_STATIC)
    find_library(PULSAR_LIBRARIES NAMES "pulsarWithDeps")
else ()
    find_library(PULSAR_LIBRARIES NAMES "pulsar")
endif ()
if (PULSAR_LIBRARIES)
    message(STATUS "PULSAR_LIBRARIES: " ${PULSAR_LIBRARIES})
else ()
    message(FATAL_ERROR "Failed to find PULSAR_LIBRARIES")
endif ()

if (LINK_STATIC)
    string(REGEX REPLACE "/MD" "/MT" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    add_definitions(-DPULSAR_STATIC)
endif ()
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: " ${CMAKE_CXX_FLAGS_RELEASE})

add_executable(win-example "example.cc")
include_directories(${PULSAR_INCLUDES})
target_link_libraries(win-example PRIVATE ${PULSAR_LIBRARIES})

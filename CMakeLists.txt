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

cmake_minimum_required(VERSION 3.13)

option(INTEGRATE_VCPKG "Integrate with Vcpkg" OFF)
if (INTEGRATE_VCPKG)
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake")
endif ()

option(BUILD_TESTS "Build tests" ON)
message(STATUS "BUILD_TESTS:  " ${BUILD_TESTS})
if (BUILD_TESTS)
    list(APPEND VCPKG_MANIFEST_FEATURES "tests")
endif ()

option(BUILD_PERF_TOOLS "Build Pulsar CLI perf producer/consumer" OFF)
message(STATUS "BUILD_PERF_TOOLS:  " ${BUILD_PERF_TOOLS})
if (BUILD_PERF_TOOLS)
    list(APPEND VCPKG_MANIFEST_FEATURES "perf")
endif ()

project (pulsar-cpp)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake_modules")

execute_process(COMMAND cat ${PROJECT_SOURCE_DIR}/version.txt OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE PULSAR_CLIENT_VERSION)
message(STATUS "Pulsar Client version: ${PULSAR_CLIENT_VERSION}")

execute_process(COMMAND ${PROJECT_SOURCE_DIR}/build-support/gen-pulsar-version-macro.py OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE PULSAR_CLIENT_VERSION_MACRO)
message(STATUS "Pulsar Client version macro: ${PULSAR_CLIENT_VERSION_MACRO}")

set(PVM_COMMENT "This is generated from Version.h.in by CMAKE. DO NOT EDIT DIRECTLY")
configure_file(templates/Version.h.in include/pulsar/Version.h @ONLY)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "ccache")
    MESSAGE(STATUS "Using CCache")
endif(CCACHE_PROGRAM)

MESSAGE(STATUS "ARCHITECTURE: ${CMAKE_SYSTEM_PROCESSOR}")

option(BUILD_DYNAMIC_LIB "Build dynamic lib" ON)
MESSAGE(STATUS "BUILD_DYNAMIC_LIB:  " ${BUILD_DYNAMIC_LIB})

option(BUILD_STATIC_LIB "Build static lib" ON)
MESSAGE(STATUS "BUILD_STATIC_LIB:  " ${BUILD_STATIC_LIB})

IF (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
ENDIF ()

MESSAGE(STATUS "CMAKE_BUILD_TYPE:  " ${CMAKE_BUILD_TYPE})

set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
MESSAGE(STATUS "Threads library: " ${CMAKE_THREAD_LIBS_INIT})

# Compiler specific configuration:
# https://stackoverflow.com/questions/10046114/in-cmake-how-can-i-test-if-the-compiler-is-clang
if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    add_definitions(-DWIN32_LEAN_AND_MEAN -DNOGDI -D_WIN32_WINNT=0x0501 -D_CRT_SECURE_NO_WARNINGS)
    add_compile_options(/wd4244 /wd4267 /wd4018 /wd4715 /wd4251 /wd4275 /wd4819)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    # ?? Don't have this to test with
else() # GCC or Clang are mostly compatible:
    # Turn on warnings and enable warnings-as-errors:
    add_compile_options(-Wall -Wformat-security -Wvla -Werror) 
    # Turn off certain warnings that are too much pain for too little gain:
    add_compile_options(-Wno-sign-compare -Wno-deprecated-declarations -Wno-error=cpp)
    if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR APPLE)
        add_compile_options(-msse4.2 -mpclmul)
    endif()
    # Options unique to Clang or GCC:
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options(-Qunused-arguments) 
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8.1))
        add_compile_options(-Wno-stringop-truncation)
    endif()
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_definitions(-DBUILDING_PULSAR -DBOOST_ALL_NO_LIB -DBOOST_ALLOW_DEPRECATED_HEADERS)

set(AUTOGEN_DIR ${PROJECT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${AUTOGEN_DIR})

if (INTEGRATE_VCPKG)
    if (NOT CMAKE_CXX_STANDARD)
        set(CMAKE_CXX_STANDARD 11)
    endif ()
    set(CMAKE_C_STANDARD 11)
    set(Boost_NO_BOOST_CMAKE ON)
    find_package(Boost REQUIRED)
    include_directories(${Boost_INCLUDE_DIRS})
    message("Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")

    set(CURL_NO_CURL_CMAKE ON)
    find_package(CURL REQUIRED)
    find_package(ZLIB REQUIRED)
    find_package(OpenSSL REQUIRED)
    find_package(protobuf CONFIG REQUIRED)
    find_package(zstd CONFIG REQUIRED)
    find_package(Snappy CONFIG REQUIRED)
    set(COMMON_LIBS CURL::libcurl
        ZLIB::ZLIB
        OpenSSL::SSL
        OpenSSL::Crypto
        protobuf::libprotobuf
        $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>
        Snappy::snappy
        )
    add_definitions(-DHAS_ZSTD -DHAS_SNAPPY)
    if (MSVC)
        find_package(dlfcn-win32 CONFIG REQUIRED)
    endif ()
    if (BUILD_PERF_TOOLS)
        find_package(Boost COMPONENTS program_options REQUIRED)
    endif ()
else ()
    include(./LegacyFindPackages.cmake)
endif ()

set(LIB_NAME $ENV{PULSAR_LIBRARY_NAME})
if (NOT LIB_NAME)
    set(LIB_NAME pulsar)
endif(NOT LIB_NAME)

set(CLIENT_LIBS
  ${COMMON_LIBS}
  ${LIB_NAME}
)

add_subdirectory(lib)
if(BUILD_PERF_TOOLS)
    add_subdirectory(perf)
endif(BUILD_PERF_TOOLS)

if (BUILD_DYNAMIC_LIB)
    add_subdirectory(examples)
endif()

if (BUILD_TESTS)
    set(TOKEN_PATH "${PROJECT_SOURCE_DIR}/.test-token.txt")
    set(TEST_CONF_DIR "${PROJECT_SOURCE_DIR}/test-conf")
    add_subdirectory(tests)
endif()

find_package(ClangTools)
set(BUILD_SUPPORT_DIR "${PROJECT_SOURCE_DIR}/build-support")
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        0
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        ${PROJECT_SOURCE_DIR}/lib
        ${PROJECT_SOURCE_DIR}/perf
        ${PROJECT_SOURCE_DIR}/examples
        ${PROJECT_SOURCE_DIR}/tests
        ${PROJECT_SOURCE_DIR}/tests/unix
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/wireshark)

# `make check-format` option (for CI test)
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        1
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        ${PROJECT_SOURCE_DIR}/lib
        ${PROJECT_SOURCE_DIR}/perf
        ${PROJECT_SOURCE_DIR}/examples
        ${PROJECT_SOURCE_DIR}/tests
        ${PROJECT_SOURCE_DIR}/tests/unix
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/wireshark)

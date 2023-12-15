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

option(LINK_STATIC "Link against static libraries" OFF)
if (VCPKG_TRIPLET)
    message(STATUS "Use vcpkg, triplet is ${VCPKG_TRIPLET}")
    set(CMAKE_PREFIX_PATH "${PROJECT_SOURCE_DIR}/vcpkg_installed/${VCPKG_TRIPLET}")
    message(STATUS "Use CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")
    set(PROTOC_PATH "${CMAKE_PREFIX_PATH}/tools/protobuf/protoc")
    message(STATUS "Use protoc: ${PROTOC_PATH}")
    set(VCPKG_ROOT "${PROJECT_SOURCE_DIR}/vcpkg_installed/${VCPKG_TRIPLET}")
    set(VCPKG_DEBUG_ROOT "${VCPKG_ROOT}/debug")
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(ZLIB_ROOT ${VCPKG_DEBUG_ROOT})
        set(OPENSSL_ROOT_DIR ${VCPKG_ROOT} ${VCPKG_DEBUG_ROOT})
        set(CMAKE_PREFIX_PATH ${VCPKG_DEBUG_ROOT} ${CMAKE_PREFIX_PATH})
    else ()
        set(OPENSSL_ROOT_DIR ${VCPKG_ROOT})
    endif ()
    if (VCPKG_TRIPLET MATCHES ".*-static")
        set(LINK_STATIC ON)
    else ()
        set(LINK_STATIC OFF)
    endif ()
endif()
MESSAGE(STATUS "LINK_STATIC:  " ${LINK_STATIC})

if (MSVC)
    find_package(dlfcn-win32 REQUIRED)
endif ()

set(Boost_NO_BOOST_CMAKE ON)

if (APPLE AND NOT LINK_STATIC)
    # The latest Protobuf dependency on macOS requires the C++17 support and
    # it could only be found by the CONFIG mode
    set(LATEST_PROTOBUF TRUE)
else ()
    set(LATEST_PROTOBUF FALSE)
endif ()

if (NOT CMAKE_CXX_STANDARD)
    if (LATEST_PROTOBUF)
        set(CMAKE_CXX_STANDARD 17)
    else ()
        set(CMAKE_CXX_STANDARD 11)
    endif ()
endif ()
set(CMAKE_C_STANDARD 11)

# For dependencies other than OpenSSL, dynamic libraries are forbidden to link when LINK_STATIC is ON
if (LINK_STATIC)
    if (NOT MSVC)
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    endif()
endif ()

find_package(Boost REQUIRED)
message("Boost_INCLUDE_DIRS: " ${Boost_INCLUDE_DIRS})

set(OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR} /usr/lib64/)
if (APPLE)
    set(OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR} /usr/local/opt/openssl/ /opt/homebrew/opt/openssl)
endif ()
find_package(OpenSSL REQUIRED)
message("OPENSSL_INCLUDE_DIR: " ${OPENSSL_INCLUDE_DIR})
message("OPENSSL_LIBRARIES: " ${OPENSSL_LIBRARIES})

if (LATEST_PROTOBUF)
    # See https://github.com/apache/arrow/issues/35987
    add_definitions(-DPROTOBUF_USE_DLLS)
    # Use Config mode to avoid FindProtobuf.cmake does not find the Abseil library
    find_package(Protobuf REQUIRED CONFIG)
else ()
    find_package(Protobuf REQUIRED)
endif ()
message("Protobuf_INCLUDE_DIRS: " ${Protobuf_INCLUDE_DIRS})
message("Protobuf_LIBRARIES: " ${Protobuf_LIBRARIES})

# NOTE: CMake might not find curl and zlib on some platforms like Ubuntu, in this case, find them manually
set(CURL_NO_CURL_CMAKE ON)
find_package(curl QUIET)
if (NOT CURL_FOUND)
    find_path(CURL_INCLUDE_DIRS NAMES curl/curl.h)
    find_library(CURL_LIBRARIES NAMES curl curllib libcurl_imp curllib_static libcurl)
endif ()
message("CURL_INCLUDE_DIRS: " ${CURL_INCLUDE_DIRS})
message("CURL_LIBRARIES: " ${CURL_LIBRARIES})
if (NOT CURL_INCLUDE_DIRS OR NOT CURL_LIBRARIES)
    message(FATAL_ERROR "Could not find libcurl")
endif ()

find_package(zlib QUIET)
if (NOT ZLIB_FOUND)
    find_path(ZLIB_INCLUDE_DIRS NAMES zlib.h)
    find_library(ZLIB_LIBRARIES NAMES z zlib zdll zlib1 zlibstatic)
endif ()
message("ZLIB_INCLUDE_DIRS: " ${ZLIB_INCLUDE_DIRS})
message("ZLIB_LIBRARIES: " ${ZLIB_LIBRARIES})
if (NOT ZLIB_INCLUDE_DIRS OR NOT ZLIB_LIBRARIES)
    message(FATAL_ERROR "Could not find zlib")
endif ()

if (LINK_STATIC AND NOT VCPKG_TRIPLET)
    find_library(LIB_ZSTD NAMES libzstd.a)
    message(STATUS "ZStd: ${LIB_ZSTD}")
    find_library(LIB_SNAPPY NAMES libsnappy.a)
    message(STATUS "LIB_SNAPPY: ${LIB_SNAPPY}")

    if (MSVC)
        add_definitions(-DCURL_STATICLIB)
    endif()
elseif (LINK_STATIC AND VCPKG_TRIPLET)
    find_package(Protobuf REQUIRED)
    message(STATUS "Found protobuf static library: " ${Protobuf_LIBRARIES})
    if (MSVC AND (${CMAKE_BUILD_TYPE} STREQUAL Debug))
        find_library(ZLIB_LIBRARIES NAMES zlibd)
    else ()
        find_library(ZLIB_LIBRARIES NAMES zlib z)
    endif ()
    if (ZLIB_LIBRARIES)
        message(STATUS "Found zlib static library: " ${ZLIB_LIBRARIES})
    else ()
        message(FATAL_ERROR "Failed to find zlib static library")
    endif ()
    if (MSVC AND (${CMAKE_BUILD_TYPE} STREQUAL Debug))
        find_library(CURL_LIBRARIES NAMES libcurl-d)
    else ()
        find_library(CURL_LIBRARIES NAMES libcurl)
    endif ()
    if (CURL_LIBRARIES)
        message(STATUS "Found libcurl: ${CURL_LIBRARIES}")
    else ()
        message(FATAL_ERROR "Cannot find libcurl")
    endif ()
    find_library(LIB_ZSTD zstd)
    if (LIB_ZSTD)
        message(STATUS "Found ZSTD library: ${LIB_ZSTD}")
    endif ()
    find_library(LIB_SNAPPY NAMES snappy)
    if (LIB_SNAPPY)
        message(STATUS "Found Snappy library: ${LIB_SNAPPY}")
    endif ()
else()
    if (MSVC AND (${CMAKE_BUILD_TYPE} STREQUAL Debug))
        find_library(LIB_ZSTD zstdd HINTS "${VCPKG_DEBUG_ROOT}/lib")
    else ()
        find_library(LIB_ZSTD zstd)
    endif ()
    if (MSVC AND (${CMAKE_BUILD_TYPE} STREQUAL Debug))
        find_library(LIB_SNAPPY NAMES snappyd HINTS "${VCPKG_DEBUG_ROOT}/lib")
    else ()
        find_library(LIB_SNAPPY NAMES snappy libsnappy)
    endif ()
endif ()

if (Boost_MAJOR_VERSION EQUAL 1 AND Boost_MINOR_VERSION LESS 69)
    # Boost System does not require linking since 1.69
    set(BOOST_COMPONENTS ${BOOST_COMPONENTS} system)
    MESSAGE(STATUS "Linking with Boost:System")
endif()

if (MSVC)
    set(BOOST_COMPONENTS ${BOOST_COMPONENTS} date_time)
endif()

if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.9)
    # GCC 4.8.2 implementation of std::regex is buggy
    set(BOOST_COMPONENTS ${BOOST_COMPONENTS} regex)
    set(CMAKE_CXX_FLAGS " -DPULSAR_USE_BOOST_REGEX")
    MESSAGE(STATUS "Using Boost::Regex")
elseif (CMAKE_COMPILER_IS_GNUCC)
    MESSAGE(STATUS "Using std::regex")
    # Turn on color error messages and show additional help with errors (only available in GCC v4.9+):
    add_compile_options(-fdiagnostics-show-option -fdiagnostics-color)
endif()

if(BUILD_PERF_TOOLS)
    set(BOOST_COMPONENTS ${BOOST_COMPONENTS} program_options)
endif()

find_package(Boost REQUIRED COMPONENTS ${BOOST_COMPONENTS})

if (BUILD_TESTS)
    find_path(GTEST_INCLUDE_PATH gtest/gtest.h)
    find_path(GMOCK_INCLUDE_PATH gmock/gmock.h)
endif ()

if (NOT APPLE AND NOT MSVC)
    # Hide all non-exported symbols to avoid conflicts
    add_compile_options(-fvisibility=hidden)
    if (CMAKE_COMPILER_IS_GNUCC)
        add_link_options(-Wl,--exclude-libs=ALL)
    endif ()
endif ()

if (LIB_ZSTD)
    set(HAS_ZSTD 1)
else ()
    set(HAS_ZSTD 0)
endif ()
MESSAGE(STATUS "HAS_ZSTD: ${HAS_ZSTD}")

if (LIB_SNAPPY)
    set(HAS_SNAPPY 1)
else ()
    set(HAS_SNAPPY 0)
endif ()
MESSAGE(STATUS "HAS_SNAPPY: ${HAS_SNAPPY}")

set(ADDITIONAL_LIBRARIES $ENV{PULSAR_ADDITIONAL_LIBRARIES})
link_directories( $ENV{PULSAR_ADDITIONAL_LIBRARY_PATH} )

include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_BINARY_DIR}/include
  ${AUTOGEN_DIR}
  ${Boost_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIR}
  ${ZLIB_INCLUDE_DIRS}
  ${CURL_INCLUDE_DIRS}
  ${Protobuf_INCLUDE_DIRS}
  ${GTEST_INCLUDE_PATH}
  ${GMOCK_INCLUDE_PATH}
)

set(COMMON_LIBS
  ${COMMON_LIBS}
  ${CMAKE_THREAD_LIBS_INIT}
  ${Boost_REGEX_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${Boost_DATE_TIME_LIBRARY}
  ${CURL_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${ADDITIONAL_LIBRARIES}
  ${CMAKE_DL_LIBS}
)

if (LATEST_PROTOBUF)
    # Protobuf_LIBRARIES is empty when finding Protobuf in Config mode
    set(COMMON_LIBS ${COMMON_LIBS} protobuf::libprotobuf)
else ()
    set(COMMON_LIBS ${COMMON_LIBS} ${Protobuf_LIBRARIES})
endif ()

if (MSVC)
    set(COMMON_LIBS
        ${COMMON_LIBS}
        ${Boost_DATE_TIME_LIBRARY}
        wldap32.lib
        Normaliz.lib)
    if (LINK_STATIC)
        # add external dependencies of libcurl
        set(COMMON_LIBS ${COMMON_LIBS} ws2_32.lib crypt32.lib)
        # the default compile options have /MD, which cannot be used to build DLLs that link static libraries
        string(REGEX REPLACE "/MD" "/MT" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
        string(REGEX REPLACE "/MD" "/MT" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
        string(REGEX REPLACE "/MD" "/MT" CMAKE_CXX_FLAGS_RELWITHDEBINFO ${CMAKE_CXX_FLAGS_RELWITHDEBINFO})
        message(STATUS "CMAKE_CXX_FLAGS_DEBUG: " ${CMAKE_CXX_FLAGS_DEBUG})
        message(STATUS "CMAKE_CXX_FLAGS_RELEASE: " ${CMAKE_CXX_FLAGS_RELEASE})
        message(STATUS "CMAKE_CXX_FLAGS_RELWITHDEBINFO: " ${CMAKE_CXX_FLAGS_RELWITHDEBINFO})
    endif ()
else()
    set(COMMON_LIBS ${COMMON_LIBS} m)
endif()

if (USE_LOG4CXX)
    set(COMMON_LIBS
      ${COMMON_LIBS}
      ${LOG4CXX_LIBRARY_PATH}
      ${APR_LIBRARY_PATH}
      ${APR_UTIL_LIBRARY_PATH}
      ${EXPAT_LIBRARY_PATH}
      ${ICONV_LIBRARY_PATH}
    )
endif ()

if (HAS_ZSTD)
    set(COMMON_LIBS ${COMMON_LIBS} ${LIB_ZSTD} )
endif ()

add_definitions(-DHAS_ZSTD=${HAS_ZSTD})

if (HAS_SNAPPY)
    set(COMMON_LIBS ${COMMON_LIBS} ${LIB_SNAPPY} )
endif ()

add_definitions(-DHAS_SNAPPY=${HAS_SNAPPY})

if(NOT APPLE AND NOT MSVC)
    set(COMMON_LIBS ${COMMON_LIBS} rt)
endif ()

link_directories(${PROJECT_BINARY_DIR}/lib)

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

option(LINK_STATIC "Link against static libraries" OFF)
if (VCPKG_TRIPLET)
    message(STATUS "Use vcpkg, triplet is ${VCPKG_TRIPLET}")
    set(CMAKE_PREFIX_PATH "${PROJECT_SOURCE_DIR}/vcpkg_installed/${VCPKG_TRIPLET}")
    message(STATUS "Use CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")
    set(PROTOC_PATH "${CMAKE_PREFIX_PATH}/tools/protobuf/protoc")
    message(STATUS "Use protoc: ${PROTOC_PATH}")
    set(VCPKG_DEBUG_ROOT "${PROJECT_SOURCE_DIR}/vcpkg_installed/${VCPKG_TRIPLET}/debug")
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(ZLIB_ROOT ${VCPKG_DEBUG_ROOT})
        set(OPENSSL_ROOT_DIR ${VCPKG_DEBUG_ROOT})
        set(CMAKE_PREFIX_PATH ${VCPKG_DEBUG_ROOT} ${CMAKE_PREFIX_PATH})
    endif ()
    if (VCPKG_TRIPLET MATCHES ".*-static")
        set(LINK_STATIC ON)
    else ()
        set(LINK_STATIC OFF)
    endif ()
endif()
MESSAGE(STATUS "LINK_STATIC:  " ${LINK_STATIC})

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

option(BUILD_TESTS "Build tests" ON)
MESSAGE(STATUS "BUILD_TESTS:  " ${BUILD_TESTS})

option(BUILD_WIRESHARK "Build Pulsar Wireshark dissector" OFF)
MESSAGE(STATUS "BUILD_WIRESHARK:  " ${BUILD_WIRESHARK})

option(BUILD_PERF_TOOLS "Build Pulsar CLI perf producer/consumer" OFF)
MESSAGE(STATUS "BUILD_PERF_TOOLS:  " ${BUILD_PERF_TOOLS})

option(USE_LOG4CXX "Build with Log4cxx support" OFF)
MESSAGE(STATUS "USE_LOG4CXX:  " ${USE_LOG4CXX})

IF (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
ENDIF ()

MESSAGE(STATUS "CMAKE_BUILD_TYPE:  " ${CMAKE_BUILD_TYPE})

set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
MESSAGE(STATUS "Threads library: " ${CMAKE_THREAD_LIBS_INIT})

set(Boost_NO_BOOST_CMAKE ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 11)

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

set(LOG_CATEGORY_NAME $ENV{LOG_CATEGORY_NAME})

if (NOT LOG_CATEGORY_NAME)
    set(LOG_CATEGORY_NAME "\"pulsar.\"")
endif(NOT LOG_CATEGORY_NAME)

add_definitions(-DLOG_CATEGORY_NAME=${LOG_CATEGORY_NAME} -DBUILDING_PULSAR -DBOOST_ALL_NO_LIB -DBOOST_ALLOW_DEPRECATED_HEADERS)

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

find_package(Protobuf REQUIRED)
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

    if (USE_LOG4CXX)
        if (LOG4CXX_USE_DYNAMIC_LIBS)
            find_library(LOG4CXX_LIBRARY_PATH log4cxx)
        else ()
            find_library(LOG4CXX_LIBRARY_PATH NAMES liblog4cxx.a)

            # Libraries needed by log4cxx to link statically with
            find_library(APR_LIBRARY_PATH NAMES libapr-1 PATHS /usr/lib /usr/local/apr/lib /usr/local/opt/apr/libexec/lib/)
            find_library(APR_UTIL_LIBRARY_PATH NAMES libaprutil-1 PATHS /usr/lib /usr/local/apr/lib /usr/local/opt/apr-util/libexec/lib/)
            find_library(EXPAT_LIBRARY_PATH NAMES libexpat expat)
            if (APPLE)
                find_library(ICONV_LIBRARY_PATH NAMES libiconv iconv)
            else ()
                set(ICONV_LIBRARY_PATH )
            endif (APPLE)
        endif (LOG4CXX_USE_DYNAMIC_LIBS)
    endif (USE_LOG4CXX)

    if (MSVC)
        add_definitions(-DCURL_STATICLIB)
    endif()
elseif (LINK_STATIC AND VCPKG_TRIPLET)
    find_package(protobuf REQUIRED)
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

    if (USE_LOG4CXX)
        find_library(LOG4CXX_LIBRARY_PATH log4cxx)
        find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)
    endif (USE_LOG4CXX)
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

if (USE_LOG4CXX)
    set(CMAKE_CXX_FLAGS " -DUSE_LOG4CXX ${CMAKE_CXX_FLAGS}")
    find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)
endif (USE_LOG4CXX)

if (NOT APPLE AND NOT MSVC)
    # Hide all non-exported symbols to avoid conflicts
    add_compile_options(-fvisibility=hidden) 
    if (CMAKE_COMPILER_IS_GNUCC)
        add_compile_options(-Wl,--exclude-libs,ALL)
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

set(AUTOGEN_DIR ${PROJECT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${AUTOGEN_DIR})

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
  ${LOG4CXX_INCLUDE_PATH}
  ${GTEST_INCLUDE_PATH}
  ${GMOCK_INCLUDE_PATH}
)

set(COMMON_LIBS
  ${COMMON_LIBS}
  ${CMAKE_THREAD_LIBS_INIT}
  ${Boost_REGEX_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${Boost_DATE_TIME_LIBRARY}
  ${Protobuf_LIBRARIES}
  ${CURL_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${ADDITIONAL_LIBRARIES}
  ${CMAKE_DL_LIBS}
)

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
    add_subdirectory(tests)
endif()

if (BUILD_WIRESHARK)
    add_subdirectory(wireshark)
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
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/wireshark)

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

set(LIB_AUTOGEN_DIR ${AUTOGEN_DIR}/tests)
file(MAKE_DIRECTORY ${LIB_AUTOGEN_DIR})
include_directories(${LIB_AUTOGEN_DIR})

find_package(GTest CONFIG REQUIRED)
set(GTEST_TARGETS GTest::gtest GTest::gtest_main GTest::gmock GTest::gmock_main)

add_library(TESTS_PROTO_OBJECTS OBJECT
    ${CMAKE_CURRENT_SOURCE_DIR}/PaddingDemo.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/ExternalTest.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/Test.proto
    )
target_link_libraries(TESTS_PROTO_OBJECTS PUBLIC protobuf::libprotobuf)
protobuf_generate(
    TARGET TESTS_PROTO_OBJECTS
    IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/proto" "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOC_OUT_DIR ${LIB_AUTOGEN_DIR})

file(GLOB TEST_SOURCES *.cc c/*.cc)
add_executable(pulsar-tests ${TEST_SOURCES})
target_include_directories(pulsar-tests PRIVATE ${AUTOGEN_DIR}/lib)
target_compile_options(pulsar-tests PRIVATE -DTOKEN_PATH="${TOKEN_PATH}" -DTEST_CONF_DIR="${TEST_CONF_DIR}")
target_link_libraries(pulsar-tests PRIVATE pulsarStatic TESTS_PROTO_OBJECTS ${GTEST_TARGETS})

if (UNIX)
    add_executable(ConnectionFailTest unix/ConnectionFailTest.cc HttpHelper.cc)
    target_link_libraries(ConnectionFailTest pulsarStatic ${GTEST_TARGETS})
endif ()

add_executable(BrokerMetadataTest brokermetadata/BrokerMetadataTest.cc)
target_link_libraries(BrokerMetadataTest pulsarStatic ${GTEST_TARGETS})

add_executable(Oauth2Test oauth2/Oauth2Test.cc)
target_compile_options(Oauth2Test PRIVATE -DTEST_CONF_DIR="${TEST_CONF_DIR}")
target_link_libraries(Oauth2Test pulsarStatic ${GTEST_TARGETS})

add_executable(ChunkDedupTest chunkdedup/ChunkDedupTest.cc HttpHelper.cc)
target_link_libraries(ChunkDedupTest pulsarStatic ${GTEST_TARGETS})

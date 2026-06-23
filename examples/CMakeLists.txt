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

if (NOT MSVC)
  set(CMAKE_C_FLAGS " -std=c99 ${CMAKE_C_FLAGS}")
  add_compile_options(-Wextra -Wno-unused-parameter)
endif()

set(SAMPLE_ASYNC_PRODUCER_SOURCES
  SampleAsyncProducer.cc
)

set(SAMPLE_CONSUMER_SOURCES
  SampleConsumer.cc
)

set(SAMPLE_CONSUMER_LISTENER_SOURCES
  SampleConsumerListener.cc
)

set(SAMPLE_PRODUCER_SOURCES
  SampleProducer.cc
)

set(SAMPLE_FILE_LOGGER_SOURCES
  SampleFileLogger.cc
)

set(SAMPLE_PRODUCER_C_SOURCES
    SampleProducerCApi.c
)

set(SAMPLE_CONSUMER_C_SOURCES
    SampleConsumerCApi.c
)

set(SAMPLE_ASYNC_CONSUMER_C_SOURCES
    SampleAsyncConsumerCApi.c
)

set(SAMPLE_CONSUMER_LISTENER_C_SOURCES
    SampleConsumerListenerCApi.c
)

set(SAMPLE_READER_C_SOURCES
        SampleReaderCApi.c
)
set(SAMPLE_KEY_VALUE_SCHEMA_CONSUMER
        SampleKeyValueSchemaConsumer.cc
)

set(SAMPLE_KEY_VALUE_SCHEMA_PRODUCER
        SampleKeyValueSchemaProducer.cc
)

set(SAMPLE_CUSTOM_LOGGER_CAPI
        SampleCustomLoggerCApi.c
        )

add_executable(SampleAsyncProducer                    ${SAMPLE_ASYNC_PRODUCER_SOURCES})
add_executable(SampleConsumer                         ${SAMPLE_CONSUMER_SOURCES})
add_executable(SampleConsumerListener                 ${SAMPLE_CONSUMER_LISTENER_SOURCES})
add_executable(SampleProducer                         ${SAMPLE_PRODUCER_SOURCES})
add_executable(SampleFileLogger                       ${SAMPLE_FILE_LOGGER_SOURCES})
add_executable(SampleProducerCApi                     ${SAMPLE_PRODUCER_C_SOURCES})
add_executable(SampleConsumerCApi                     ${SAMPLE_CONSUMER_C_SOURCES})
add_executable(SampleAsyncConsumerCApi                ${SAMPLE_CONSUMER_LISTENER_C_SOURCES})
add_executable(SampleConsumerListenerCApi             ${SAMPLE_CONSUMER_LISTENER_C_SOURCES})
add_executable(SampleReaderCApi                       ${SAMPLE_READER_C_SOURCES})
add_executable(SampleKeyValueSchemaConsumer           ${SAMPLE_KEY_VALUE_SCHEMA_CONSUMER})
add_executable(SampleKeyValueSchemaProducer           ${SAMPLE_KEY_VALUE_SCHEMA_PRODUCER})
add_executable(SampleCustomLoggerCApi                 ${SAMPLE_CUSTOM_LOGGER_CAPI})

if (INTEGRATE_VCPKG)
    # pulsarShared already carries INCLUDE_DIRECTORIES and LINK_DIRECTORIES properties
    set(CLIENT_LIBS "")
endif ()

target_link_libraries(SampleAsyncProducer              ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleConsumer                   ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleConsumerListener           ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleProducer                   ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleFileLogger                 ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleProducerCApi               ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleConsumerCApi               ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleAsyncConsumerCApi          ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleConsumerListenerCApi       ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleReaderCApi                 ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleKeyValueSchemaConsumer     ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleKeyValueSchemaProducer     ${CLIENT_LIBS} pulsarShared)
target_link_libraries(SampleCustomLoggerCApi           ${CLIENT_LIBS} pulsarShared)

# --- Scalable topics (pulsar::st) examples ---------------------------------
# These use the new typed scalable-topics API under include/pulsar/st. Its
# implementation (lib/st) does not exist yet, so the examples are COMPILED here
# for header/API verification but are NOT linked into executables (there are no
# symbols to link against). Building this OBJECT library on every build keeps the
# examples from bit-rotting while the API is reviewed.
#
# TODO(scalable-topics): once lib/st lands, replace this with one
# add_executable + target_link_libraries(... pulsarShared) per file, exactly like
# the samples above.
# The core samples are header-only previews of the pulsar::st API and build with
# no extra dependency.
set(SAMPLE_ST_SOURCES
    st/SampleStProducer.cc
    st/SampleStStreamConsumer.cc
    st/SampleStQueueConsumer.cc
    st/SampleStCheckpointConsumer.cc
)
# reflect-cpp powers jsonSchema<T>() (reflection-based JSON SerDe + schema). It is
# optional for this API-only PR: when the package is present the JSON sample is
# added and linked against it; when absent, only that one sample is skipped. (The
# reflectcpp vcpkg port does not yet ship an Avro backend, so it is not yet wired
# into the manifest; it will be added with the lib/st implementation.)
find_package(reflectcpp CONFIG QUIET)
if (reflectcpp_FOUND)
    list(APPEND SAMPLE_ST_SOURCES st/SampleStJsonSchema.cc)
endif ()

add_library(StExamples OBJECT ${SAMPLE_ST_SOURCES})
# The scalable-topics (pulsar::st) API targets C++20; the rest of the client stays
# C++17. Set the standard per-target so only this code requires C++20.
set_target_properties(StExamples PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
# PRIVATE link gives the object sources pulsarShared's include directories; an
# OBJECT library is not itself linked, so the missing lib/st symbols are fine.
target_link_libraries(StExamples PRIVATE ${CLIENT_LIBS} pulsarShared)
if (reflectcpp_FOUND)
    target_link_libraries(StExamples PRIVATE reflectcpp::reflectcpp)
endif ()

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

file(GLOB PULSAR_SOURCES *.cc *.h lz4/*.cc lz4/*.h checksum/*.cc checksum/*.h stats/*.cc stats/*.h c/*.cc c/*.h auth/*.cc auth/*.h auth/athenz/*.cc auth/athenz/*.h)

if (NOT PROTOC_PATH)
    set(PROTOC_PATH protoc)
endif()

set(LIB_AUTOGEN_DIR ${AUTOGEN_DIR}/lib)
file(MAKE_DIRECTORY ${LIB_AUTOGEN_DIR})
include_directories(${LIB_AUTOGEN_DIR})

# Protobuf generation is only supported natively starting from CMake 3.8
# Using custom command for now
set(PROTO_SOURCES ${LIB_AUTOGEN_DIR}/PulsarApi.pb.cc ${LIB_AUTOGEN_DIR}/PulsarApi.pb.h)
set(PULSAR_SOURCES ${PULSAR_SOURCES} ${PROTO_SOURCES})
ADD_CUSTOM_COMMAND(
         OUTPUT ${PROTO_SOURCES}
         COMMAND ${PROTOC_PATH} -I ../proto ../proto/PulsarApi.proto --cpp_out=${LIB_AUTOGEN_DIR}
         DEPENDS
         ../proto/PulsarApi.proto
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(LIBRARY_VERSION $ENV{PULSAR_LIBRARY_VERSION})
if (NOT LIBRARY_VERSION)
    set(LIBRARY_VERSION ${PV})
endif(NOT LIBRARY_VERSION)

if (MSVC)
    find_package(dlfcn-win32 REQUIRED)
    set(CMAKE_DL_LIBS dlfcn-win32::dl psapi.lib)
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        get_target_property(dlfcn-win32_LIBRARY dlfcn-win32::dl IMPORTED_LOCATION_DEBUG)
    else ()
        get_target_property(dlfcn-win32_LIBRARY dlfcn-win32::dl IMPORTED_LOCATION_RELEASE)
    endif ()
    message(STATUS "dlfcn-win32_LIBRARY: " ${dlfcn-win32_LIBRARY})
endif(MSVC)


set(LIB_NAME_SHARED ${LIB_NAME})

# this is the "object library" target: compiles the sources only once
add_library(PULSAR_OBJECT_LIB OBJECT ${PULSAR_SOURCES})
set_property(TARGET PULSAR_OBJECT_LIB PROPERTY POSITION_INDEPENDENT_CODE 1)

if (BUILD_DYNAMIC_LIB)
    add_library(pulsarShared SHARED $<TARGET_OBJECTS:PULSAR_OBJECT_LIB>)
    set_property(TARGET pulsarShared PROPERTY OUTPUT_NAME ${LIB_NAME_SHARED})
    set_property(TARGET pulsarShared PROPERTY VERSION ${LIBRARY_VERSION})
    target_link_libraries(pulsarShared ${COMMON_LIBS} ${CMAKE_DL_LIBS})
    if (MSVC)
        target_include_directories(pulsarShared PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
        target_link_options(pulsarShared PRIVATE $<$<CONFIG:DEBUG>:/NODEFAULTLIB:MSVCRT>)
    endif()
endif()

include(CheckCXXSymbolExists)
check_cxx_symbol_exists(getauxval auvx.h HAVE_AUXV_GETAUXVAL)
if(HAVE_AUXV_GETAUXVAL)
    add_definitions(-DPULSAR_AUXV_GETAUXVAL_PRESENT)
endif()

if (BUILD_STATIC_LIB)
    add_library(pulsarStatic STATIC $<TARGET_OBJECTS:PULSAR_OBJECT_LIB>)
    if (MSVC)
        set_property(TARGET pulsarStatic PROPERTY OUTPUT_NAME "${LIB_NAME}-static")
        target_include_directories(pulsarStatic PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
    else ()
        set_property(TARGET pulsarStatic PROPERTY OUTPUT_NAME ${LIB_NAME})
    endif()
    set_property(TARGET pulsarStatic PROPERTY VERSION ${LIBRARY_VERSION})
    target_compile_definitions(pulsarStatic PRIVATE PULSAR_STATIC)
endif()

# When linking statically, install a libpulsar.a that contains all the
# required dependencies except ssl
if (LINK_STATIC AND BUILD_STATIC_LIB)
    if (MSVC)
        set(COMMON_LIBS ${COMMON_LIBS} ${dlfcn-win32_LIBRARY})
        # This function is to remove either "debug" or "optimized" library names
        # out of the COMMON_LIBS list and return the sanitized list of libraries
        function(remove_libtype LIBLIST LIBTYPE OUTLIST)
            list(FIND LIBLIST ${LIBTYPE} LIST_INDEX)
            while(${LIST_INDEX} GREATER -1)
                list(REMOVE_AT LIBLIST ${LIST_INDEX})
                list(REMOVE_AT LIBLIST ${LIST_INDEX})
                list(FIND LIBLIST ${LIBTYPE} LIST_INDEX)
            endwhile()
            list(REMOVE_ITEM LIBLIST "debug")
            list(REMOVE_ITEM LIBLIST "optimized")
            string(REPLACE ";" " " TEMP_OUT "${LIBLIST}")
            set(${OUTLIST} ${TEMP_OUT} PARENT_SCOPE)
        endfunction(remove_libtype)

        add_library(pulsarStaticWithDeps STATIC ${PULSAR_SOURCES})
        target_include_directories(pulsarStaticWithDeps PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
        remove_libtype("${COMMON_LIBS}" "optimized" DEBUG_STATIC_LIBS)
        remove_libtype("${COMMON_LIBS}" "debug" STATIC_LIBS)
        set_property(TARGET pulsarStaticWithDeps PROPERTY STATIC_LIBRARY_FLAGS_DEBUG ${DEBUG_STATIC_LIBS})
        set_property(TARGET pulsarStaticWithDeps PROPERTY STATIC_LIBRARY_FLAGS_RELEASE ${STATIC_LIBS})
        set_property(TARGET pulsarStaticWithDeps PROPERTY OUTPUT_NAME ${LIB_NAME}WithDeps)
        set_property(TARGET pulsarStaticWithDeps PROPERTY VERSION ${LIBRARY_VERSION})
        install(TARGETS pulsarStaticWithDeps DESTINATION lib)
    else()
        # Build a list of the requird .a libs (except ssl) to merge
        SET(STATIC_LIBS "")
        foreach (LIB IN LISTS COMMON_LIBS)
            if (${LIB} MATCHES ".+\\.a$")
                set(STATIC_LIBS "${STATIC_LIBS} ${LIB}")
            endif()
        endforeach()

        set(PULSAR_WITH_DEPS ${CMAKE_BINARY_DIR}/lib/libpulsarwithdeps.a)
        add_custom_target(pulsarStaticWithDeps
                ALL
                BYPRODUCTS merged-library
                COMMAND ./build-support/merge_archives.sh libpulsar.a $<TARGET_FILE:pulsarStatic> ${STATIC_LIBS} && mv merged-library/libpulsar.a ${PULSAR_WITH_DEPS}
                DEPENDS pulsarStatic
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
        install(PROGRAMS ${PULSAR_WITH_DEPS} DESTINATION lib)
    endif(MSVC)
elseif(BUILD_STATIC_LIB)
    # Install regular libpulsar.a
    target_link_libraries(pulsarStatic ${COMMON_LIBS})
    install(TARGETS pulsarStatic DESTINATION lib)
endif()

if (BUILD_STATIC_LIB)
    install(TARGETS pulsarStatic DESTINATION lib)
endif()

if (BUILD_DYNAMIC_LIB)
    install(TARGETS pulsarShared RUNTIME DESTINATION bin LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
endif()

install(DIRECTORY "../include/pulsar" DESTINATION include)

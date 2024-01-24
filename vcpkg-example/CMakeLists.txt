cmake_minimum_required(VERSION 3.15)

if (NOT CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_SOURCE_DIR}/../vcpkg/scripts/buildsystems/vcpkg.cmake")
endif ()
if (VCPKG_TARGET_TRIPLET MATCHES ".*windows-static")
    cmake_policy(SET CMP0091 NEW)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif ()
project(PulsarDemo CXX)

if (NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif ()

find_package(unofficial-pulsar CONFIG)
add_executable(main main.cc)
target_link_libraries(main PRIVATE unofficial::pulsar::pulsar)

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar C++ client library

Examples for using the API to publish and consume messages can be found under the [examples](https://github.com/apache/pulsar-client-cpp/tree/main/examples) folder.

## Generate the API documents

Pulsar C++ client uses [doxygen](https://www.doxygen.nl) to build API documents. After installing `doxygen`, you only need to run `doxygen` to generate the API documents whose main page is under the `doxygen/html/index.html` path.

## Requirements

* A C++ compiler that supports C++11, like GCC >= 4.8
* CMake >= 3.4
* [Boost](http://www.boost.org/)
* [Protocol Buffer](https://developers.google.com/protocol-buffers/) >= 3
* [libcurl](https://curl.se/libcurl/)
* [openssl](https://github.com/openssl/openssl)

The default supported [compression types](include/pulsar/CompressionType.h) are:

* `CompressionNone`
* `CompressionLZ4`

If you want to enable other compression types, you need to install:

* `CompressionZLib`: [zlib](https://zlib.net/)
* `CompressionZSTD`: [zstd](https://github.com/facebook/zstd)
* `CompressionSNAPPY`: [snappy](https://github.com/google/snappy)

If you want to build and run the tests, you need to install [GTest](https://github.com/google/googletest). Otherwise, you need to add CMake option `-DBUILD_TESTS=OFF`.

If you want to use `ClientConfiguration::setLogConfFilePath`, you need to install the [Log4CXX](https://logging.apache.org/log4cxx) and add CMake option `-DUSE_LOG4CXX=ON`.

## Platforms

Pulsar C++ Client Library has been tested on:

* Linux
* Mac OS X
* Windows x64

## Compilation

### Clone

First of all, clone the source code:

```shell
git clone https://github.com/apache/pulsar-client-cpp
cd pulsar-client-cpp
```

### Compile on Ubuntu

#### Install all dependencies:

```shell
sudo apt-get update -y && apt-get install -y g++ cmake libssl-dev libcurl4-openssl-dev \
                libprotobuf-dev libboost-all-dev libgtest-dev libgmock-dev \
                protobuf-compiler
```

#### Compile Pulsar client library:

```shell
cmake .
make
```

#### Checks

Client library will be placed in:

```
lib/libpulsar.so
lib/libpulsar.a
```

Tools will be placed in:

```
perf/perfProducer
perf/perfConsumer
```

### Compile on Mac OS X

#### Install all dependencies:

```shell
brew install openssl protobuf boost boost-python3 googletest zstd snappy
```

#### Compile Pulsar client library:

```shell
cmake .
make
```

#### Checks

Client library will be placed in:

```
lib/libpulsar.dylib
lib/libpulsar.a
```

Tools will be placed in:

```
perf/perfProducer
perf/perfConsumer
```

### Compile on Windows

#### Install with [vcpkg](https://github.com/microsoft/vcpkg)

It's highly recommended to use `vcpkg` for C++ package management on Windows. It's easy to install and well supported by Visual Studio (2015/2017/2019) and CMake. See [here](https://github.com/microsoft/vcpkg#quick-start-windows) for quick start.

Take Windows 64-bit library as an example, you only need to run

```bash
vcpkg install --feature-flags=manifests --triplet x64-windows
```

> NOTE: For Windows 32-bit library, change `x64-windows` to `x86-windows`, see [here](https://github.com/microsoft/vcpkg/blob/master/docs/users/triplets.md) for more details about the triplet concept in Vcpkg.

The all dependencies, which are specified by [vcpkg.json](vcpkg.json), will be installed in `vcpkg_installed/` subdirectory,

With `vcpkg`, you only need to run two commands:

```bash
cmake \
 -B ./build \
 -A x64 \
 -DBUILD_TESTS=OFF \
 -DVCPKG_TRIPLET=x64-windows \
 -DCMAKE_BUILD_TYPE=Release \
 -S .
cmake --build ./build --config Release
```

Then all artifacts will be built into `build` subdirectory.

> **NOTE**:
>
> 1. For Windows 32-bit, you need to use `-A Win32` and `-DVCPKG_TRIPLET=x86-windows`.
> 2. For MSVC Debug mode, you need to replace `Release` with `Debug` for both `CMAKE_BUILD_TYPE` variable and `--config` option.

#### Install dependencies manually

You need to install [dlfcn-win32](https://github.com/dlfcn-win32/dlfcn-win32) in addition.

If you installed the dependencies manually, you need to run

```shell
#If all dependencies are in your path, all that is necessary is
cmake .

#if all dependencies are not in your path, then passing in a PROTOC_PATH and CMAKE_PREFIX_PATH is necessary
cmake -DPROTOC_PATH=C:/protobuf/bin/protoc -DCMAKE_PREFIX_PATH="C:/boost;C:/openssl;C:/zlib;C:/curl;C:/protobuf;C:/googletest;C:/dlfcn-win32" .

#This will generate pulsar-cpp.sln. Open this in Visual Studio and build the desired configurations.
```

#### Checks

Client library will be placed in:

```
build/lib/Release/pulsar.lib
build/lib/Release/pulsar.dll
```

#### Examples

Add windows environment paths:

```
build/lib/Release
vcpkg_installed
```

Examples will be available in:

```
build/examples/Release
```

## Tests

```shell
# Execution
# Start standalone broker
./pulsar-test-service-start.sh

# Run the tests
cd tests
./pulsar-tests

# When no longer needed, stop standalone broker
./pulsar-test-service-stop.sh
```

## Requirements for Contributors

It's required to install [LLVM](https://llvm.org/builds/) for `clang-tidy` and `clang-format`. Pulsar C++ client use `clang-format` **11** to format files.  `make format` automatically formats the files.

For Ubuntu users, you can install `clang-format-11` via `apt install clang-format-11`. For other users, run `./build-support/docker-format.sh` if you have Docker installed.

We welcome contributions from the open source community, kindly make sure your changes are backward compatible with GCC 4.8 and Boost 1.53.

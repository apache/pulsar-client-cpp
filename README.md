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

Pulsar C++ clients support a variety of Pulsar features to enable building applications connecting to your Pulsar cluster.

For the supported Pulsar features, see [Client Feature Matrix](https://pulsar.apache.org/client-feature-matrix/).

For how to use APIs to publish and consume messages, see [examples](https://github.com/apache/pulsar-client-cpp/tree/main/examples).

## Generate the API documents

Pulsar C++ client uses [doxygen](https://www.doxygen.nl) to build API documents. After installing `doxygen`, you only need to run `doxygen` to generate the API documents whose main page is under the `doxygen/html/index.html` path.

## Build with vcpkg

Since it's integrated with vcpkg, see [vcpkg#README](https://github.com/microsoft/vcpkg#readme) for the requirements. See [LEGACY_BUILD](./LEGACY_BUILD.md) if you want to manage dependencies by yourself or you cannot install vcpkg in your own environment.

### How to build from source

```bash
git clone https://github.com/apache/pulsar-client-cpp.git
cd pulsar-client-cpp
git submodule update --init --recursive
cmake -B build -DINTEGRATE_VCPKG=ON
cmake --build build -j8
```

The 1st step will download vcpkg and then install all dependencies according to the version description in [vcpkg.json](./vcpkg.json). The 2nd step will build the Pulsar C++ libraries under `./build/lib/`, where `./build` is the CMake build directory.

After the build, the hierarchy of the `build` directory will be:

```
build/
  include/   -- extra C++ headers
  lib/       -- libraries
  tests/     -- test executables
  examples/  -- example executables
  generated/
    lib/     -- protobuf source files for PulsarApi.proto
    tests/   -- protobuf source files for *.proto used in tests
```

### How to install

To install the C++ headers and libraries into a specific path, e.g. `/tmp/pulsar`, run the following commands:

```bash
cmake -B build -DINTEGRATE_VCPKG=ON -DCMAKE_INSTALL_PREFIX=/tmp/pulsar
cmake --build build -j8 --target install
```

For example, on macOS you will see:

```
/tmp/pulsar/
  include/pulsar     -- C/C++ headers
  lib/
    libpulsar.a      -- Static library
    libpulsar.dylib  -- Dynamic library
```

### Tests

Tests are built by default. You should execute [run-unit-tests.sh](./run-unit-tests.sh) to run tests locally.

If you don't want to build the tests, disable the `BUILD_TESTS` option:

```bash
cmake -B build -DINTEGRATE_VCPKG=ON -DBUILD_TESTS=OFF
cmake --build build -j8
```

### Build perf tools

If you want to build the perf tools, enable the `BUILD_PERF_TOOLS` option:

```bash
cmake -B build -DINTEGRATE_VCPKG=ON -DBUILD_PERF_TOOLS=ON
cmake --build build -j8
```

Then the perf tools will be built under `./build/perf/`.

## Platforms

Pulsar C++ Client Library has been tested on:

- Linux
- Mac OS X
- Windows x64

## Wireshark Dissector

See the [wireshark](wireshark/) directory for details.

## Requirements for Contributors

It's required to install [LLVM](https://llvm.org/builds/) for `clang-tidy` and `clang-format`. Pulsar C++ client use `clang-format` **11** to format files. `make format` automatically formats the files.

For Ubuntu users, you can install `clang-format-11` via `apt install clang-format-11`. For other users, run `./build-support/docker-format.sh` if you have Docker installed.

We welcome contributions from the open source community, kindly make sure your changes are backward compatible with GCC 4.8 and Boost 1.53.

If your contribution adds Pulsar features for C++ clients, you need to update both the [Pulsar docs](https://pulsar.apache.org/docs/client-libraries/) and the [Client Feature Matrix](https://pulsar.apache.org/client-feature-matrix/). See [Contribution Guide](https://pulsar.apache.org/contribute/site-intro/#pages) for more details.

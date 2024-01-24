# vcpkg-example

A simple example vcpkg project that imported pulsar-client-cpp 3.4.2 as the dependency.

## How to build

Before running the commands below, ensure the vcpkg has been installed. If you have already downloaded the submodule of this project, just run the following commands. If you want to specify an existing vcpkg installation directory (assuming it's `VCPKG_ROOT`), add the `-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake` option to the 1st command.

```bash
cmake -B build
cmake --build build
```

Then the `main` executable will be generated under `./build` for single-configuration generators or `build/Debug` for multi-configuration generators.

## How to link release libraries

By default, debug libraries are linked so that the executable can be debugged by debuggers. However, when used for production, release libraries should be linked for better performance.

See [cmake-generators](https://cmake.org/cmake/help/latest/manual/cmake-generators.7.html) for the concept of CMake Generator.

### Single-configuration generator

With single-configuration generators like Unix Makefiles on Linux and macOS, you need to specify the `CMAKE_BUILD_TYPE`.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

The `main` executable will still be generated under the `./build` directory but release libraries are linked now.

### Multi-configuration generator

With multi-configuration generators like Visual Studio on Windows, you just need to specify the `--config` option.

```bash
cmake -B build
cmake --build build --config Release
```

The `main` executable that links release libraries will be generated under the `./build/Release` directory.

## Link static libraries on Windows

The default vcpkg triplet is `x64-windows` on Windows. If you changed the triplet for static libraries, like `x64-windows-static`, you need to add a line before the `project(PulsarDemo CXX)` in CMakeLists.txt.

```cmake
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
```

See [CMAKE_MSVC_RUNTIME_LIBRARY](https://cmake.org/cmake/help/latest/variable/CMAKE_MSVC_RUNTIME_LIBRARY.html) for details.

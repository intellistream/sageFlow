# Third-Party Dependencies

This directory manages external dependencies using CMake's FetchContent module instead of embedding source code directly.

## Dependencies

- **argparse** (v3.1): Command line argument parser for C++
- **fmt** (11.0.2): A modern formatting library
- **googletest** (v1.15.2): Google's C++ testing framework
- **spdlog** (v1.15.0): Fast C++ logging library
- **tomlplusplus** (v3.4.0): Header-only TOML parser and serializer

## Migration from Embedded Source Code

The third-party dependencies have been migrated from embedded source code to FetchContent-based downloads. This provides several benefits:

1. **No source code maintenance**: We don't need to fix linting issues in third-party code
2. **Better version control**: Easier to update to specific versions
3. **Cleaner repository**: Smaller repository size without embedded dependencies
4. **Consistent builds**: Dependencies are fetched from official sources

## Usage

The dependencies are automatically fetched and built when you run CMake configuration:

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

All dependencies are linked through the `externalLibs` interface library, and individual components can also be linked directly using their proper namespaced targets:

- `argparse`
- `fmt::fmt`
- `spdlog::spdlog`
- `tomlplusplus::tomlplusplus`
- `GTest::gtest`
- `GTest::gtest_main`

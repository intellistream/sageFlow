#!/bin/bash
set -e

BUILD_TYPE=${BUILD_TYPE:-Debug}

echo "Building sageFlow with CMake (CMAKE_BUILD_TYPE=${BUILD_TYPE})..."

# Create build directory if not exists
mkdir -p build

# Configure with CMake (default to Debug for easier debugging; override via BUILD_TYPE env)
cmake_args=(
	-DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
)
if [[ -n "${SAGE_COMMON_DEPS_FILE:-}" ]]; then
	cmake_args+=(-DSAGE_COMMON_DEPS_FILE="${SAGE_COMMON_DEPS_FILE}")
fi
if [[ -n "${SAGE_ENABLE_GPERFTOOLS:-}" ]]; then
	cmake_args+=(-DSAGE_ENABLE_GPERFTOOLS="${SAGE_ENABLE_GPERFTOOLS}")
fi
if [[ -n "${SAGE_PYBIND11_VERSION:-}" ]]; then
	cmake_args+=(-DSAGE_PYBIND11_VERSION="${SAGE_PYBIND11_VERSION}")
fi
if [[ -n "${SAGE_GPERFTOOLS_ROOT:-}" ]]; then
	cmake_args+=(-DSAGE_GPERFTOOLS_ROOT="${SAGE_GPERFTOOLS_ROOT}")
fi

cmake -B build "${cmake_args[@]}"

# Build
cmake --build build -j "$(nproc)"

echo "sageFlow build completed."
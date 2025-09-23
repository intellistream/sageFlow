#!/bin/bash
set -e

echo "Building candyFlow with CMake..."

# Create build directory if not exists
mkdir -p build

# Configure with CMake
cmake -B build

# Build
cmake --build build -j $(nproc)

echo "candyFlow build completed."
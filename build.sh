#!/bin/bash
set -e

echo "Building sageFlow with CMake..."

# Create build directory if not exists
mkdir -p build

# Configure with CMake
cmake -B build

# Build
cmake --build build -j $(nproc)

echo "sageFlow build completed."
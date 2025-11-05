#!/bin/bash
set -e

BUILD_TYPE=${BUILD_TYPE:-Debug}

echo "Building sageFlow with CMake (CMAKE_BUILD_TYPE=${BUILD_TYPE})..."

# Function to check and fix libstdc++ version issue in conda environment
check_libstdcxx() {
    # Only check if we're in a conda environment
    if [[ -z "${CONDA_PREFIX}" ]]; then
        return 0
    fi
    
    # Check if conda libstdc++ needs update
    local conda_libstdcxx="${CONDA_PREFIX}/lib/libstdc++.so.6"
    if [[ ! -f "${conda_libstdcxx}" ]]; then
        return 0
    fi
    
    # Check GCC version requirement
    local gcc_version=$(gcc -dumpversion | cut -d. -f1)
    if [[ ${gcc_version} -ge 11 ]]; then
        # Check if conda libstdc++ has required GLIBCXX version
        if ! strings "${conda_libstdcxx}" | grep -q "GLIBCXX_3.4.30"; then
            echo "⚠️  检测到conda环境中的libstdc++版本过低，正在更新..."
            echo "   这是C++20/GCC 11+编译所必需的"
            
            # Try to update libstdc++ in conda environment
            if command -v conda &> /dev/null; then
                conda install -c conda-forge libstdcxx-ng -y || {
                    echo "⚠️  无法自动更新libstdc++，将使用系统版本"
                    # Set LD_LIBRARY_PATH to prefer system libstdc++
                    if [[ -f "/usr/lib/x86_64-linux-gnu/libstdc++.so.6" ]]; then
                        export LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH}"
                        echo "   已设置LD_LIBRARY_PATH优先使用系统libstdc++"
                    fi
                }
            fi
        fi
    fi
}

# Check libstdc++ before building
check_libstdcxx

# 确定构建目录：优先使用 .sage/build/sage_flow（统一构建目录）
# 如果在 middleware 上下文中构建，会由父 CMake 管理
# 如果独立构建（开发/测试），则使用本地 build/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 路径: sageFlow -> sage_flow -> components -> middleware -> sage -> src -> sage-middleware -> packages -> SAGE
SAGE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../../../.." && pwd)"

if [[ -d "${SAGE_ROOT}/.sage" ]]; then
    # 在 SAGE 项目根目录下，使用统一构建目录
    BUILD_DIR="${SAGE_ROOT}/.sage/build/sage_flow"
    echo "使用统一构建目录: ${BUILD_DIR}"
else
    # 独立构建（子模块开发模式）
    BUILD_DIR="${SCRIPT_DIR}/build"
    echo "使用本地构建目录: ${BUILD_DIR}"
fi

# Create build directory if not exists
mkdir -p "${BUILD_DIR}"

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

cmake -B "${BUILD_DIR}" -S "${SCRIPT_DIR}" "${cmake_args[@]}"

# Build
cmake --build "${BUILD_DIR}" -j "$(nproc)"

echo "sageFlow build completed."
cmake_minimum_required(VERSION 3.20)

project(CANDY CXX)

set(CMAKE_CXX_STANDARD 20)

# 是否启用运行时指标采集代码（通过编译宏控制）。默认开启以保持现有行为。
option(CANDY_ENABLE_METRICS "Enable metrics instrumentation in join/operator and tests" ON)
message(STATUS "CANDY_ENABLE_METRICS: ${CANDY_ENABLE_METRICS}")

# 启用测试选项（CLion 识别 gtest 必需）
option(BUILD_TESTING "Build tests" ON)
if(BUILD_TESTING)
    enable_testing()
endif()

include(cmake/macros.cmake)
include(cmake/gperftools.cmake)

# 先构建第三方库，确保 gtest/gtest_main 目标可用
add_subdirectory(third-party)

# 再引入 add_gtest 宏（需要已存在的 gtest 目标）
include(cmake/enableTest.cmake)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(
    CMAKE_CXX_FLAGS
    "-Wall -Werror=return-type -Wno-error=unused-variable -Wno-error=unused-parameter -fPIC"
)

add_compile_definitions(SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO) 

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNO_RACE_CHECK")
set(CMAKE_CXX_FLAGS_RELEASE "-Wno-ignored-qualifiers -Wno-sign-compare -O3")
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})


include_directories(
    include
    ${CMAKE_SOURCE_DIR}/third-party/spdlog/include
)

add_definitions(
    -DPROJECT_DIR="${CMAKE_SOURCE_DIR}"
)

if(ENABLE_GPERFTOOLS)
    add_definitions(-DENABLE_GPERFTOOLS)
endif()

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)

# Python bindings
option(CANDY_BUILD_PYBIND "Build Python bindings (_sage_flow)" ON)
message(STATUS "CANDY_BUILD_PYBIND: ${CANDY_BUILD_PYBIND}")
if(CANDY_BUILD_PYBIND)
    if(POLICY CMP0148)
        # Suppress pybind11's use of deprecated FindPython modules warning on newer CMake
        cmake_policy(SET CMP0148 OLD)
    endif()
    include(FetchContent)
    FetchContent_Declare(
        pybind11
        GIT_REPOSITORY https://github.com/pybind/pybind11.git
        GIT_TAG v2.11.1
    )
    FetchContent_MakeAvailable(pybind11)

    pybind11_add_module(_sage_flow python/bindings.cpp)
    target_link_libraries(_sage_flow PRIVATE
        common
        stream
        function
        operator
        index
        concurrency
        storage
        compute_engine
        utils
        spdlog::spdlog
        fmt::fmt
    )
    target_include_directories(_sage_flow PRIVATE include)
endif()

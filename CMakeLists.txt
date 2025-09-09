cmake_minimum_required(VERSION 3.20)

project(CANDY CXX)

set(CMAKE_CXX_STANDARD 20)

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
    "-Wall -Werror=return-type -Wno-error=unused-variable -Wno-error=unused-parameter"
)

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

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)
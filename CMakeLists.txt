cmake_minimum_required(VERSION 3.20)

project(CANDY CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 是否启用运行时指标采集代码（通过编译宏控制）。默认开启以保持现有行为。
option(CANDY_ENABLE_METRICS "Enable metrics instrumentation in join/operator and tests" ON)
message(STATUS "CANDY_ENABLE_METRICS: ${CANDY_ENABLE_METRICS}")

# 启用测试选项（CLion 识别 gtest 必需）
option(BUILD_TESTING "Build tests" ON)
if(BUILD_TESTING)
    enable_testing()
endif()


set(_sage_flow_shared_deps FALSE)
if(DEFINED SAGE_COMMON_DEPS_FILE AND EXISTS "${SAGE_COMMON_DEPS_FILE}")
    include("${SAGE_COMMON_DEPS_FILE}")
    set(_sage_flow_shared_deps TRUE)
endif()

set(_sage_flow_enable_gperftools_default OFF)
if(DEFINED SAGE_ENABLE_GPERFTOOLS)
    set(_sage_flow_enable_gperftools_default ${SAGE_ENABLE_GPERFTOOLS})
endif()
option(ENABLE_GPERFTOOLS "Enable gperftools profiling support" ${_sage_flow_enable_gperftools_default})
if(DEFINED SAGE_ENABLE_GPERFTOOLS)
    set(ENABLE_GPERFTOOLS ${SAGE_ENABLE_GPERFTOOLS} CACHE BOOL "Enable gperftools profiling support" FORCE)
endif()

include(cmake/macros.cmake)

if(ENABLE_GPERFTOOLS)
    if(NOT _sage_flow_shared_deps OR NOT (DEFINED SAGE_GPERFTOOLS_LIBS AND SAGE_GPERFTOOLS_LIBS))
        include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/gperftools.cmake)
    endif()
    if(NOT (DEFINED SAGE_GPERFTOOLS_LIBS AND SAGE_GPERFTOOLS_LIBS))
        message(WARNING "ENABLE_GPERFTOOLS requested but gperftools libraries were not found; disabling support")
        set(ENABLE_GPERFTOOLS OFF CACHE BOOL "Enable gperftools profiling support" FORCE)
    endif()
endif()

# 先构建第三方库，确保 gtest/gtest_main 目标可用
add_subdirectory(third-party)

include(cmake/enableTest.cmake)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(
    CMAKE_CXX_FLAGS
    "-Wall -Werror=return-type -Wno-error=unused-variable -Wno-error=unused-parameter -fPIC"
)

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNO_RACE_CHECK")
set(CMAKE_CXX_FLAGS_RELEASE "-Wno-ignored-qualifiers -Wno-sign-compare -O3")
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})


include_directories(
    include
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
if(NOT _sage_flow_shared_deps)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/pybind11_dependency.cmake)
endif()

pybind11_add_module(_sage_flow python/bindings.cpp)
target_link_libraries(_sage_flow PRIVATE
    candy
    externalRuntimeLibs
)
target_include_directories(_sage_flow PRIVATE include)
if(DEFINED SAGE_COMMON_COMPILE_DEFINITIONS)
    target_compile_definitions(_sage_flow PRIVATE ${SAGE_COMMON_COMPILE_DEFINITIONS})
else()
    target_compile_definitions(_sage_flow PRIVATE PYBIND11_INTERNALS_ID="sage_pybind11_shared")
endif()

# Reduce exported symbol surface to minimize potential cross-module clashes
if(DEFINED SAGE_COMMON_COMPILE_OPTIONS)
    target_compile_options(_sage_flow PRIVATE ${SAGE_COMMON_COMPILE_OPTIONS})
else()
    target_compile_options(_sage_flow PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
endif()

if(ENABLE_GPERFTOOLS AND DEFINED SAGE_GPERFTOOLS_LIBS AND SAGE_GPERFTOOLS_LIBS)
    target_link_libraries(_sage_flow PRIVATE ${SAGE_GPERFTOOLS_LIBS})
endif()
set_target_properties(_sage_flow PROPERTIES CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)



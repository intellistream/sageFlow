cmake_minimum_required(VERSION 3.20)

project(CANDY CXX)

set(CMAKE_CXX_STANDARD 20)

include(cmake/macros.cmake)
include(cmake/gperftools.cmake)
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

add_subdirectory(third-party)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)

# Python bindings
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


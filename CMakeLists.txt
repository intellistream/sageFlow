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
    "-Wall -Werror=return-type -Wno-error=unused-variable -Wno-error=unused-parameter"
)

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNO_RACE_CHECK")
set(CMAKE_CXX_FLAGS_RELEASE "-Wno-ignored-qualifiers -Wno-sign-compare -O3")
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

find_package(Protobuf REQUIRED)

include_directories(
    include
    ${Protobuf_INCLUDE_DIRS}
)

add_definitions(
    -DPROJECT_DIR="${CMAKE_SOURCE_DIR}"
)

add_subdirectory(third-party)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS "${CMAKE_SOURCE_DIR}/third-praty/proto/message.proto")
# include(${PROTO_HDRS})
link_libraries(${Protobuf_LIBRARIES})
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)
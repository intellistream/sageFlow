# Fetch third-party dependencies using FetchContent
include(FetchContent)

# argparse
FetchContent_Declare(
    argparse
    GIT_REPOSITORY https://github.com/p-ranav/argparse.git
    GIT_TAG        v3.1
)

# fmt
FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG        11.0.2
)

# googletest
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
)

# spdlog
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.15.0
)

# tomlplusplus
FetchContent_Declare(
    tomlplusplus
    GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
    GIT_TAG        v3.4.0
)

# Configure options before making available
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(BUILD_GTEST ON CACHE BOOL "" FORCE)
set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)

# Make all dependencies available
FetchContent_MakeAvailable(argparse fmt googletest spdlog tomlplusplus)

# Retrieve source/include directories for header-only dependencies
FetchContent_GetProperties(argparse)
FetchContent_GetProperties(fmt)
FetchContent_GetProperties(spdlog)
FetchContent_GetProperties(tomlplusplus)

# Create interface libraries for external dependencies
add_library(externalRuntimeLibs INTERFACE)
target_link_libraries(
    externalRuntimeLibs INTERFACE
    argparse
    fmt::fmt-header-only
    spdlog::spdlog_header_only
    tomlplusplus::tomlplusplus
)
target_compile_definitions(
    externalRuntimeLibs INTERFACE
    SPDLOG_FMT_EXTERNAL=1
    FMT_HEADER_ONLY=1
)
set(_external_runtime_include_dirs "")
if(argparse_SOURCE_DIR)
    list(APPEND _external_runtime_include_dirs ${argparse_SOURCE_DIR}/include)
endif()
if(fmt_SOURCE_DIR)
    list(APPEND _external_runtime_include_dirs ${fmt_SOURCE_DIR}/include)
endif()
if(spdlog_SOURCE_DIR)
    list(APPEND _external_runtime_include_dirs ${spdlog_SOURCE_DIR}/include)
endif()
if(tomlplusplus_SOURCE_DIR)
    list(APPEND _external_runtime_include_dirs ${tomlplusplus_SOURCE_DIR}/include)
endif()

if(_external_runtime_include_dirs)
    list(REMOVE_DUPLICATES _external_runtime_include_dirs)
    message(STATUS "externalRuntimeLibs include dirs: ${_external_runtime_include_dirs}")
    target_include_directories(
        externalRuntimeLibs INTERFACE
        ${_external_runtime_include_dirs}
    )
endif()

add_library(externalTestLibs INTERFACE)
target_link_libraries(
    externalTestLibs INTERFACE
    GTest::gtest
    GTest::gtest_main
)
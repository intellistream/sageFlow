# Fetch third-party dependencies using FetchContent
include(FetchContent)

# Dependency declarations -------------------------------------------------
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

# Configure upstream projects before fetching --------------------------------
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(BUILD_GTEST ON CACHE BOOL "" FORCE)
set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)

# Fetch and build third-party projects ---------------------------------------
FetchContent_MakeAvailable(argparse fmt googletest spdlog tomlplusplus)

# Ensure compiled libraries are position independent (shared objects, pybind11)
foreach(_pic_target fmt spdlog)
    if(TARGET ${_pic_target})
        set_target_properties(${_pic_target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    endif()
endforeach()

# Collect include directories exposed transitively through our interface libs
set(_external_runtime_include_dirs "")
foreach(_dep IN ITEMS argparse fmt spdlog tomlplusplus)
    FetchContent_GetProperties(${_dep})
    if(${_dep}_SOURCE_DIR AND EXISTS "${${_dep}_SOURCE_DIR}/include")
        list(APPEND _external_runtime_include_dirs "${${_dep}_SOURCE_DIR}/include")
    endif()
endforeach()
if(_external_runtime_include_dirs)
    list(REMOVE_DUPLICATES _external_runtime_include_dirs)
    message(STATUS "externalRuntimeLibs include dirs: ${_external_runtime_include_dirs}")
endif()

# Interface library consumed by runtime components ---------------------------
add_library(externalRuntimeLibs INTERFACE)
target_link_libraries(
    externalRuntimeLibs INTERFACE
    argparse
    fmt::fmt
    spdlog::spdlog
    tomlplusplus::tomlplusplus
)
target_compile_definitions(
    externalRuntimeLibs INTERFACE
    SPDLOG_FMT_EXTERNAL=1
)
if(_external_runtime_include_dirs)
    target_include_directories(
        externalRuntimeLibs INTERFACE
        ${_external_runtime_include_dirs}
    )
endif()

# Interface library consumed by tests ----------------------------------------
add_library(externalTestLibs INTERFACE)
target_link_libraries(
    externalTestLibs INTERFACE
    GTest::gtest
    GTest::gtest_main
)
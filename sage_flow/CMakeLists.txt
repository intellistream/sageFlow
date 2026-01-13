cmake_minimum_required(VERSION 3.15)

# Find Python - use Development.Module for wheel builds (not Development.Embed)
find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)

# Find or fetch pybind11 (check if parent project already provided it)
if(NOT TARGET pybind11::module)
    find_package(pybind11 CONFIG QUIET)
    
    if(NOT pybind11_FOUND)
        message(STATUS "pybind11 not found, fetching from GitHub...")
        include(FetchContent)
        FetchContent_Declare(
            pybind11
            GIT_REPOSITORY https://github.com/pybind/pybind11.git
            GIT_TAG v2.13.0
        )
        FetchContent_MakeAvailable(pybind11)
    endif()
else()
    message(STATUS "Using pybind11 provided by parent project")
endif()

# Create Python bindings module
pybind11_add_module(_sage_flow bindings.cpp)

# Link against unified SAGE Flow library
target_link_libraries(_sage_flow PRIVATE sageflow)

# Include directories
target_include_directories(_sage_flow PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

# Add build-time dependencies include directories
if(DEFINED tomlplusplus_SOURCE_DIR)
    target_include_directories(_sage_flow SYSTEM PRIVATE ${tomlplusplus_SOURCE_DIR}/include)
endif()
if(DEFINED fmt_SOURCE_DIR)
    target_include_directories(_sage_flow SYSTEM PRIVATE ${fmt_SOURCE_DIR}/include)
endif()
if(DEFINED spdlog_SOURCE_DIR)
    target_include_directories(_sage_flow SYSTEM PRIVATE ${spdlog_SOURCE_DIR}/include)
endif()

# Set module properties
set_target_properties(_sage_flow PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    PREFIX "${PYTHON_MODULE_PREFIX}"
    SUFFIX "${PYTHON_MODULE_EXTENSION}"
    BUILD_RPATH_USE_ORIGIN TRUE
    INSTALL_RPATH "$ORIGIN"
    INSTALL_RPATH_USE_LINK_PATH TRUE
)

# Install the module
if(DEFINED SKBUILD)
    # Wheel build: install to wheel platlib/sage_flow/
    set(_install_dest "${SKBUILD_PLATLIB_DIR}/sage_flow")
    message(STATUS "_sage_flow module: Wheel build, install to ${_install_dest}")
    
    install(TARGETS _sage_flow
        LIBRARY DESTINATION ${_install_dest}
        COMPONENT sage_flow
    )
    
    # Also install libsageflow.so to the same directory
    install(TARGETS sageflow
        LIBRARY DESTINATION ${_install_dest}
        COMPONENT sage_flow
    )
else()
    # Standalone C++ project build
    install(TARGETS _sage_flow
        LIBRARY DESTINATION sage_flow
        COMPONENT sage_flow
    )
endif()

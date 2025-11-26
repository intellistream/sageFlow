add_subdirectory(
        utils
)
add_subdirectory(
        index
)
add_subdirectory(
        common
)
add_subdirectory(
        compute_engine
)
add_subdirectory(
        concurrency
)
add_subdirectory(
        execution
)
add_subdirectory(
        function
)
add_subdirectory(
        operator
)
add_subdirectory(
        query
)
add_subdirectory(
       storage
)
add_subdirectory(
        state
)
add_subdirectory(
        stream
)

# 收集所有源文件生成动态库（像 sageDB 一样）
file(GLOB_RECURSE SAGEFLOW_SOURCES
    utils/*.cpp
    index/*.cpp
    common/*.cpp
    compute_engine/*.cpp
    concurrency/*.cpp
    execution/*.cpp
    function/*.cpp
    operator/*.cpp
    query/*.cpp
    storage/*.cpp
    state/*.cpp
    stream/*.cpp
)

# 排除测试和示例文件
list(FILTER SAGEFLOW_SOURCES EXCLUDE REGEX ".*test.*")
list(FILTER SAGEFLOW_SOURCES EXCLUDE REGEX ".*example.*")
# 由 stream_runtime 单独构建，避免与 stream/optimizer 的依赖循环
list(FILTER SAGEFLOW_SOURCES EXCLUDE REGEX ".*/stream/stream_environment\\.cpp$")

# 创建动态库，包含 stream_runtime 的对象文件以避免链接问题
add_library(sageflow SHARED ${SAGEFLOW_SOURCES} $<TARGET_OBJECTS:stream_runtime>)

target_link_libraries(
        sageflow
        PUBLIC
        externalRuntimeLibs
        stream_runtime  # Link dependencies of stream_runtime
)
if(ENABLE_GPERFTOOLS AND DEFINED SAGE_GPERFTOOLS_LIBS AND SAGE_GPERFTOOLS_LIBS)
        target_link_libraries(
                sageflow
                PRIVATE
                ${SAGE_GPERFTOOLS_LIBS}
        )
endif()

# 安装动态库
# 条件化安装：支持 SKBUILD (Python 包构建) 和独立 C++ 构建
if(DEFINED SKBUILD)
    # 判断是否是 editable install
    # SKBUILD_STATE 可能是 "editable" 或 "wheel" 或未定义
    # 如果未定义或不是 "wheel"，默认按 editable 处理
    set(_is_editable TRUE)
    if(DEFINED SKBUILD_STATE)
        if(SKBUILD_STATE STREQUAL "wheel")
            set(_is_editable FALSE)
        endif()
    endif()
    
    if(_is_editable)
        # Editable install: install to source tree directly
        # 注意：这个 CMakeLists.txt 在 sageFlow/src 目录下
        # 需要回到 sageFlow/../python 目录（即 sage_flow/python）
        set(_lib_install_dest "${CMAKE_CURRENT_SOURCE_DIR}/../../python")
        message(STATUS "sageFlow: Editable install, lib -> ${_lib_install_dest}")
    else()
        # Wheel build: install to wheel platlib (same directory as Python extension)
        set(_lib_install_dest "${SKBUILD_PLATLIB_DIR}/sage/middleware/components/sage_flow/python")
        message(STATUS "sageFlow: Wheel build, lib -> ${_lib_install_dest}")
    endif()
    
    install(TARGETS sageflow
            LIBRARY DESTINATION ${_lib_install_dest}
            ARCHIVE DESTINATION ${_lib_install_dest}
            RUNTIME DESTINATION ${_lib_install_dest}
            COMPONENT python
    )
else()
    # 独立 C++ 构建模式：标准安装路径
    install(TARGETS sageflow
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            RUNTIME DESTINATION bin
    )
endif()
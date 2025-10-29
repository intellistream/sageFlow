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
    stream/*.cpp
)

# 排除测试和示例文件
list(FILTER SAGEFLOW_SOURCES EXCLUDE REGEX ".*test.*")
list(FILTER SAGEFLOW_SOURCES EXCLUDE REGEX ".*example.*")

# 创建动态库
add_library(sageflow SHARED ${SAGEFLOW_SOURCES})

target_link_libraries(
        sageflow
        PUBLIC
        externalRuntimeLibs
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
if(DEFINED SKBUILD_PLATLIB_DIR)
    # Python 包构建模式：安装到源码树 (与 Python 扩展同目录)
    install(TARGETS sageflow
            LIBRARY DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../python
            ARCHIVE DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../python
            RUNTIME DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../python
    )
else()
    # 独立 C++ 构建模式：标准安装路径
    install(TARGETS sageflow
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            RUNTIME DESTINATION bin
    )
endif()
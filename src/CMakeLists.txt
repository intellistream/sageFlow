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
install(TARGETS sageflow
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
)
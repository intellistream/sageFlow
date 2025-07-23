# CMakeLists.txt for Integration Tests

# 不使用find_package，而是直接使用项目中的googletest
# 确保googletest已经被构建
if(NOT TARGET gtest)
    message(FATAL_ERROR "GoogleTest not found. Make sure third-party/googletest is built.")
endif()

# 添加集成测试可执行文件 - 修改为test_pipeline
add_executable(test_pipeline
    test_pipeline.cpp
)

# 链接所需的库 - 使用项目内部的gtest目标和项目库
target_link_libraries(test_pipeline
    gtest
    gtest_main
    candy
    pthread
)

# 设置包含目录
target_include_directories(test_pipeline PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third-party/googletest/googletest/include
)

# 设置编译标志
target_compile_features(test_pipeline PRIVATE cxx_std_17)
target_compile_options(test_pipeline PRIVATE
    -Wall
    -Wextra
    -O2
)

# 添加测试到CTest
if(BUILD_TESTING)
    add_test(NAME PipelineConstructionTest COMMAND test_pipeline --gtest_filter=PipelineConstructionTest.*)

    # 设置测试属性
    set_tests_properties(PipelineConstructionTest PROPERTIES
        TIMEOUT 30
        LABELS "integration;pipeline"
    )
endif()

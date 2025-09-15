# Top-level test CMakeLists.txt (reconstructed)
# 说明: 本文件使用项目根目录引入的 enableTest.cmake 中的 add_gtest 宏。
# 不再调用 find_package(GTest)；googletest 已在 third-party 子目录构建。

if(NOT BUILD_TESTING)
    return()
endif()

# -----------------------------------------------------------------------------
# 公共测试支持库：数据生成、配置等（可继续扩展）
# -----------------------------------------------------------------------------
add_library(test_data_support
    test_utils/test_data_generator.cpp
    test_utils/test_config_manager.cpp
    test_utils/dynamic_config.cpp
)

# 头文件路径
# 将 include 设为 PUBLIC，使链接该库的测试目标能继承 utils/ 等头文件路径
target_include_directories(test_data_support
    PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 依赖主工程接口库 candy（其 transitively 链接全部内部组件）
target_link_libraries(test_data_support
    PRIVATE
    candy
)

# 让公共库也拥有相同的编译宏（例如 PROJECT_DIR、CANDY_ENABLE_METRICS）
target_compile_definitions(test_data_support PRIVATE ${TEST_COMMON_DEFINES})

# 向所有测试目标注入的公共宏定义，包括CANDY_ENABLE_METRICS以支持性能测试
set(TEST_COMMON_DEFINES 
    PROJECT_DIR="${CMAKE_SOURCE_DIR}"
    CANDY_ENABLE_METRICS=1
)

# -----------------------------------------------------------------------------
# Helper: 注册单个测试源文件
# 参数:  名称  路径(相对 test/ )  超时(秒)  类型标签
# -----------------------------------------------------------------------------
function(register_test TEST_NAME REL_PATH TIMEOUT_SECS GROUP_TAG)
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${REL_PATH}")
        message(WARNING "Test source not found: ${REL_PATH} (skip ${TEST_NAME})")
        return()
    endif()
    add_gtest(${TEST_NAME} ${REL_PATH})           # 宏自动链接 gtest_main
    target_link_libraries(${TEST_NAME} PRIVATE test_data_support candy)
    target_compile_definitions(${TEST_NAME} PRIVATE ${TEST_COMMON_DEFINES})
    # 为每个测试目标补充包含路径：工程 include/ 与 test/ 根目录（包含 utils/、UnitTest/、Performance/ 等）
    target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
    set_tests_properties(${TEST_NAME} PROPERTIES TIMEOUT ${TIMEOUT_SECS} LABELS ${GROUP_TAG})
endfunction()

# -----------------------------------------------------------------------------
# 单元测试 (UnitTest)
# -----------------------------------------------------------------------------
set(UNIT_TEST_SPECS
    test_join_bruteforce           UnitTest/test_join_bruteforce.cpp          300 UNIT
    test_join_ivf                  UnitTest/test_join_ivf.cpp                 300 UNIT
    test_compute_engine            UnitTest/test_compute_engine.cpp           180 UNIT
    test_file_stream_source        UnitTest/test_file_stream_source.cpp       120 UNIT
)

list(LENGTH UNIT_TEST_SPECS _ulen)
math(EXPR _uend "${_ulen}-1")
foreach(i RANGE 0 ${_uend} 4)
    list(SUBLIST UNIT_TEST_SPECS ${i} 4 entry)
    list(GET entry 0 t_name)
    list(GET entry 1 t_path)
    list(GET entry 2 t_timeout)
    list(GET entry 3 t_label)
    register_test(${t_name} ${t_path} ${t_timeout} ${t_label})
endforeach()

# -----------------------------------------------------------------------------
# 性能测试 (Performance) - 特别启用METRICS收集
# -----------------------------------------------------------------------------
set(PERF_TEST_SPECS
    test_join_perf_scaling         Performance/test_join_perf_scaling.cpp     900 PERF
    test_join_performance_methods  Performance/test_join_performance_methods.cpp 900 PERF
    test_window_pipeline           Performance/test_window_pipeline.cpp       600 PERF
    IndexTest                      Performance/IndexTest.cpp                  600 PERF
)

list(LENGTH PERF_TEST_SPECS _plen)
math(EXPR _pend "${_plen}-1")
foreach(i RANGE 0 ${_pend} 4)
    list(SUBLIST PERF_TEST_SPECS ${i} 4 entry)
    list(GET entry 0 t_name)
    list(GET entry 1 t_path)
    list(GET entry 2 t_timeout)
    list(GET entry 3 t_label)
    register_test(${t_name} ${t_path} ${t_timeout} ${t_label})
endforeach()

# -----------------------------------------------------------------------------
# 集成测试 (IntegrationTest)
# -----------------------------------------------------------------------------
set(INTEG_TEST_SPECS
    test_pipeline_basic            IntegrationTest/test_pipeline_basic.cpp    600 INTEGRATION
    test_pipeline                  IntegrationTest/test_pipeline.cpp          600 INTEGRATION
    test_pipeline_execution        IntegrationTest/test_pipeline_execution.cpp 600 INTEGRATION
)

list(LENGTH INTEG_TEST_SPECS _ilen)
math(EXPR _iend "${_ilen}-1")
foreach(i RANGE 0 ${_iend} 4)
    list(SUBLIST INTEG_TEST_SPECS ${i} 4 entry)
    list(GET entry 0 t_name)
    list(GET entry 1 t_path)
    list(GET entry 2 t_timeout)
    list(GET entry 3 t_label)
    register_test(${t_name} ${t_path} ${t_timeout} ${t_label})
endforeach()

# -----------------------------------------------------------------------------
# 聚合自定义目标
# -----------------------------------------------------------------------------
# 仅依赖已创建的测试可执行（未显式依赖 ctest 运行，方便 IDE 构建）
set(ALL_UNIT_TARGETS test_join_bruteforce test_join_ivf test_compute_engine test_config_driven test_config_manager test_file_stream_source)
set(ALL_PERF_TARGETS test_join_perf_scaling test_join_performance_methods test_window_pipeline IndexTest)
set(ALL_INTEG_TARGETS test_pipeline_basic test_pipeline test_pipeline_execution)

add_custom_target(build_unit_tests DEPENDS ${ALL_UNIT_TARGETS})
add_custom_target(build_perf_tests DEPENDS ${ALL_PERF_TARGETS})
add_custom_target(build_integration_tests DEPENDS ${ALL_INTEG_TARGETS})
add_custom_target(build_all_tests DEPENDS ${ALL_UNIT_TARGETS} ${ALL_PERF_TARGETS} ${ALL_INTEG_TARGETS})

# 直接运行分类测试的快捷目标（执行 ctest 过滤）
add_custom_target(run_unit_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L UNIT
    DEPENDS build_unit_tests
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
add_custom_target(run_perf_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L PERF
    DEPENDS build_perf_tests
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
add_custom_target(run_integration_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L INTEGRATION
    DEPENDS build_integration_tests
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
add_custom_target(run_all_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS build_all_tests
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# 快速核心回归：挑选核心 join + 基础 pipeline
add_custom_target(run_quick_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "(test_join_bruteforce|test_join_ivf|test_pipeline_basic)"
    DEPENDS test_join_bruteforce test_join_ivf test_pipeline_basic
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# 输出说明
message(STATUS "Test targets registered: UNIT=${ALL_UNIT_TARGETS}; PERF=${ALL_PERF_TARGETS}; INTEGRATION=${ALL_INTEG_TARGETS}")
message(STATUS "CANDY_ENABLE_METRICS enabled for all tests to collect performance data")
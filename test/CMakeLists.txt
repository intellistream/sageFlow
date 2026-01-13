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
    test_utils/join_config_loader.cpp
    test_utils/integration_test_config.cpp
    test_utils/data_source/random_data_source.cpp
    test_utils/data_source/dataset_data_source.cpp
    test_utils/data_source/json_data_source.cpp
    test_utils/data_writer/fvecs_writer.cpp
    test_utils/data_writer/json_writer.cpp
    test_utils/join_data_source.cpp
    test_utils/join_test_helper.cpp
    test_utils/join_integration_pipeline_helper.cpp
    test_utils/test_report_generator.cpp
)

# 头文件路径
# 将 include 设为 PUBLIC，使链接该库的测试目标能继承 utils/ 等头文件路径
target_include_directories(test_data_support
    PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 依赖主工程接口库 sageflow（其 transitively 链接全部内部组件）
target_link_libraries(test_data_support
    PRIVATE
    sageflow
)

# 向所有测试目标注入的公共宏定义（基线：PROJECT_DIR；按选项追加 SAGEFLOW_ENABLE_METRICS）
set(TEST_COMMON_DEFINES 
    PROJECT_DIR="${CMAKE_SOURCE_DIR}"
)
if(SAGEFLOW_ENABLE_METRICS)
    list(APPEND TEST_COMMON_DEFINES SAGEFLOW_ENABLE_METRICS=1)
endif()

# 让公共库也拥有相同的编译宏（例如 PROJECT_DIR、可选的 SAGEFLOW_ENABLE_METRICS）
target_compile_definitions(test_data_support PRIVATE ${TEST_COMMON_DEFINES})

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
    target_link_libraries(${TEST_NAME} PRIVATE test_data_support sageflow)
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
    test_join_hnsw                 UnitTest/test_join_hnsw.cpp                300 UNIT
    test_join_operator_state       UnitTest/test_join_operator_state.cpp      300 UNIT
    test_bruteforce_baseline       UnitTest/test_bruteforce_baseline.cpp      300 UNIT
    test_ivf_method                UnitTest/test_ivf_method.cpp               300 UNIT
    test_hdr_tree                  UnitTest/test_hdr_tree.cpp                 300 UNIT
    test_hdr_rknn                  UnitTest/test_hdr_rknn.cpp                 300 UNIT
    # test_clustered_join 和 test_clustered_join_method 已备份，待架构重构后重写
    # test_clustered_join            UnitTest/test_clustered_join.cpp           300 UNIT
    # test_clustered_join_method     UnitTest/test_clustered_join_method.cpp    300 UNIT
    test_s3j_method                UnitTest/test_s3j_method.cpp               300 UNIT
    test_lsh_method                UnitTest/test_lsh_method.cpp               300 UNIT
    test_partitioner               UnitTest/test_partitioner.cpp              120 UNIT
    test_clustered_partitioner     UnitTest/test_clustered_partitioner.cpp    120 UNIT
    test_centroid_cold_start       UnitTest/test_centroid_cold_start.cpp      180 UNIT
    test_compute_engine            UnitTest/test_compute_engine.cpp           180 UNIT
    test_file_stream_source        UnitTest/test_file_stream_source.cpp       120 UNIT
    test_data_source               UnitTest/test_data_source.cpp              120 UNIT
    test_data_persistence          UnitTest/test_data_persistence.cpp         120 UNIT
    test_join_data_source          UnitTest/test_join_data_source.cpp         120 UNIT
    test_connection_strategy       UnitTest/test_connection_strategy.cpp      120 UNIT
    test_runtime_context           UnitTest/test_runtime_context.cpp          120 UNIT
    test_window_state              UnitTest/test_window_state.cpp             120 UNIT
    test_two_tier_window_state     UnitTest/test_two_tier_window_state.cpp    120 UNIT
    test_boundary_tracker          UnitTest/test_boundary_tracker.cpp         120 UNIT
    test_late_arrival_handler      UnitTest/test_late_arrival_handler.cpp     120 UNIT
    test_vector_space_partitioner  UnitTest/test_vector_space_partitioner.cpp 120 UNIT
    test_partition_coordinator     UnitTest/test_partition_coordinator.cpp    120 UNIT
    test_distance_verifier         UnitTest/test_distance_verifier.cpp        120 UNIT
    test_pca                       UnitTest/test_pca.cpp                      120 UNIT
    test_simd_distance             UnitTest/test_simd_distance.cpp            180 UNIT
    test_partitioned_index         UnitTest/test_partitioned_index.cpp        180 UNIT
    test_partitioned_vector_state  UnitTest/test_partitioned_vector_state.cpp 180 UNIT
    test_async_candidate_generator UnitTest/test_async_candidate_generator.cpp 180 UNIT
    test_join_strategy_factory     UnitTest/test_join_strategy_factory.cpp    180 UNIT
    test_partitioner_factory       UnitTest/test_partitioner_factory.cpp      120 UNIT
    test_window_state_factory      UnitTest/test_window_state_factory.cpp     120 UNIT
    test_join_method_registry      UnitTest/test_join_method_registry.cpp     180 UNIT
    test_join_config_validator     UnitTest/test_join_config_validator.cpp    120 UNIT
    test_join_config_loader        UnitTest/test_join_config_loader.cpp       120 UNIT
    test_integration_config        UnitTest/test_integration_config.cpp       120 UNIT
    test_join_integration_pipeline UnitTest/test_join_integration_pipeline.cpp 300 UNIT
    test_join_operator_strategy    UnitTest/test_join_operator_strategy.cpp   180 UNIT
    test_join_metrics              UnitTest/test_join_metrics.cpp             180 UNIT
    test_report_generator          UnitTest/test_report_generator.cpp         180 UNIT
    test_multicast_partitioner     UnitTest/test_multicast_partitioner.cpp    120 UNIT
    test_clustered_config          UnitTest/test_clustered_config.cpp         120 UNIT
    test_result_partition_switch   UnitTest/test_result_partition_switch.cpp  180 UNIT
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
    test_window_pipeline           Performance/test_window_pipeline.cpp       600 PERF
    test_join_datasource_modes     Performance/test_join_datasource_modes.cpp 900 PERF
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
# IndexTest: 非 GoogleTest 可执行，使用自定义 main（避免 gtest_discover_tests 传入 --gtest_list_tests）
# -----------------------------------------------------------------------------
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/Performance/IndexTest.cpp")
    add_executable(IndexTest Performance/IndexTest.cpp)
    target_link_libraries(IndexTest PRIVATE test_data_support sageflow)
    target_compile_definitions(IndexTest PRIVATE ${TEST_COMMON_DEFINES})
    target_include_directories(IndexTest PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
    add_test(NAME IndexTest COMMAND IndexTest)
    set_tests_properties(IndexTest PROPERTIES TIMEOUT 600 LABELS PERF)
endif()

# -----------------------------------------------------------------------------
# 集成测试 (IntegrationTest)
# -----------------------------------------------------------------------------
set(INTEG_TEST_SPECS
    test_pipeline_basic            IntegrationTest/test_pipeline_basic.cpp    600 INTEGRATION
    test_pipeline_execution        IntegrationTest/test_pipeline_execution.cpp 600 INTEGRATION
    test_vsjoin_integration        IntegrationTest/test_vsjoin_integration.cpp 600 INTEGRATION
    test_join_baseline_integration IntegrationTest/join_baseline_integration_test.cpp 600 INTEGRATION
    test_clustered_join_cold_start IntegrationTest/test_clustered_join_cold_start.cpp 600 INTEGRATION
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

# 在 CI 环境（例如 GitHub Actions 的 ubuntu-latest）下，过滤掉 test_pipeline_basic 中的高并发用例，避免 2vCPU 上的不稳定
if(DEFINED ENV{GITHUB_ACTIONS} OR DEFINED ENV{CI})
    message(STATUS "CI detected: will exclude MultiThreadPipelineTest.HighConcurrencyDeadlockTest from test_pipeline_basic via GTEST_FILTER")
    # 注意：ENVIRONMENT 属性为以分号分隔的 VAR=VALUE 列表，这里仅设置 GTEST_FILTER
    set_tests_properties(test_pipeline_basic PROPERTIES ENVIRONMENT "GTEST_FILTER=-MultiThreadPipelineTest.HighConcurrencyDeadlockTest")
endif()

# -----------------------------------------------------------------------------
# 聚合自定义目标
# -----------------------------------------------------------------------------
# 仅依赖已创建的测试可执行（未显式依赖 ctest 运行，方便 IDE 构建）
set(ALL_UNIT_TARGETS test_join_bruteforce test_join_ivf test_partitioner test_compute_engine test_file_stream_source test_data_source test_data_persistence test_join_data_source)
set(ALL_PERF_TARGETS test_window_pipeline test_join_datasource_modes IndexTest)
set(ALL_INTEG_TARGETS test_pipeline_basic test_pipeline_execution test_vsjoin_integration test_join_baseline_integration)

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

# -----------------------------------------------------------------------------
# Clustered Join 专用测试目标
# -----------------------------------------------------------------------------
# 运行 Clustered Join 单元测试
add_custom_target(run_clustered_unit_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "(test_clustered_partitioner|test_clustered_config)"
    DEPENDS test_clustered_partitioner test_clustered_config
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running Clustered Join unit tests (integration tests via test_join_baseline_integration)"
)

# 运行 Clustered Join 集成测试（通过 gtest_filter）
add_custom_target(run_clustered_integration_tests
    COMMAND $<TARGET_FILE:test_join_baseline_integration> --gtest_filter=*clustered*
    DEPENDS test_join_baseline_integration
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running Clustered Join integration tests"
)

# 组合目标：运行所有 Clustered Join 相关测试
add_custom_target(run_clustered_tests
    DEPENDS run_clustered_unit_tests run_clustered_integration_tests
    COMMENT "Running all Clustered Join tests (unit + integration)"
)

# 添加 CTest 测试用例，便于 ctest -L CLUSTERED 过滤
add_test(
    NAME ClusteredJoinIntegration_BruteForce
    COMMAND $<TARGET_FILE:test_join_baseline_integration> --gtest_filter=*clustered_bruteforce*
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
set_tests_properties(ClusteredJoinIntegration_BruteForce PROPERTIES 
    TIMEOUT 600 
    LABELS "INTEGRATION;CLUSTERED"
)

add_test(
    NAME ClusteredJoinIntegration_IVF
    COMMAND $<TARGET_FILE:test_join_baseline_integration> --gtest_filter=*clustered_ivf*
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
set_tests_properties(ClusteredJoinIntegration_IVF PROPERTIES 
    TIMEOUT 600 
    LABELS "INTEGRATION;CLUSTERED"
)

add_test(
    NAME ClusteredJoinIntegration_NoMulticast
    COMMAND $<TARGET_FILE:test_join_baseline_integration> --gtest_filter=*clustered_no_multicast*
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
set_tests_properties(ClusteredJoinIntegration_NoMulticast PROPERTIES 
    TIMEOUT 600 
    LABELS "INTEGRATION;CLUSTERED"
)

# 输出说明
message(STATUS "Test targets registered: UNIT=${ALL_UNIT_TARGETS}; PERF=${ALL_PERF_TARGETS}; INTEGRATION=${ALL_INTEG_TARGETS}")
if(SAGEFLOW_ENABLE_METRICS)
    message(STATUS "SAGEFLOW_ENABLE_METRICS enabled for tests to collect performance data")
else()
    message(STATUS "SAGEFLOW_ENABLE_METRICS disabled for tests (metrics code excluded)")
endif()

# -----------------------------------------------------------------------------
# Optional: Data Source Example (demonstrates the data source framework)
# -----------------------------------------------------------------------------
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/test_data_source_example.cpp")
    add_executable(test_data_source_example examples/test_data_source_example.cpp)
    target_link_libraries(test_data_source_example PRIVATE test_data_support sageflow)
    target_compile_definitions(test_data_source_example PRIVATE ${TEST_COMMON_DEFINES})
    target_include_directories(test_data_source_example PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
    message(STATUS "Added optional example: test_data_source_example")
endif()

# -----------------------------------------------------------------------------
# Optional: Data Persistence Example (demonstrates save/load functionality)
# -----------------------------------------------------------------------------
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/data_persistence_example.cpp")
    add_executable(data_persistence_example examples/data_persistence_example.cpp)
    target_link_libraries(data_persistence_example PRIVATE test_data_support sageflow)
    target_compile_definitions(data_persistence_example PRIVATE ${TEST_COMMON_DEFINES})
    target_include_directories(data_persistence_example PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
    message(STATUS "Added optional example: data_persistence_example")
endif()
add_lib(
        operator
        filter_operator.cpp
        join_operator.cpp
        map_operator.cpp
        operator.cpp
        output_operator.cpp
        sink_operator.cpp
        topk_operator.cpp
        window_operator.cpp
        itopk_operator.cpp
        aggregate_operator.cpp
        # utils/ 子目录 - 配置和工厂相关
        utils/join_strategy_config.cpp
        utils/join_strategy_factory.cpp
        utils/join_config_validator.cpp
        utils/join_method_registry.cpp
        # join_operator_methods/ 子目录 - 各种 join 方法实现
        join_operator_methods/base_method.cpp
        join_operator_methods/bruteforce.cpp
        join_operator_methods/bruteforce_baseline.cpp
        join_operator_methods/ivf.cpp
        join_operator_methods/hnsw.cpp
        join_operator_methods/hdr_tree_method.cpp
        join_operator_methods/clustered_join_method.cpp
        join_operator_methods/s3j_method.cpp
        join_operator_methods/vsjoin_method.cpp
        join_operator_methods/s3j_components/adaptive_partitioner.cpp
        join_operator_methods/s3j_components/adaptive_index_selector.cpp
        # vsjoin_components/ 子目录 - VSJoin 专用组件
        join_operator_methods/vsjoin_components/distance_verifier.cpp
        join_operator_methods/vsjoin_components/async_candidate_generator.cpp
)
target_link_libraries(
        operator
        PRIVATE
        function
)

# 打开 join_operator 的数据打桩开关，启用指标采集代码块（可通过顶层选项控制）
if(SAGEFLOW_ENABLE_METRICS)
    target_compile_definitions(
            operator
            PRIVATE
            SAGEFLOW_ENABLE_METRICS=1
    )
endif()
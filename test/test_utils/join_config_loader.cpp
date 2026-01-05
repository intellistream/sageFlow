#include "test_utils/join_config_loader.h"
#include "test_utils/dynamic_config.h"
#include "utils/logger.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <toml++/toml.hpp>

namespace sageFlow {
namespace test {

// ==================== 路径解析 ====================

std::string JoinConfigLoader::resolvePath(const std::string& path) {
    return DynamicConfigManager::resolveProjectRelativePath(path);
}

std::string JoinConfigLoader::getDefaultConfigPath() {
    return resolvePath("config/join_strategies.toml");
}

// ==================== 加载单个配置 ====================

JoinStrategyConfig JoinConfigLoader::loadFromFile(const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);

    try {
        return loadJoinStrategyConfig(resolved_path);
    } catch (const std::exception& e) {
        throw std::runtime_error("JoinConfigLoader: Failed to load config from '" +
                                 config_path + "': " + e.what());
    }
}

JoinStrategyConfig JoinConfigLoader::loadByName(const std::string& config_path,
                                                const std::string& strategy_name) {
    std::string resolved_path = resolvePath(config_path);

    try {
        return loadJoinStrategyConfig(resolved_path, strategy_name);
    } catch (const std::exception& e) {
        throw std::runtime_error("JoinConfigLoader: Failed to load strategy '" +
                                 strategy_name + "' from '" + config_path +
                                 "': " + e.what());
    }
}

// ==================== 批量加载配置 ====================

std::vector<JoinStrategyConfig> JoinConfigLoader::loadAllFromFile(
    const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);

    std::vector<JoinStrategyConfig> configs;

    try {
        auto doc = toml::parse_file(resolved_path);

        // 加载默认配置作为基础
        JoinStrategyConfig base_config;
        if (doc.contains("default")) {
            if (auto* defaults = doc["default"].as_table()) {
                // 使用现有的 loadJoinStrategyConfig 从临时结构加载
                // 这里直接解析 default 节点
                if (auto threshold = (*defaults)["similarity_threshold"].value<double>()) {
                    base_config.similarity_threshold = *threshold;
                }
                if (auto dim = (*defaults)["dimension"].value<int64_t>()) {
                    base_config.dimension = static_cast<int>(*dim);
                }
                if (auto ws = (*defaults)["window_size_ms"].value<int64_t>()) {
                    base_config.window_size_ms = *ws;
                }
                if (auto ss = (*defaults)["step_size_ms"].value<int64_t>()) {
                    base_config.step_size_ms = *ss;
                }
                if (auto eager = (*defaults)["is_eager"].value<bool>()) {
                    base_config.is_eager = *eager;
                }
            }
        }

        // 遍历 [strategies] 下的所有策略
        if (auto* strategies = doc["strategies"].as_table()) {
            for (const auto& [key, value] : *strategies) {
                if (auto* strategy_table = value.as_table()) {
                    try {
                        // 从策略名加载完整配置（会自动合并 default）
                        auto config =
                            loadJoinStrategyConfig(resolved_path, std::string(key.str()));
                        configs.push_back(config);
                    } catch (const std::exception& e) {
                        SAGEFLOW_LOG_WARN("JoinConfigLoader",
                                          "Failed to load strategy '{}': {}",
                                          std::string(key.str()), e.what());
                    }
                }
            }
        }

    } catch (const toml::parse_error& e) {
        throw std::runtime_error("JoinConfigLoader: Failed to parse TOML config '" +
                                 config_path + "': " + e.what());
    }

    return configs;
}

std::vector<JoinStrategyConfig> JoinConfigLoader::loadByNames(
    const std::string& config_path, const std::vector<std::string>& strategy_names) {
    std::vector<JoinStrategyConfig> configs;
    configs.reserve(strategy_names.size());

    for (const auto& name : strategy_names) {
        try {
            configs.push_back(loadByName(config_path, name));
        } catch (const std::exception& e) {
            SAGEFLOW_LOG_WARN("JoinConfigLoader", "Failed to load strategy '{}': {}",
                              name, e.what());
        }
    }

    return configs;
}

std::vector<JoinStrategyConfig> JoinConfigLoader::loadByAlgorithm(
    const std::string& config_path, JoinAlgorithm algorithm) {
    auto all_configs = loadAllFromFile(config_path);
    std::vector<JoinStrategyConfig> filtered;

    for (auto& config : all_configs) {
        if (config.algorithm == algorithm) {
            filtered.push_back(std::move(config));
        }
    }

    return filtered;
}

// ==================== 配置合并 ====================

JoinStrategyConfig JoinConfigLoader::merge(const JoinStrategyConfig& base,
                                           const JoinStrategyConfig& override_config) {
    JoinStrategyConfig result = base;

    // 检查 override 的各字段是否为非默认值，如果是则覆盖

    // 算法类型：如果 override 不是 BRUTEFORCE（默认值），则覆盖
    if (override_config.algorithm != JoinAlgorithm::BRUTEFORCE) {
        result.algorithm = override_config.algorithm;
    }

    // 分区策略
    if (override_config.partition_strategy != PartitionStrategy::ROUND_ROBIN) {
        result.partition_strategy = override_config.partition_strategy;
    }

    // 窗口状态类型
    if (override_config.window_state_type != WindowStateType::SHARED) {
        result.window_state_type = override_config.window_state_type;
    }

    // 索引策略
    if (override_config.index_strategy != IndexStrategy::SHARED) {
        result.index_strategy = override_config.index_strategy;
    }

    // is_eager（只有当 override 为 true 时才覆盖，因为 false 是默认值）
    if (override_config.is_eager) {
        result.is_eager = override_config.is_eager;
    }

    // 数值参数：使用非默认值覆盖
    // similarity_threshold: 默认 0.8
    if (override_config.similarity_threshold != 0.8 &&
        override_config.similarity_threshold > 0) {
        result.similarity_threshold = override_config.similarity_threshold;
    }

    // dimension: 默认 128
    if (override_config.dimension != 128 && override_config.dimension > 0) {
        result.dimension = override_config.dimension;
    }

    // num_partitions: 默认 4
    if (override_config.num_partitions != 4 && override_config.num_partitions > 0) {
        result.num_partitions = override_config.num_partitions;
    }

    // window_size_ms: 默认 10000
    if (override_config.window_size_ms != 10000 && override_config.window_size_ms > 0) {
        result.window_size_ms = override_config.window_size_ms;
    }

    // step_size_ms: 默认 1000
    if (override_config.step_size_ms != 1000 && override_config.step_size_ms > 0) {
        result.step_size_ms = override_config.step_size_ms;
    }

    // IVF 参数
    if (override_config.ivf_nlist != 100 && override_config.ivf_nlist > 0) {
        result.ivf_nlist = override_config.ivf_nlist;
    }
    if (override_config.ivf_nprobes != 10 && override_config.ivf_nprobes > 0) {
        result.ivf_nprobes = override_config.ivf_nprobes;
    }
    if (override_config.ivf_rebuild_threshold != 0.3 &&
        override_config.ivf_rebuild_threshold > 0) {
        result.ivf_rebuild_threshold = override_config.ivf_rebuild_threshold;
    }

    // HNSW 参数
    if (override_config.hnsw_m != 16 && override_config.hnsw_m > 0) {
        result.hnsw_m = override_config.hnsw_m;
    }
    if (override_config.hnsw_ef_construction != 200 &&
        override_config.hnsw_ef_construction > 0) {
        result.hnsw_ef_construction = override_config.hnsw_ef_construction;
    }
    if (override_config.hnsw_ef_search != 50 && override_config.hnsw_ef_search > 0) {
        result.hnsw_ef_search = override_config.hnsw_ef_search;
    }

    // VSJoin 参数
    if (override_config.vsjoin_num_hash_functions != 8 &&
        override_config.vsjoin_num_hash_functions > 0) {
        result.vsjoin_num_hash_functions = override_config.vsjoin_num_hash_functions;
    }
    if (override_config.vsjoin_boundary_threshold != 0.1 &&
        override_config.vsjoin_boundary_threshold > 0) {
        result.vsjoin_boundary_threshold = override_config.vsjoin_boundary_threshold;
    }
    if (override_config.vsjoin_async_threads != 2 &&
        override_config.vsjoin_async_threads > 0) {
        result.vsjoin_async_threads = override_config.vsjoin_async_threads;
    }
    if (override_config.vsjoin_allowed_lateness != 1000 &&
        override_config.vsjoin_allowed_lateness > 0) {
        result.vsjoin_allowed_lateness = override_config.vsjoin_allowed_lateness;
    }

    // S3J 参数
    if (override_config.s3j_num_centroids != 16 && override_config.s3j_num_centroids > 0) {
        result.s3j_num_centroids = override_config.s3j_num_centroids;
    }
    if (override_config.s3j_adapt_interval_ms != 1000 &&
        override_config.s3j_adapt_interval_ms > 0) {
        result.s3j_adapt_interval_ms = override_config.s3j_adapt_interval_ms;
    }
    if (override_config.s3j_load_threshold != 0.3 &&
        override_config.s3j_load_threshold > 0) {
        result.s3j_load_threshold = override_config.s3j_load_threshold;
    }
    // s3j_enable_adaptive: 直接使用 override 的值
    result.s3j_enable_adaptive = override_config.s3j_enable_adaptive;

    // ClusteredJoin 参数
    if (override_config.clustered_overlap_ratio != 0.1 &&
        override_config.clustered_overlap_ratio > 0) {
        result.clustered_overlap_ratio = override_config.clustered_overlap_ratio;
    }
    if (override_config.clustered_rebalance_threshold != 0.3 &&
        override_config.clustered_rebalance_threshold > 0) {
        result.clustered_rebalance_threshold = override_config.clustered_rebalance_threshold;
    }
    // clustered_border_replication 已废弃
    if (override_config.clustered_training_samples != 1000 &&
        override_config.clustered_training_samples > 0) {
        result.clustered_training_samples = override_config.clustered_training_samples;
    }
    if (override_config.clustered_multicast_k != 0) {
        result.clustered_multicast_k = override_config.clustered_multicast_k;
    }

    // HDR-Tree 参数
    if (override_config.hdr_projected_dim != 8 && override_config.hdr_projected_dim > 0) {
        result.hdr_projected_dim = override_config.hdr_projected_dim;
    }
    if (override_config.hdr_max_node_size != 100 && override_config.hdr_max_node_size > 0) {
        result.hdr_max_node_size = override_config.hdr_max_node_size;
    }
    if (override_config.hdr_delta_buffer_size != 1000 &&
        override_config.hdr_delta_buffer_size > 0) {
        result.hdr_delta_buffer_size = override_config.hdr_delta_buffer_size;
    }
    if (override_config.hdr_pca_sample_size != 10000 &&
        override_config.hdr_pca_sample_size > 0) {
        result.hdr_pca_sample_size = override_config.hdr_pca_sample_size;
    }

    // 双层窗口参数
    if (override_config.two_tier_compact_threshold != 100 &&
        override_config.two_tier_compact_threshold > 0) {
        result.two_tier_compact_threshold = override_config.two_tier_compact_threshold;
    }
    result.two_tier_enable_boundary_tracking =
        override_config.two_tier_enable_boundary_tracking;

    return result;
}

// ==================== 保存配置 ====================

void JoinConfigLoader::saveToFile(const JoinStrategyConfig& config,
                                  const std::string& output_path) {
    std::string resolved_path = resolvePath(output_path);

    // 创建目录（如果不存在）
    std::filesystem::path file_path(resolved_path);
    if (file_path.has_parent_path()) {
        std::filesystem::create_directories(file_path.parent_path());
    }

    std::ofstream ofs(resolved_path);
    if (!ofs) {
        throw std::runtime_error("JoinConfigLoader: Failed to open file for writing: " +
                                 output_path);
    }

    // 写入 TOML 格式
    ofs << "# Auto-generated JoinStrategyConfig\n\n";

    // 基础配置
    ofs << "algorithm = \"" << toString(config.algorithm) << "\"\n";
    ofs << "is_eager = " << (config.is_eager ? "true" : "false") << "\n";
    ofs << "similarity_threshold = " << config.similarity_threshold << "\n";
    ofs << "dimension = " << config.dimension << "\n\n";

    // 分区配置
    ofs << "partition_strategy = \"" << toString(config.partition_strategy) << "\"\n";
    ofs << "num_partitions = " << config.num_partitions << "\n\n";

    // 窗口状态配置
    ofs << "window_state_type = \"" << toString(config.window_state_type) << "\"\n";
    ofs << "window_size_ms = " << config.window_size_ms << "\n";
    ofs << "step_size_ms = " << config.step_size_ms << "\n\n";

    // 索引配置
    ofs << "index_strategy = \"" << toString(config.index_strategy) << "\"\n\n";

    // IVF 参数
    ofs << "# IVF Parameters\n";
    ofs << "ivf_nlist = " << config.ivf_nlist << "\n";
    ofs << "ivf_nprobes = " << config.ivf_nprobes << "\n";
    ofs << "ivf_rebuild_threshold = " << config.ivf_rebuild_threshold << "\n\n";

    // HNSW 参数
    ofs << "# HNSW Parameters\n";
    ofs << "hnsw_m = " << config.hnsw_m << "\n";
    ofs << "hnsw_ef_construction = " << config.hnsw_ef_construction << "\n";
    ofs << "hnsw_ef_search = " << config.hnsw_ef_search << "\n\n";

    // VSJoin 参数
    ofs << "# VSJoin Parameters\n";
    ofs << "vsjoin_num_hash_functions = " << config.vsjoin_num_hash_functions << "\n";
    ofs << "vsjoin_boundary_threshold = " << config.vsjoin_boundary_threshold << "\n";
    ofs << "vsjoin_async_threads = " << config.vsjoin_async_threads << "\n";
    ofs << "vsjoin_allowed_lateness = " << config.vsjoin_allowed_lateness << "\n\n";

    // S3J 参数
    ofs << "# S3J Parameters\n";
    ofs << "s3j_num_centroids = " << config.s3j_num_centroids << "\n";
    ofs << "s3j_adapt_interval_ms = " << config.s3j_adapt_interval_ms << "\n";
    ofs << "s3j_load_threshold = " << config.s3j_load_threshold << "\n";
    ofs << "s3j_enable_adaptive = " << (config.s3j_enable_adaptive ? "true" : "false")
        << "\n\n";

    // ClusteredJoin 参数
    ofs << "# ClusteredJoin Parameters\n";
    ofs << "clustered_multicast_k = " << config.clustered_multicast_k << "\n";
    ofs << "clustered_overlap_ratio = " << config.clustered_overlap_ratio << "\n";
    ofs << "clustered_rebalance_threshold = " << config.clustered_rebalance_threshold
        << "\n";
    ofs << "clustered_training_samples = " << config.clustered_training_samples << "\n\n";

    // HDR-Tree 参数
    ofs << "# HDR-Tree Parameters\n";
    ofs << "hdr_projected_dim = " << config.hdr_projected_dim << "\n";
    ofs << "hdr_max_node_size = " << config.hdr_max_node_size << "\n";
    ofs << "hdr_delta_buffer_size = " << config.hdr_delta_buffer_size << "\n";
    ofs << "hdr_pca_sample_size = " << config.hdr_pca_sample_size << "\n\n";

    // 双层窗口参数
    ofs << "# Two-Tier Window Parameters\n";
    ofs << "two_tier_compact_threshold = " << config.two_tier_compact_threshold << "\n";
    ofs << "two_tier_enable_boundary_tracking = "
        << (config.two_tier_enable_boundary_tracking ? "true" : "false") << "\n";

    ofs.close();
}

// ==================== 辅助函数 ====================

std::vector<std::string> JoinConfigLoader::listStrategyNames(
    const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);
    std::vector<std::string> names;

    try {
        auto doc = toml::parse_file(resolved_path);

        if (auto* strategies = doc["strategies"].as_table()) {
            for (const auto& [key, value] : *strategies) {
                if (value.is_table()) {
                    names.emplace_back(key.str());
                }
            }
        }

    } catch (const toml::parse_error& e) {
        throw std::runtime_error("JoinConfigLoader: Failed to parse TOML config '" +
                                 config_path + "': " + e.what());
    }

    return names;
}

bool JoinConfigLoader::isValidConfigFile(const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);

    if (!std::filesystem::exists(resolved_path)) {
        return false;
    }

    try {
        auto doc = toml::parse_file(resolved_path);
        return true;
    } catch (...) {
        return false;
    }
}

bool JoinConfigLoader::isDefaultConfig(const JoinStrategyConfig& config) {
    JoinStrategyConfig default_config;
    return config.algorithm == default_config.algorithm &&
           config.partition_strategy == default_config.partition_strategy &&
           config.window_state_type == default_config.window_state_type &&
           config.index_strategy == default_config.index_strategy &&
           config.similarity_threshold == default_config.similarity_threshold &&
           config.dimension == default_config.dimension;
}

}  // namespace test
}  // namespace sageFlow

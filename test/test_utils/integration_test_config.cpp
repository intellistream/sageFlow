#include "test_utils/integration_test_config.h"
#include "test_utils/dynamic_config.h"
#include "utils/logger.h"

#include <toml++/toml.hpp>

#include <filesystem>
#include <sstream>

namespace sageFlow {
namespace test {

// ==================== IntegrationTestCase 实现 ====================

std::string IntegrationTestCase::summary() const {
    std::ostringstream oss;
    oss << "TestCase{name=" << name << ", algorithm=" << toString(strategy.algorithm)
        << ", sizes=[";
    for (size_t i = 0; i < data_sizes.size(); ++i) {
        if (i > 0) oss << ",";
        oss << data_sizes[i];
    }
    oss << "], parallelism=[";
    for (size_t i = 0; i < parallelism.size(); ++i) {
        if (i > 0) oss << ",";
        oss << parallelism[i];
    }
    oss << "], min_recall=" << expected_min_recall
        << ", enabled=" << (enabled ? "true" : "false") << "}";
    return oss.str();
}

std::vector<std::string> IntegrationTestCase::validate() const {
    std::vector<std::string> errors;

    if (name.empty()) {
        errors.push_back("Test name is required");
    }

    if (data_sizes.empty()) {
        errors.push_back("At least one data size is required");
    }

    if (parallelism.empty()) {
        errors.push_back("At least one parallelism value is required");
    }

    for (int size : data_sizes) {
        if (size <= 0) {
            errors.push_back("Data size must be positive, got: " + std::to_string(size));
            break;
        }
    }

    for (int p : parallelism) {
        if (p <= 0) {
            errors.push_back("Parallelism must be positive, got: " + std::to_string(p));
            break;
        }
    }

    if (vector_dim <= 0) {
        errors.push_back("Vector dimension must be positive, got: " +
                         std::to_string(vector_dim));
    }

    if (expected_min_recall < 0.0 || expected_min_recall > 1.0) {
        errors.push_back("Expected min recall must be in [0, 1], got: " +
                         std::to_string(expected_min_recall));
    }

    if (expected_min_precision < 0.0 || expected_min_precision > 1.0) {
        errors.push_back("Expected min precision must be in [0, 1], got: " +
                         std::to_string(expected_min_precision));
    }

    if (positive_ratio < 0.0 || positive_ratio > 1.0) {
        errors.push_back("Positive ratio must be in [0, 1], got: " +
                         std::to_string(positive_ratio));
    }

    if (negative_ratio < 0.0 || negative_ratio > 1.0) {
        errors.push_back("Negative ratio must be in [0, 1], got: " +
                         std::to_string(negative_ratio));
    }

    // 验证策略配置
    auto strategy_errors = strategy.validate();
    errors.insert(errors.end(), strategy_errors.begin(), strategy_errors.end());

    return errors;
}

bool IntegrationTestCase::isValid() const {
    return validate().empty();
}

// ==================== IntegrationTestConfigLoader 实现 ====================

std::string IntegrationTestConfigLoader::resolvePath(const std::string& path) {
    return DynamicConfigManager::resolveProjectRelativePath(path);
}

std::string IntegrationTestConfigLoader::getDefaultConfigPath() {
    return resolvePath("config/integration_test_cases.toml");
}

std::vector<int> IntegrationTestConfigLoader::parseIntArray(const toml::array& arr) {
    std::vector<int> result;
    for (const auto& elem : arr) {
        if (auto v = elem.value<int64_t>()) {
            result.push_back(static_cast<int>(*v));
        }
    }
    return result;
}

JoinStrategyConfig IntegrationTestConfigLoader::parseStrategyConfig(
    const toml::table& table) {
    JoinStrategyConfig config;

    // 算法类型
    if (auto algo = table["algorithm"].value<std::string>()) {
        config.algorithm = parseJoinAlgorithm(*algo);
    }

    // 分区策略
    if (auto ps = table["partition_strategy"].value<std::string>()) {
        config.partition_strategy = parsePartitionStrategy(*ps);
    }

    // 窗口状态类型
    if (auto ws = table["window_state_type"].value<std::string>()) {
        config.window_state_type = parseWindowStateType(*ws);
    }

    // 索引策略
    if (auto is = table["index_strategy"].value<std::string>()) {
        config.index_strategy = parseIndexStrategy(*is);
    }

    // is_eager 模式
    if (auto eager = table["is_eager"].value<bool>()) {
        config.is_eager = *eager;
    }

    // 基础参数
    if (auto v = table["similarity_threshold"].value<double>()) {
        config.similarity_threshold = *v;
    }
    if (auto v = table["dimension"].value<int64_t>()) {
        config.dimension = static_cast<int>(*v);
    }
    if (auto v = table["num_partitions"].value<int64_t>()) {
        config.num_partitions = static_cast<int>(*v);
    }
    if (auto v = table["window_size_ms"].value<int64_t>()) {
        config.window_size_ms = *v;
    }
    if (auto v = table["step_size_ms"].value<int64_t>()) {
        config.step_size_ms = *v;
    }

    // IVF 参数
    if (auto v = table["ivf_nlist"].value<int64_t>()) {
        config.ivf_nlist = static_cast<int>(*v);
    }
    if (auto v = table["ivf_nprobes"].value<int64_t>()) {
        config.ivf_nprobes = static_cast<int>(*v);
    }
    if (auto v = table["ivf_rebuild_threshold"].value<double>()) {
        config.ivf_rebuild_threshold = *v;
    }

    // HNSW 参数
    if (auto v = table["hnsw_m"].value<int64_t>()) {
        config.hnsw_m = static_cast<int>(*v);
    }
    if (auto v = table["hnsw_ef_construction"].value<int64_t>()) {
        config.hnsw_ef_construction = static_cast<int>(*v);
    }
    if (auto v = table["hnsw_ef_search"].value<int64_t>()) {
        config.hnsw_ef_search = static_cast<int>(*v);
    }

    // HDR-Tree 参数
    if (auto v = table["hdr_projected_dim"].value<int64_t>()) {
        config.hdr_projected_dim = static_cast<int>(*v);
    }
    if (auto v = table["hdr_max_node_size"].value<int64_t>()) {
        config.hdr_max_node_size = static_cast<int>(*v);
    }
    if (auto v = table["hdr_delta_buffer_size"].value<int64_t>()) {
        config.hdr_delta_buffer_size = static_cast<size_t>(*v);
    }
    if (auto v = table["hdr_pca_sample_size"].value<int64_t>()) {
        config.hdr_pca_sample_size = static_cast<int>(*v);
    }

    // S3J 参数
    if (auto v = table["s3j_num_centroids"].value<int64_t>()) {
        config.s3j_num_centroids = static_cast<int>(*v);
    }
    if (auto v = table["s3j_adapt_interval_ms"].value<int64_t>()) {
        config.s3j_adapt_interval_ms = *v;
    }
    if (auto v = table["s3j_load_threshold"].value<double>()) {
        config.s3j_load_threshold = *v;
    }
    if (auto v = table["s3j_enable_adaptive"].value<bool>()) {
        config.s3j_enable_adaptive = *v;
    }

    // ClusteredJoin 参数
    if (auto v = table["clustered_overlap_ratio"].value<double>()) {
        config.clustered_overlap_ratio = *v;
    }
    if (auto v = table["clustered_rebalance_threshold"].value<double>()) {
        config.clustered_rebalance_threshold = *v;
    }
    if (auto v = table["clustered_border_replication"].value<bool>()) {
        config.clustered_border_replication = *v;
    }
    if (auto v = table["clustered_training_samples"].value<int64_t>()) {
        config.clustered_training_samples = static_cast<int>(*v);
    }

    // VSJoin 参数
    if (auto v = table["vsjoin_num_hash_functions"].value<int64_t>()) {
        config.vsjoin_num_hash_functions = static_cast<int>(*v);
    }
    if (auto v = table["vsjoin_boundary_threshold"].value<double>()) {
        config.vsjoin_boundary_threshold = *v;
    }
    if (auto v = table["vsjoin_async_threads"].value<int64_t>()) {
        config.vsjoin_async_threads = static_cast<int>(*v);
    }
    if (auto v = table["vsjoin_allowed_lateness"].value<int64_t>()) {
        config.vsjoin_allowed_lateness = *v;
    }

    // 双层窗口参数
    if (auto v = table["two_tier_compact_threshold"].value<int64_t>()) {
        config.two_tier_compact_threshold = static_cast<size_t>(*v);
    }
    if (auto v = table["two_tier_enable_boundary_tracking"].value<bool>()) {
        config.two_tier_enable_boundary_tracking = *v;
    }

    return config;
}

IntegrationTestCase IntegrationTestConfigLoader::parseTestCase(
    const toml::table& table,
    const IntegrationTestCase& common) {
    IntegrationTestCase tc = common;  // 继承通用配置

    // 基本信息
    if (auto name = table["name"].value<std::string>()) {
        tc.name = *name;
    }
    if (auto desc = table["description"].value<std::string>()) {
        tc.description = *desc;
    }
    if (auto enabled = table["enabled"].value<bool>()) {
        tc.enabled = *enabled;
    }

    // 策略配置
    tc.strategy = parseStrategyConfig(table);
    
    // 如果通用配置中有策略配置，需要合并
    // 注意：这里需要保留通用配置中的基础值，仅覆盖测试用例中显式指定的值
    if (common.strategy.dimension > 0 && tc.strategy.dimension == 128) {
        // 如果测试用例没有指定维度，使用通用配置的维度
        if (!table["dimension"].value<int64_t>()) {
            tc.strategy.dimension = common.strategy.dimension;
        }
    }
    if (common.strategy.similarity_threshold > 0 && 
        tc.strategy.similarity_threshold == 0.8) {
        if (!table["similarity_threshold"].value<double>()) {
            tc.strategy.similarity_threshold = common.strategy.similarity_threshold;
        }
    }
    if (common.strategy.window_size_ms > 0 && tc.strategy.window_size_ms == 10000) {
        if (!table["window_size_ms"].value<int64_t>()) {
            tc.strategy.window_size_ms = common.strategy.window_size_ms;
        }
    }
    if (common.strategy.step_size_ms > 0 && tc.strategy.step_size_ms == 1000) {
        if (!table["step_size_ms"].value<int64_t>()) {
            tc.strategy.step_size_ms = common.strategy.step_size_ms;
        }
    }

    // 数据配置
    if (auto dim = table["vector_dim"].value<int64_t>()) {
        tc.vector_dim = static_cast<int>(*dim);
        tc.strategy.dimension = tc.vector_dim;
    } else if (tc.vector_dim != 128) {
        // 如果继承了通用配置的 vector_dim，同步到策略配置
        tc.strategy.dimension = tc.vector_dim;
    }

    if (auto* sizes_arr = table["data_sizes"].as_array()) {
        tc.data_sizes = parseIntArray(*sizes_arr);
    }

    if (auto* para_arr = table["parallelism"].as_array()) {
        tc.parallelism = parseIntArray(*para_arr);
    }

    // 数据生成配置
    if (auto v = table["positive_ratio"].value<double>()) {
        tc.positive_ratio = *v;
    }
    if (auto v = table["negative_ratio"].value<double>()) {
        tc.negative_ratio = *v;
    }
    if (auto v = table["time_interval_ms"].value<int64_t>()) {
        tc.time_interval_ms = *v;
    }
    if (auto v = table["seed"].value<int64_t>()) {
        tc.seed = static_cast<uint32_t>(*v);
    }
    if (auto v = table["base_timestamp"].value<int64_t>()) {
        tc.base_timestamp = *v;
    }
    
    // 数据生成高级配置
    if (auto v = table["positive_pairs"].value<int64_t>()) {
        tc.positive_pairs = static_cast<int>(*v);
    }
    if (auto v = table["near_threshold_pairs"].value<int64_t>()) {
        tc.near_threshold_pairs = static_cast<int>(*v);
    }
    if (auto v = table["negative_pairs"].value<int64_t>()) {
        tc.negative_pairs = static_cast<int>(*v);
    }
    if (auto v = table["random_tail"].value<int64_t>()) {
        tc.random_tail = static_cast<int>(*v);
    }
    if (auto v = table["alpha"].value<double>()) {
        tc.alpha = *v;
    }

    // 验证配置
    if (auto v = table["expected_min_recall"].value<double>()) {
        tc.expected_min_recall = *v;
    }
    if (auto v = table["expected_min_precision"].value<double>()) {
        tc.expected_min_precision = *v;
    }
    if (auto v = table["compare_with_ground_truth"].value<bool>()) {
        tc.compare_with_ground_truth = *v;
    }
    if (auto v = table["allow_approximate_match"].value<bool>()) {
        tc.allow_approximate_match = *v;
    }

    // 输出配置
    if (auto v = table["save_results"].value<bool>()) {
        tc.save_results = *v;
    }
    if (auto path = table["result_output_dir"].value<std::string>()) {
        tc.result_output_dir = resolvePath(*path);
    }
    if (auto v = table["generate_report"].value<bool>()) {
        tc.generate_report = *v;
    }

    return tc;
}

std::vector<IntegrationTestCase> IntegrationTestConfigLoader::loadFromFile(
    const std::string& config_path) {
    std::vector<IntegrationTestCase> cases;

    std::string resolved_path = resolvePath(config_path);

    try {
        auto config = toml::parse_file(resolved_path);

        // 加载通用配置
        IntegrationTestCase common;
        if (config.contains("common")) {
            if (auto* common_table = config["common"].as_table()) {
                common = parseTestCase(*common_table, IntegrationTestCase{});
            }
        }

        // 加载测试用例
        if (auto* arr = config["test_case"].as_array()) {
            for (const auto& elem : *arr) {
                if (auto* table = elem.as_table()) {
                    auto test_case = parseTestCase(*table, common);

                    // 验证配置
                    auto errors = test_case.validate();
                    if (!errors.empty()) {
                        SAGEFLOW_LOG_WARN("IntegrationTestConfig",
                                          "Skipping invalid test case '{}': {}",
                                          test_case.name, errors[0]);
                        continue;
                    }

                    cases.push_back(std::move(test_case));
                }
            }
        }

        SAGEFLOW_LOG_INFO("IntegrationTestConfig", "Loaded {} test cases from {}",
                          cases.size(), config_path);

    } catch (const toml::parse_error& e) {
        throw std::runtime_error("Failed to parse config '" + config_path +
                                 "': " + std::string(e.what()));
    }

    return cases;
}

std::vector<IntegrationTestCase> IntegrationTestConfigLoader::loadByAlgorithm(
    const std::string& config_path,
    JoinAlgorithm algorithm) {
    auto all_cases = loadFromFile(config_path);

    std::vector<IntegrationTestCase> filtered;
    for (auto& tc : all_cases) {
        if (tc.strategy.algorithm == algorithm) {
            filtered.push_back(std::move(tc));
        }
    }

    return filtered;
}

std::optional<IntegrationTestCase> IntegrationTestConfigLoader::loadByName(
    const std::string& config_path,
    const std::string& test_name) {
    auto all_cases = loadFromFile(config_path);

    for (auto& tc : all_cases) {
        if (tc.name == test_name) {
            return tc;
        }
    }

    return std::nullopt;
}

IntegrationTestCase IntegrationTestConfigLoader::loadCommonConfig(
    const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);

    try {
        auto config = toml::parse_file(resolved_path);

        if (config.contains("common")) {
            if (auto* common_table = config["common"].as_table()) {
                return parseTestCase(*common_table, IntegrationTestCase{});
            }
        }
    } catch (const toml::parse_error& e) {
        throw std::runtime_error("Failed to parse config '" + config_path +
                                 "': " + std::string(e.what()));
    }

    // 如果没有 [common] 节点，返回默认配置
    return IntegrationTestCase{};
}

std::vector<std::string> IntegrationTestConfigLoader::listTestCaseNames(
    const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);
    std::vector<std::string> names;

    try {
        auto config = toml::parse_file(resolved_path);

        if (auto* arr = config["test_case"].as_array()) {
            for (const auto& elem : *arr) {
                if (auto* table = elem.as_table()) {
                    if (auto name = (*table)["name"].value<std::string>()) {
                        names.push_back(*name);
                    }
                }
            }
        }

    } catch (const toml::parse_error& e) {
        throw std::runtime_error("Failed to parse config '" + config_path +
                                 "': " + std::string(e.what()));
    }

    return names;
}

bool IntegrationTestConfigLoader::isValidConfigFile(const std::string& config_path) {
    std::string resolved_path = resolvePath(config_path);

    if (!std::filesystem::exists(resolved_path)) {
        return false;
    }

    try {
        auto config = toml::parse_file(resolved_path);
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<IntegrationTestCase> IntegrationTestConfigLoader::loadEnabledTests(
    const std::string& config_path) {
    auto all_cases = loadFromFile(config_path);

    std::vector<IntegrationTestCase> enabled;
    for (auto& tc : all_cases) {
        if (tc.enabled) {
            enabled.push_back(std::move(tc));
        }
    }

    return enabled;
}

std::vector<IntegrationTestCase> IntegrationTestConfigLoader::filterByDataSize(
    const std::vector<IntegrationTestCase>& test_cases,
    int min_size,
    int max_size) {
    std::vector<IntegrationTestCase> filtered;

    for (const auto& tc : test_cases) {
        // 检查测试用例的数据规模是否在范围内
        bool has_valid_size = false;
        for (int size : tc.data_sizes) {
            if (size >= min_size && size <= max_size) {
                has_valid_size = true;
                break;
            }
        }

        if (has_valid_size) {
            filtered.push_back(tc);
        }
    }

    return filtered;
}

}  // namespace test
}  // namespace sageFlow

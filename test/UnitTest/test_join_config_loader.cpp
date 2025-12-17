/**
 * @file test_join_config_loader.cpp
 * @brief JoinConfigLoader 单元测试
 *
 * 测试 JoinConfigLoader 的配置加载、合并和保存功能。
 */

#include <gtest/gtest.h>

#include "test_utils/join_config_loader.h"
#include "operator/utils/join_strategy_config.h"

#include <filesystem>
#include <fstream>

namespace sageFlow {
namespace test {

class JoinConfigLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 获取默认配置路径
        default_config_path_ = JoinConfigLoader::getDefaultConfigPath();
    }

    void TearDown() override {
        // 清理测试生成的文件
        if (std::filesystem::exists(temp_config_path_)) {
            std::filesystem::remove(temp_config_path_);
        }
    }

    std::string default_config_path_;
    std::string temp_config_path_ = "/tmp/test_join_config_output.toml";
};

// ==================== 基本加载测试 ====================

TEST_F(JoinConfigLoaderTest, LoadFromFile_DefaultConfigPath) {
    // 验证默认配置文件存在
    ASSERT_TRUE(JoinConfigLoader::isValidConfigFile(default_config_path_))
        << "Default config file not found: " << default_config_path_;

    // 加载默认配置
    auto config = JoinConfigLoader::loadFromFile(default_config_path_);

    // 验证基本字段被加载
    EXPECT_GT(config.dimension, 0);
    EXPECT_GT(config.similarity_threshold, 0.0);
    EXPECT_LE(config.similarity_threshold, 1.0);
}

TEST_F(JoinConfigLoaderTest, LoadByName_BruteforceBaseline) {
    auto config = JoinConfigLoader::loadByName(default_config_path_, "bruteforce_baseline");

    EXPECT_EQ(config.algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
    EXPECT_EQ(config.index_strategy, IndexStrategy::SHARED);
}

TEST_F(JoinConfigLoaderTest, LoadByName_IvfBaseline) {
    auto config = JoinConfigLoader::loadByName(default_config_path_, "ivf_baseline");

    EXPECT_EQ(config.algorithm, JoinAlgorithm::IVF);
    EXPECT_GT(config.ivf_nlist, 0);
    EXPECT_GT(config.ivf_nprobes, 0);
    EXPECT_LE(config.ivf_nprobes, config.ivf_nlist);
}

TEST_F(JoinConfigLoaderTest, LoadByName_HnswBaseline) {
    auto config = JoinConfigLoader::loadByName(default_config_path_, "hnsw_baseline");

    EXPECT_EQ(config.algorithm, JoinAlgorithm::HNSW);
    EXPECT_GT(config.hnsw_m, 0);
    EXPECT_GT(config.hnsw_ef_construction, 0);
    EXPECT_GT(config.hnsw_ef_search, 0);
}

TEST_F(JoinConfigLoaderTest, LoadByName_NonExistent_ThrowsException) {
    EXPECT_THROW(
        JoinConfigLoader::loadByName(default_config_path_, "non_existent_strategy"),
        std::runtime_error);
}

// ==================== 批量加载测试 ====================

TEST_F(JoinConfigLoaderTest, LoadAllFromFile_ReturnsMultipleConfigs) {
    auto configs = JoinConfigLoader::loadAllFromFile(default_config_path_);

    // 至少有几个预定义的策略
    EXPECT_GE(configs.size(), 4u);

    // 验证包含不同的算法类型
    std::set<JoinAlgorithm> algorithms;
    for (const auto& c : configs) {
        algorithms.insert(c.algorithm);
    }

    EXPECT_TRUE(algorithms.count(JoinAlgorithm::BRUTEFORCE) > 0)
        << "Missing BruteForce algorithm";
    EXPECT_TRUE(algorithms.count(JoinAlgorithm::IVF) > 0) << "Missing IVF algorithm";
}

TEST_F(JoinConfigLoaderTest, LoadByNames_LoadsSpecificStrategies) {
    std::vector<std::string> names = {"bruteforce_baseline", "ivf_baseline"};
    auto configs = JoinConfigLoader::loadByNames(default_config_path_, names);

    EXPECT_EQ(configs.size(), 2u);
    EXPECT_EQ(configs[0].algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(configs[1].algorithm, JoinAlgorithm::IVF);
}

TEST_F(JoinConfigLoaderTest, LoadByAlgorithm_FiltersCorrectly) {
    auto ivf_configs =
        JoinConfigLoader::loadByAlgorithm(default_config_path_, JoinAlgorithm::IVF);

    EXPECT_GE(ivf_configs.size(), 1u);
    for (const auto& c : ivf_configs) {
        EXPECT_EQ(c.algorithm, JoinAlgorithm::IVF);
    }
}

// ==================== 策略名称列表测试 ====================

TEST_F(JoinConfigLoaderTest, ListStrategyNames_ReturnsNonEmpty) {
    auto names = JoinConfigLoader::listStrategyNames(default_config_path_);

    EXPECT_GE(names.size(), 4u);

    // 验证包含已知的策略名称
    auto contains = [&names](const std::string& name) {
        return std::find(names.begin(), names.end(), name) != names.end();
    };

    EXPECT_TRUE(contains("bruteforce_baseline"));
    EXPECT_TRUE(contains("ivf_baseline"));
}

// ==================== 配置合并测试 ====================

TEST_F(JoinConfigLoaderTest, Merge_OverrideAlgorithm) {
    JoinStrategyConfig base;
    base.dimension = 128;
    base.similarity_threshold = 0.8;
    base.algorithm = JoinAlgorithm::BRUTEFORCE;

    JoinStrategyConfig override_config;
    override_config.algorithm = JoinAlgorithm::IVF;
    override_config.ivf_nlist = 200;

    auto merged = JoinConfigLoader::merge(base, override_config);

    // 验证合并结果
    EXPECT_EQ(merged.dimension, 128);                   // 从 base
    EXPECT_EQ(merged.algorithm, JoinAlgorithm::IVF);   // 从 override
    EXPECT_EQ(merged.ivf_nlist, 200);                  // 从 override
}

TEST_F(JoinConfigLoaderTest, Merge_KeepsBaseWhenOverrideIsDefault) {
    JoinStrategyConfig base;
    base.dimension = 256;
    base.similarity_threshold = 0.9;
    base.ivf_nlist = 150;

    JoinStrategyConfig override_config;  // 全部默认值

    auto merged = JoinConfigLoader::merge(base, override_config);

    // 验证 base 的非默认值被保留
    EXPECT_EQ(merged.dimension, 256);
    EXPECT_DOUBLE_EQ(merged.similarity_threshold, 0.9);
    EXPECT_EQ(merged.ivf_nlist, 150);
}

TEST_F(JoinConfigLoaderTest, Merge_OverridesNumericParameters) {
    JoinStrategyConfig base;
    base.hnsw_m = 16;
    base.hnsw_ef_construction = 200;
    base.hnsw_ef_search = 50;

    JoinStrategyConfig override_config;
    override_config.hnsw_m = 32;
    override_config.hnsw_ef_search = 100;

    auto merged = JoinConfigLoader::merge(base, override_config);

    EXPECT_EQ(merged.hnsw_m, 32);              // 从 override
    EXPECT_EQ(merged.hnsw_ef_construction, 200);  // 从 base (override 是默认值)
    EXPECT_EQ(merged.hnsw_ef_search, 100);       // 从 override
}

// ==================== 保存和加载往返测试 ====================

TEST_F(JoinConfigLoaderTest, SaveToFile_CreatesValidFile) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.dimension = 256;
    config.similarity_threshold = 0.85;
    config.ivf_nlist = 150;
    config.ivf_nprobes = 15;

    // 保存配置
    JoinConfigLoader::saveToFile(config, temp_config_path_);

    // 验证文件存在且可解析
    ASSERT_TRUE(std::filesystem::exists(temp_config_path_));
    EXPECT_TRUE(JoinConfigLoader::isValidConfigFile(temp_config_path_));

    // 加载并验证
    auto loaded = JoinConfigLoader::loadFromFile(temp_config_path_);

    EXPECT_EQ(loaded.algorithm, JoinAlgorithm::IVF);
    EXPECT_EQ(loaded.dimension, 256);
    EXPECT_DOUBLE_EQ(loaded.similarity_threshold, 0.85);
    EXPECT_EQ(loaded.ivf_nlist, 150);
    EXPECT_EQ(loaded.ivf_nprobes, 15);
}

// ==================== 验证辅助函数测试 ====================

TEST_F(JoinConfigLoaderTest, IsValidConfigFile_ValidFile) {
    EXPECT_TRUE(JoinConfigLoader::isValidConfigFile(default_config_path_));
}

TEST_F(JoinConfigLoaderTest, IsValidConfigFile_NonExistentFile) {
    EXPECT_FALSE(JoinConfigLoader::isValidConfigFile("/non/existent/path.toml"));
}

TEST_F(JoinConfigLoaderTest, GetDefaultConfigPath_ReturnsValidPath) {
    auto path = JoinConfigLoader::getDefaultConfigPath();

    EXPECT_FALSE(path.empty());
    EXPECT_TRUE(path.find("join_strategies.toml") != std::string::npos);
}

// ==================== 算法类型解析测试 ====================

TEST_F(JoinConfigLoaderTest, ParseAllAlgorithmTypes) {
    const std::vector<std::pair<std::string, JoinAlgorithm>> cases = {
        {"bruteforce", JoinAlgorithm::BRUTEFORCE},
        {"ivf", JoinAlgorithm::IVF},
        {"hnsw", JoinAlgorithm::HNSW},
        {"hdr_tree", JoinAlgorithm::HDR_TREE},
        {"clustered_join", JoinAlgorithm::CLUSTERED_JOIN},
        {"s3j", JoinAlgorithm::S3J},
        {"vsjoin", JoinAlgorithm::VSJOIN},
    };

    for (const auto& [str, expected] : cases) {
        EXPECT_EQ(parseJoinAlgorithm(str), expected) << "Failed for: " << str;
    }
}

TEST_F(JoinConfigLoaderTest, ParseAllPartitionStrategies) {
    const std::vector<std::pair<std::string, PartitionStrategy>> cases = {
        {"round_robin", PartitionStrategy::ROUND_ROBIN},
        {"key_hash", PartitionStrategy::KEY_HASH},
        {"vector_hash", PartitionStrategy::VECTOR_HASH},
        {"lsh", PartitionStrategy::LSH},
        {"centroid", PartitionStrategy::CENTROID},
    };

    for (const auto& [str, expected] : cases) {
        EXPECT_EQ(parsePartitionStrategy(str), expected) << "Failed for: " << str;
    }
}

TEST_F(JoinConfigLoaderTest, ParseAllWindowStateTypes) {
    const std::vector<std::pair<std::string, WindowStateType>> cases = {
        {"shared", WindowStateType::SHARED},
        {"partitioned", WindowStateType::PARTITIONED},
        {"two_tier", WindowStateType::TWO_TIER},
        {"partitioned_vector", WindowStateType::PARTITIONED_VECTOR},
    };

    for (const auto& [str, expected] : cases) {
        EXPECT_EQ(parseWindowStateType(str), expected) << "Failed for: " << str;
    }
}

TEST_F(JoinConfigLoaderTest, ParseAllIndexStrategies) {
    const std::vector<std::pair<std::string, IndexStrategy>> cases = {
        {"shared", IndexStrategy::SHARED},
        {"partitioned", IndexStrategy::PARTITIONED},
    };

    for (const auto& [str, expected] : cases) {
        EXPECT_EQ(parseIndexStrategy(str), expected) << "Failed for: " << str;
    }
}

// ==================== 边界情况测试 ====================

TEST_F(JoinConfigLoaderTest, LoadByNames_WithEmptyList) {
    std::vector<std::string> empty_names;
    auto configs = JoinConfigLoader::loadByNames(default_config_path_, empty_names);

    EXPECT_TRUE(configs.empty());
}

TEST_F(JoinConfigLoaderTest, LoadByNames_WithPartiallyInvalidNames) {
    std::vector<std::string> names = {"bruteforce_baseline", "invalid_name"};
    auto configs = JoinConfigLoader::loadByNames(default_config_path_, names);

    // 应该只加载有效的配置
    EXPECT_EQ(configs.size(), 1u);
    EXPECT_EQ(configs[0].algorithm, JoinAlgorithm::BRUTEFORCE);
}

// ==================== VSJoin 特殊配置测试 ====================

TEST_F(JoinConfigLoaderTest, LoadVSJoinConfig_HasCorrectDefaults) {
    // 检查是否有 vsjoin 配置
    auto names = JoinConfigLoader::listStrategyNames(default_config_path_);
    bool has_vsjoin = std::find(names.begin(), names.end(), "vsjoin") != names.end();

    if (has_vsjoin) {
        auto config = JoinConfigLoader::loadByName(default_config_path_, "vsjoin");

        EXPECT_EQ(config.algorithm, JoinAlgorithm::VSJOIN);
        EXPECT_EQ(config.partition_strategy, PartitionStrategy::LSH);
        EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED_VECTOR);
        EXPECT_EQ(config.index_strategy, IndexStrategy::PARTITIONED);
        EXPECT_GT(config.vsjoin_num_hash_functions, 0);
    } else {
        GTEST_SKIP() << "vsjoin strategy not defined in config file";
    }
}

// ==================== S3J 特殊配置测试 ====================

TEST_F(JoinConfigLoaderTest, LoadS3JConfig_HasCorrectDefaults) {
    // 检查是否有 s3j 配置
    auto names = JoinConfigLoader::listStrategyNames(default_config_path_);
    bool has_s3j = std::find(names.begin(), names.end(), "s3j_baseline") != names.end();

    if (has_s3j) {
        auto config = JoinConfigLoader::loadByName(default_config_path_, "s3j_baseline");

        EXPECT_EQ(config.algorithm, JoinAlgorithm::S3J);
        EXPECT_EQ(config.partition_strategy, PartitionStrategy::CENTROID);
        EXPECT_GT(config.s3j_num_centroids, 0);
    } else {
        GTEST_SKIP() << "s3j_baseline strategy not defined in config file";
    }
}

}  // namespace test
}  // namespace sageFlow

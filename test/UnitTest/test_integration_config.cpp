/**
 * @file test_integration_config.cpp
 * @brief IntegrationTestConfigLoader 单元测试
 *
 * 测试 IntegrationTestConfigLoader 的配置加载、验证和过滤功能。
 */

#include <gtest/gtest.h>

#include "test_utils/integration_test_config.h"
#include "operator/utils/join_strategy_config.h"

#include <filesystem>
#include <fstream>

namespace sageFlow {
namespace test {

class IntegrationTestConfigLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 获取默认配置路径
        default_config_path_ = IntegrationTestConfigLoader::getDefaultConfigPath();
    }

    void TearDown() override {
        // 清理测试生成的临时文件
        if (std::filesystem::exists(temp_config_path_)) {
            std::filesystem::remove(temp_config_path_);
        }
    }

    std::string default_config_path_;
    std::string temp_config_path_ = "/tmp/test_integration_config.toml";
};

// ==================== 基本加载测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, LoadFromFile_DefaultConfigPath) {
    // 验证默认配置文件存在
    ASSERT_TRUE(IntegrationTestConfigLoader::isValidConfigFile(default_config_path_))
        << "Default config file not found: " << default_config_path_;

    // 加载所有测试用例
    auto cases = IntegrationTestConfigLoader::loadFromFile(default_config_path_);

    // 至少有 6 个测试用例（根据配置文件定义）
    EXPECT_GE(cases.size(), 6u);
}

TEST_F(IntegrationTestConfigLoaderTest, LoadFromFile_TestCasesHaveBasicFields) {
    auto cases = IntegrationTestConfigLoader::loadFromFile(default_config_path_);

    for (const auto& tc : cases) {
        // 每个测试用例应该有名称
        EXPECT_FALSE(tc.name.empty()) << "Test case has empty name";

        // 每个测试用例应该有数据规模配置
        EXPECT_FALSE(tc.data_sizes.empty())
            << "Test case '" << tc.name << "' has no data sizes";

        // 每个测试用例应该有并行度配置
        EXPECT_FALSE(tc.parallelism.empty())
            << "Test case '" << tc.name << "' has no parallelism values";

        // 向量维度应该大于 0
        EXPECT_GT(tc.vector_dim, 0)
            << "Test case '" << tc.name << "' has invalid vector_dim";
    }
}

TEST_F(IntegrationTestConfigLoaderTest, LoadFromFile_BruteforceBaseline) {
    auto cases = IntegrationTestConfigLoader::loadFromFile(default_config_path_);

    // 查找 bruteforce_baseline 测试用例
    auto it = std::find_if(cases.begin(), cases.end(),
                           [](const IntegrationTestCase& tc) {
                               return tc.name == "bruteforce_baseline";
                           });

    ASSERT_NE(it, cases.end()) << "bruteforce_baseline test case not found";

    auto& bf = *it;
    EXPECT_EQ(bf.strategy.algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(bf.strategy.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(bf.strategy.window_state_type, WindowStateType::SHARED);
    // BruteForce 在滑动窗口场景下的合理召回率阈值（配置文件设置）
    EXPECT_GE(bf.expected_min_recall, 0.80);
    EXPECT_LE(bf.expected_min_recall, 1.0);
    EXPECT_EQ(bf.expected_min_precision, 1.0);
}

// ==================== 按算法加载测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, LoadByAlgorithm_BruteForce) {
    auto cases = IntegrationTestConfigLoader::loadByAlgorithm(default_config_path_,
                                                              JoinAlgorithm::BRUTEFORCE);

    EXPECT_GE(cases.size(), 1u);
    for (const auto& tc : cases) {
        EXPECT_EQ(tc.strategy.algorithm, JoinAlgorithm::BRUTEFORCE);
    }
}

TEST_F(IntegrationTestConfigLoaderTest, LoadByAlgorithm_IVF) {
    auto cases =
        IntegrationTestConfigLoader::loadByAlgorithm(default_config_path_, JoinAlgorithm::IVF);

    EXPECT_GE(cases.size(), 1u);
    for (const auto& tc : cases) {
        EXPECT_EQ(tc.strategy.algorithm, JoinAlgorithm::IVF);
        // IVF 特定参数应该被设置
        EXPECT_GT(tc.strategy.ivf_nlist, 0);
        EXPECT_GT(tc.strategy.ivf_nprobes, 0);
    }
}

TEST_F(IntegrationTestConfigLoaderTest, LoadByAlgorithm_HNSW) {
    auto cases =
        IntegrationTestConfigLoader::loadByAlgorithm(default_config_path_, JoinAlgorithm::HNSW);

    EXPECT_GE(cases.size(), 1u);
    for (const auto& tc : cases) {
        EXPECT_EQ(tc.strategy.algorithm, JoinAlgorithm::HNSW);
        // HNSW 特定参数应该被设置
        EXPECT_GT(tc.strategy.hnsw_m, 0);
        EXPECT_GT(tc.strategy.hnsw_ef_construction, 0);
        EXPECT_GT(tc.strategy.hnsw_ef_search, 0);
    }
}

TEST_F(IntegrationTestConfigLoaderTest, LoadByAlgorithm_S3J) {
    auto cases =
        IntegrationTestConfigLoader::loadByAlgorithm(default_config_path_, JoinAlgorithm::S3J);

    EXPECT_GE(cases.size(), 1u);
    for (const auto& tc : cases) {
        EXPECT_EQ(tc.strategy.algorithm, JoinAlgorithm::S3J);
        // S3J 特定参数
        EXPECT_GT(tc.strategy.s3j_num_centroids, 0);
    }
}

TEST_F(IntegrationTestConfigLoaderTest, LoadByAlgorithm_VSJoin) {
    auto cases = IntegrationTestConfigLoader::loadByAlgorithm(default_config_path_,
                                                              JoinAlgorithm::VSJOIN);

    EXPECT_GE(cases.size(), 1u);
    for (const auto& tc : cases) {
        EXPECT_EQ(tc.strategy.algorithm, JoinAlgorithm::VSJOIN);
        EXPECT_EQ(tc.strategy.partition_strategy, PartitionStrategy::LSH);
        EXPECT_EQ(tc.strategy.window_state_type, WindowStateType::PARTITIONED_VECTOR);
    }
}

// ==================== 按名称加载测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, LoadByName_ExistingCase) {
    auto tc = IntegrationTestConfigLoader::loadByName(default_config_path_, "ivf_standard");

    ASSERT_TRUE(tc.has_value());
    EXPECT_EQ(tc->name, "ivf_standard");
    EXPECT_EQ(tc->strategy.algorithm, JoinAlgorithm::IVF);
}

TEST_F(IntegrationTestConfigLoaderTest, LoadByName_NonExistentCase) {
    auto tc =
        IntegrationTestConfigLoader::loadByName(default_config_path_, "non_existent_case");

    EXPECT_FALSE(tc.has_value());
}

TEST_F(IntegrationTestConfigLoaderTest, LoadByName_S3JAdaptive) {
    auto tc = IntegrationTestConfigLoader::loadByName(default_config_path_, "s3j_adaptive");

    ASSERT_TRUE(tc.has_value());
    EXPECT_EQ(tc->name, "s3j_adaptive");
    EXPECT_EQ(tc->strategy.algorithm, JoinAlgorithm::S3J);
    EXPECT_TRUE(tc->strategy.s3j_enable_adaptive);
}

// ==================== 通用配置继承测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, CommonConfigInherited) {
    auto cases = IntegrationTestConfigLoader::loadFromFile(default_config_path_);

    // 所有用例应该继承通用配置中的 vector_dim
    for (const auto& tc : cases) {
        EXPECT_EQ(tc.vector_dim, 128) << "Test case '" << tc.name
                                      << "' did not inherit common vector_dim";
    }
}

TEST_F(IntegrationTestConfigLoaderTest, LoadCommonConfig) {
    auto common = IntegrationTestConfigLoader::loadCommonConfig(default_config_path_);

    EXPECT_EQ(common.vector_dim, 128);
    EXPECT_EQ(common.strategy.similarity_threshold, 0.8);
    EXPECT_EQ(common.strategy.window_size_ms, 10000);
    EXPECT_EQ(common.seed, 42u);
}

// ==================== 测试用例验证测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, TestCaseValidate_ValidCase) {
    IntegrationTestCase tc;
    tc.name = "valid_test";
    tc.data_sizes = {100, 500};
    tc.parallelism = {1, 2};

    auto errors = tc.validate();
    EXPECT_TRUE(errors.empty()) << "Expected no errors, got: " << errors[0];
}

TEST_F(IntegrationTestConfigLoaderTest, TestCaseValidate_EmptyName) {
    IntegrationTestCase tc;
    tc.name = "";
    tc.data_sizes = {100};
    tc.parallelism = {1};

    auto errors = tc.validate();
    EXPECT_FALSE(errors.empty());
    EXPECT_NE(errors[0].find("name"), std::string::npos);
}

TEST_F(IntegrationTestConfigLoaderTest, TestCaseValidate_EmptyDataSizes) {
    IntegrationTestCase tc;
    tc.name = "test";
    tc.data_sizes = {};
    tc.parallelism = {1};

    auto errors = tc.validate();
    EXPECT_FALSE(errors.empty());
}

TEST_F(IntegrationTestConfigLoaderTest, TestCaseValidate_EmptyParallelism) {
    IntegrationTestCase tc;
    tc.name = "test";
    tc.data_sizes = {100};
    tc.parallelism = {};

    auto errors = tc.validate();
    EXPECT_FALSE(errors.empty());
}

TEST_F(IntegrationTestConfigLoaderTest, TestCaseValidate_NegativeDataSize) {
    IntegrationTestCase tc;
    tc.name = "test";
    tc.data_sizes = {-100};
    tc.parallelism = {1};

    auto errors = tc.validate();
    EXPECT_FALSE(errors.empty());
}

TEST_F(IntegrationTestConfigLoaderTest, TestCaseValidate_InvalidRecall) {
    IntegrationTestCase tc;
    tc.name = "test";
    tc.data_sizes = {100};
    tc.parallelism = {1};
    tc.expected_min_recall = 1.5;  // Invalid: > 1.0

    auto errors = tc.validate();
    EXPECT_FALSE(errors.empty());
}

// ==================== 测试用例 Summary 测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, TestCaseSummary) {
    IntegrationTestCase tc;
    tc.name = "summary_test";
    tc.strategy.algorithm = JoinAlgorithm::IVF;
    tc.data_sizes = {100, 500};
    tc.parallelism = {1, 2, 4};
    tc.expected_min_recall = 0.85;
    tc.enabled = true;

    std::string summary = tc.summary();

    EXPECT_NE(summary.find("summary_test"), std::string::npos);
    EXPECT_NE(summary.find("ivf"), std::string::npos);
    EXPECT_NE(summary.find("0.85"), std::string::npos);
}

// ==================== 列表和过滤测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, ListTestCaseNames) {
    auto names = IntegrationTestConfigLoader::listTestCaseNames(default_config_path_);

    EXPECT_GE(names.size(), 6u);
    EXPECT_TRUE(std::find(names.begin(), names.end(), "bruteforce_baseline") != names.end());
    EXPECT_TRUE(std::find(names.begin(), names.end(), "ivf_standard") != names.end());
}

TEST_F(IntegrationTestConfigLoaderTest, LoadEnabledTests) {
    auto enabled_cases = IntegrationTestConfigLoader::loadEnabledTests(default_config_path_);

    // 所有返回的用例应该是启用的
    for (const auto& tc : enabled_cases) {
        EXPECT_TRUE(tc.enabled) << "Test case '" << tc.name << "' should be enabled";
    }

    // 加载所有用例并检查禁用的用例被排除
    auto all_cases = IntegrationTestConfigLoader::loadFromFile(default_config_path_);
    
    // 统计禁用的用例数量
    int disabled_count = 0;
    for (const auto& tc : all_cases) {
        if (!tc.enabled) {
            disabled_count++;
        }
    }
    
    // enabled_cases 应该比 all_cases 少 disabled_count 个
    EXPECT_EQ(enabled_cases.size(), all_cases.size() - disabled_count);
}

TEST_F(IntegrationTestConfigLoaderTest, FilterByDataSize) {
    auto all_cases = IntegrationTestConfigLoader::loadFromFile(default_config_path_);

    // 过滤小数据规模的测试用例
    auto small_cases =
        IntegrationTestConfigLoader::filterByDataSize(all_cases, 0, 200);

    for (const auto& tc : small_cases) {
        bool has_small_size = false;
        for (int size : tc.data_sizes) {
            if (size <= 200) {
                has_small_size = true;
                break;
            }
        }
        EXPECT_TRUE(has_small_size) << "Test case '" << tc.name
                                    << "' should have at least one size <= 200";
    }
}

// ==================== 配置文件验证测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, IsValidConfigFile_ValidFile) {
    EXPECT_TRUE(IntegrationTestConfigLoader::isValidConfigFile(default_config_path_));
}

TEST_F(IntegrationTestConfigLoaderTest, IsValidConfigFile_NonExistentFile) {
    EXPECT_FALSE(IntegrationTestConfigLoader::isValidConfigFile("/nonexistent/path.toml"));
}

TEST_F(IntegrationTestConfigLoaderTest, IsValidConfigFile_InvalidToml) {
    // 创建一个无效的 TOML 文件
    std::ofstream ofs(temp_config_path_);
    ofs << "invalid toml { [ content";
    ofs.close();

    EXPECT_FALSE(IntegrationTestConfigLoader::isValidConfigFile(temp_config_path_));
}

// ==================== 错误处理测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, LoadFromFile_NonExistentFile) {
    EXPECT_THROW(IntegrationTestConfigLoader::loadFromFile("/nonexistent/config.toml"),
                 std::runtime_error);
}

TEST_F(IntegrationTestConfigLoaderTest, LoadFromFile_InvalidToml) {
    // 创建一个无效的 TOML 文件
    std::ofstream ofs(temp_config_path_);
    ofs << "invalid toml { [ content";
    ofs.close();

    EXPECT_THROW(IntegrationTestConfigLoader::loadFromFile(temp_config_path_),
                 std::runtime_error);
}

// ==================== 自定义配置文件测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, LoadFromFile_CustomConfig) {
    // 创建一个简单的测试配置文件
    std::ofstream ofs(temp_config_path_);
    ofs << R"(
[common]
vector_dim = 64
similarity_threshold = 0.9

[[test_case]]
name = "custom_test"
description = "Custom test case"
algorithm = "bruteforce"
data_sizes = [50, 100]
parallelism = [1, 2]
expected_min_recall = 1.0
)";
    ofs.close();

    auto cases = IntegrationTestConfigLoader::loadFromFile(temp_config_path_);

    ASSERT_EQ(cases.size(), 1u);
    EXPECT_EQ(cases[0].name, "custom_test");
    EXPECT_EQ(cases[0].vector_dim, 64);
    EXPECT_EQ(cases[0].strategy.similarity_threshold, 0.9);
    EXPECT_EQ(cases[0].strategy.algorithm, JoinAlgorithm::BRUTEFORCE);
}

// ==================== isValid 测试 ====================

TEST_F(IntegrationTestConfigLoaderTest, TestCaseIsValid) {
    IntegrationTestCase valid_tc;
    valid_tc.name = "valid";
    valid_tc.data_sizes = {100};
    valid_tc.parallelism = {1};

    EXPECT_TRUE(valid_tc.isValid());

    IntegrationTestCase invalid_tc;
    invalid_tc.name = "";  // Invalid: empty name
    invalid_tc.data_sizes = {100};
    invalid_tc.parallelism = {1};

    EXPECT_FALSE(invalid_tc.isValid());
}

}  // namespace test
}  // namespace sageFlow

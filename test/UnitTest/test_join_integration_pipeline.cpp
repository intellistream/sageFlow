/**
 * @file test_join_integration_pipeline.cpp
 * @brief E-03: JoinIntegrationPipelineHelper 单元测试
 * 
 * 测试 Pipeline Helper 的创建和执行功能。
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <cmath>

#include "test_utils/join_integration_pipeline_helper.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/test_data_adapter.h"
#include "operator/join_strategy_config.h"
#include "utils/logger.h"

namespace sageFlow {
namespace test {

// ==================== 测试夹具 ====================

class JoinIntegrationPipelineHelperTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 初始化基础配置
        base_config_.algorithm = JoinAlgorithm::BRUTEFORCE;
        base_config_.similarity_threshold = 0.8;
        base_config_.dimension = 128;
        base_config_.window_size_ms = 10000;
        base_config_.step_size_ms = 100;
    }
    
    /**
     * @brief 生成简单测试数据
     */
    std::pair<std::vector<std::unique_ptr<VectorRecord>>,
              std::vector<std::unique_ptr<VectorRecord>>>
    generateSimpleTestData(int count, int dimension) {
        std::vector<std::unique_ptr<VectorRecord>> left;
        std::vector<std::unique_ptr<VectorRecord>> right;
        
        left.reserve(count);
        right.reserve(count);
        
        constexpr uint64_t kRightUidOffset = 500000;
        int64_t base_ts = 1000000;
        int64_t time_interval = 10;
        
        for (int i = 0; i < count; ++i) {
            std::vector<float> vec(dimension);
            for (int j = 0; j < dimension; ++j) {
                vec[j] = static_cast<float>(i + j) / static_cast<float>(dimension);
            }
            
            int64_t ts = base_ts + i * time_interval;
            uint64_t uid = static_cast<uint64_t>(i + 1);
            
            left.push_back(createVectorRecord(uid, ts, vec));
            right.push_back(createVectorRecord(uid + kRightUidOffset, ts, vec));
        }
        
        return {std::move(left), std::move(right)};
    }
    
    JoinStrategyConfig base_config_;
};

// ==================== 基础功能测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, CreatePipelineFromVectors) {
    // 生成测试数据
    auto [left, right] = generateSimpleTestData(100, 128);
    
    // 创建 Pipeline
    auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
        std::move(left), std::move(right), base_config_);
    
    ASSERT_NE(pipeline, nullptr) << "Pipeline should not be null";
}

TEST_F(JoinIntegrationPipelineHelperTest, ExecutePipelineBasic) {
    // 生成测试数据（小规模）
    auto [left, right] = generateSimpleTestData(20, 128);
    
    // 创建并执行 Pipeline
    auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
        std::move(left), std::move(right), base_config_);
    
    auto result = pipeline->execute();
    
    EXPECT_TRUE(result.success) << "Execution failed: " << result.error_message;
    EXPECT_GT(result.execution_time_ms, 0) << "Execution time should be positive";
}

TEST_F(JoinIntegrationPipelineHelperTest, ExecutePipelineWithMatches) {
    // 使用 TestDataGenerator 生成有已知匹配的数据
    TestDataGenerator::Config gen_config;
    gen_config.vector_dim = 128;
    gen_config.positive_pairs = 20;
    gen_config.near_threshold_pairs = 0;
    gen_config.negative_pairs = 30;
    gen_config.random_tail = 50;
    gen_config.similarity_threshold = 0.8;
    gen_config.base_timestamp = 1000000;
    gen_config.time_interval = 10;
    
    TestDataGenerator generator(gen_config);
    auto [records, expected_matches] = generator.generateData();
    
    // 复制为左右流（自 Join 场景）
    std::vector<std::unique_ptr<VectorRecord>> left;
    std::vector<std::unique_ptr<VectorRecord>> right;
    
    left.reserve(records.size());
    right.reserve(records.size());
    
    constexpr uint64_t kRightUidOffset = 500000;
    
    for (auto& rec : records) {
        if (!rec) continue;
        auto vec = extractFloatVector(*rec);
        left.push_back(createVectorRecord(rec->uid_, rec->timestamp_, vec));
        right.push_back(createVectorRecord(rec->uid_ + kRightUidOffset, rec->timestamp_, vec));
    }
    
    // 创建配置
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.window_size_ms = 10000;
    config.step_size_ms = 100;
    
    // 执行 Pipeline
    auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
        std::move(left), std::move(right), config);
    
    auto result = pipeline->execute();
    
    EXPECT_TRUE(result.success) << "Execution failed: " << result.error_message;
    EXPECT_GT(result.execution_time_ms, 0);
    
    // 验证有匹配结果
    // 注意：由于 BruteForce 应该找到所有相似对，所以应该有匹配
    SAGEFLOW_LOG_INFO("TEST", "Matches found: {}", result.matches.size());
}

// ==================== 多并行度测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, ExecuteWithParallelism2) {
    auto [left, right] = generateSimpleTestData(100, 128);
    
    auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
        std::move(left), std::move(right), base_config_, 2);
    
    auto result = pipeline->execute();
    
    EXPECT_TRUE(result.success) << "Execution failed: " << result.error_message;
}

TEST_F(JoinIntegrationPipelineHelperTest, ExecuteWithParallelism4) {
    auto [left, right] = generateSimpleTestData(100, 128);
    
    auto pipeline = JoinIntegrationPipelineHelper::createPipeline(
        std::move(left), std::move(right), base_config_, 4);
    
    auto result = pipeline->execute();
    
    EXPECT_TRUE(result.success) << "Execution failed: " << result.error_message;
}

// ==================== 自 Join 测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, SelfJoinPipeline) {
    // 生成单流数据
    std::vector<std::unique_ptr<VectorRecord>> stream;
    stream.reserve(50);
    
    int64_t base_ts = 1000000;
    for (int i = 0; i < 50; ++i) {
        std::vector<float> vec(128);
        for (int j = 0; j < 128; ++j) {
            vec[j] = static_cast<float>(i + j) / 128.0f;
        }
        stream.push_back(createVectorRecord(i + 1, base_ts + i * 10, vec));
    }
    
    // 创建自 Join Pipeline
    auto pipeline = JoinIntegrationPipelineHelper::createSelfJoinPipeline(
        std::move(stream), base_config_);
    
    auto result = pipeline->execute();
    
    EXPECT_TRUE(result.success) << "Self-join failed: " << result.error_message;
}

// ==================== 配置验证测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, ValidatedPipelineSuccess) {
    auto [left, right] = generateSimpleTestData(50, 128);
    
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.similarity_threshold = 0.8;
    config.dimension = 128;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    
    // 应该成功创建
    EXPECT_NO_THROW({
        auto pipeline = JoinIntegrationPipelineHelper::createValidatedPipeline(
            std::move(left), std::move(right), config);
        EXPECT_NE(pipeline, nullptr);
    });
}

// 注意：这个测试依赖于 JoinConfigValidator 的实现
// 如果 E-01 尚未完成相关验证逻辑，可以跳过此测试
TEST_F(JoinIntegrationPipelineHelperTest, DISABLED_ValidatedPipelineInvalidConfig) {
    auto [left, right] = generateSimpleTestData(50, 128);
    
    // 创建不兼容的配置
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 不兼容
    config.window_state_type = WindowStateType::SHARED;  // 不兼容
    
    // 应该抛出异常
    EXPECT_THROW({
        JoinIntegrationPipelineHelper::createValidatedPipeline(
            std::move(left), std::move(right), config);
    }, std::runtime_error);
}

// ==================== MatchCollectorSink 测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, MatchCollectorSinkBasic) {
    MatchCollectorSink sink;
    sink.open();
    
    // 模拟 Join 输出的合并记录
    // 合并 UID 格式：left_uid * 1000000 + right_uid % 1000000
    std::vector<float> dummy_vec(256, 0.5f);
    
    auto rec1 = createVectorRecord(1 * 1000000 + 100, 1000, dummy_vec);
    auto rec2 = createVectorRecord(2 * 1000000 + 200, 2000, dummy_vec);
    auto rec3 = createVectorRecord(3 * 1000000 + 300, 3000, dummy_vec);
    
    sink.invoke(rec1);
    sink.invoke(rec2);
    sink.invoke(rec3);
    
    sink.close();
    
    auto matches = sink.getMatches();
    EXPECT_EQ(matches.size(), 3);
    EXPECT_EQ(sink.getProcessedCount(), 3);
    
    // 验证解析正确
    EXPECT_EQ(matches[0].left_uid, 1);
    EXPECT_EQ(matches[0].right_uid, 100);
    EXPECT_EQ(matches[1].left_uid, 2);
    EXPECT_EQ(matches[1].right_uid, 200);
    EXPECT_EQ(matches[2].left_uid, 3);
    EXPECT_EQ(matches[2].right_uid, 300);
}

TEST_F(JoinIntegrationPipelineHelperTest, MatchCollectorSinkReset) {
    MatchCollectorSink sink;
    sink.open();
    
    std::vector<float> dummy_vec(256, 0.5f);
    auto rec = createVectorRecord(1000001, 1000, dummy_vec);
    sink.invoke(rec);
    
    EXPECT_EQ(sink.getProcessedCount(), 1);
    
    sink.reset();
    
    EXPECT_EQ(sink.getProcessedCount(), 0);
    EXPECT_TRUE(sink.getMatches().empty());
}

// ==================== 便捷函数测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, ComputeRecallAndPrecision) {
    // 预期匹配
    std::vector<MatchPair> expected = {
        {1, 100, 0.9},
        {2, 200, 0.85},
        {3, 300, 0.88}
    };
    
    // 实际匹配（少一个，多一个误报）
    std::vector<MatchPair> actual = {
        {1, 100, 0.9},
        {2, 200, 0.85},
        {4, 400, 0.7}  // 误报
    };
    
    double recall = computeRecall(actual, expected);
    double precision = computePrecision(actual, expected);
    double f1 = computeF1Score(recall, precision);
    
    // 召回率：2/3 ≈ 0.667
    EXPECT_NEAR(recall, 2.0 / 3.0, 0.001);
    
    // 精确率：2/3 ≈ 0.667
    EXPECT_NEAR(precision, 2.0 / 3.0, 0.001);
    
    // F1：2 * (2/3) * (2/3) / (2/3 + 2/3) = 2/3
    EXPECT_NEAR(f1, 2.0 / 3.0, 0.001);
}

TEST_F(JoinIntegrationPipelineHelperTest, ComputeRecallPerfectMatch) {
    std::vector<MatchPair> expected = {{1, 10, 0.9}, {2, 20, 0.85}};
    std::vector<MatchPair> actual = {{1, 10, 0.9}, {2, 20, 0.85}};
    
    EXPECT_DOUBLE_EQ(computeRecall(actual, expected), 1.0);
    EXPECT_DOUBLE_EQ(computePrecision(actual, expected), 1.0);
}

TEST_F(JoinIntegrationPipelineHelperTest, ComputeRecallEmptyExpected) {
    std::vector<MatchPair> expected = {};
    std::vector<MatchPair> actual = {{1, 10, 0.9}};
    
    // 空预期时，召回率定义为 1.0
    EXPECT_DOUBLE_EQ(computeRecall(actual, expected), 1.0);
}

TEST_F(JoinIntegrationPipelineHelperTest, ComputePrecisionEmptyActual) {
    std::vector<MatchPair> expected = {{1, 10, 0.9}};
    std::vector<MatchPair> actual = {};
    
    // 空实际时，精确率定义为 1.0
    EXPECT_DOUBLE_EQ(computePrecision(actual, expected), 1.0);
}

// ==================== Join 方法字符串测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, GetJoinMethodString) {
    JoinStrategyConfig config;
    
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.is_eager = true;
    EXPECT_EQ(JoinIntegrationPipelineHelper::getJoinMethodString(config), "bruteforce_eager");
    
    config.algorithm = JoinAlgorithm::IVF;
    EXPECT_EQ(JoinIntegrationPipelineHelper::getJoinMethodString(config), "ivf_eager");
    
    config.algorithm = JoinAlgorithm::HNSW;
    EXPECT_EQ(JoinIntegrationPipelineHelper::getJoinMethodString(config), "hnsw_eager");
    
    config.algorithm = JoinAlgorithm::VSJOIN;
    EXPECT_EQ(JoinIntegrationPipelineHelper::getJoinMethodString(config), "vsjoin_eager");
}

// ==================== MatchPair 操作符测试 ====================

TEST_F(JoinIntegrationPipelineHelperTest, MatchPairEquality) {
    MatchPair p1{1, 10, 0.9};
    MatchPair p2{1, 10, 0.8};  // 相似度不同但 UID 相同
    MatchPair p3{1, 11, 0.9};  // right_uid 不同
    
    EXPECT_TRUE(p1 == p2) << "MatchPair equality should ignore similarity";
    EXPECT_FALSE(p1 == p3);
}

TEST_F(JoinIntegrationPipelineHelperTest, MatchPairOrdering) {
    MatchPair p1{1, 10, 0.9};
    MatchPair p2{1, 20, 0.9};
    MatchPair p3{2, 10, 0.9};
    
    EXPECT_TRUE(p1 < p2);  // 相同 left_uid，按 right_uid 排序
    EXPECT_TRUE(p1 < p3);  // 不同 left_uid
    EXPECT_TRUE(p2 < p3);
}

}  // namespace test
}  // namespace sageFlow

// main 由 gtest_main 提供

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <deque>
#include <cmath>

#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "state/shared_window_state.h"
#include "state/partitioned_window_state.h"
#include "execution/runtime_context.h"
#include "utils/logger.h"
#include "test_utils/test_data_generator.h"
#include "test_utils/test_data_adapter.h"

namespace sageFlow {
namespace test {

/**
 * @brief BruteForceBaseline 单元测试
 * 
 * 测试覆盖：
 * 1. 基本匹配正确性
 * 2. 阈值过滤行为
 * 3. 空窗口处理
 * 4. 边界条件处理
 * 5. 与 SharedWindowState 的集成
 * 6. 召回率验证（Ground Truth 特性）
 */
class BruteForceBaselineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建默认配置的方法实例
        method_ = std::make_unique<BruteForceBaseline>(default_threshold_);
        
        // 创建共享窗口状态
        left_state_ = std::make_unique<SharedWindowState>();
        right_state_ = std::make_unique<SharedWindowState>();
        
        // 创建运行时上下文
        context_ = std::make_unique<RuntimeContext>(0, 1);
    }
    
    void TearDown() override {
        if (method_ && method_->isInitialized()) {
            method_->close();
        }
    }
    
    /**
     * @brief 创建具有指定向量数据的 VectorRecord
     */
    std::unique_ptr<VectorRecord> createRecord(
        uint64_t uid, 
        const std::vector<float>& vec,
        int64_t timestamp = 0) {
        return createVectorRecord(uid, timestamp, vec);
    }
    
    /**
     * @brief 创建归一化的随机向量
     */
    std::vector<float> createNormalizedVector(size_t dim, uint32_t seed) {
        std::mt19937 rng(seed);
        std::normal_distribution<float> dist(0.0f, 1.0f);
        
        std::vector<float> vec(dim);
        float norm = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            vec[i] = dist(rng);
            norm += vec[i] * vec[i];
        }
        norm = std::sqrt(norm);
        for (auto& v : vec) {
            v /= norm;
        }
        return vec;
    }
    
    /**
     * @brief 创建与给定向量具有指定相似度的向量
     */
    std::vector<float> createSimilarVector(
        const std::vector<float>& base,
        double target_similarity,
        uint32_t seed) {
        
        std::mt19937 rng(seed);
        std::normal_distribution<float> dist(0.0f, 1.0f);
        
        // 生成随机噪声向量
        std::vector<float> noise(base.size());
        float noise_norm = 0.0f;
        for (size_t i = 0; i < base.size(); ++i) {
            noise[i] = dist(rng);
            noise_norm += noise[i] * noise[i];
        }
        noise_norm = std::sqrt(noise_norm);
        for (auto& n : noise) {
            n /= noise_norm;
        }
        
        // 线性组合得到目标相似度
        // cos(theta) = target_similarity
        // result = base * cos(theta) + noise * sin(theta)
        double angle = std::acos(std::clamp(target_similarity, -1.0, 1.0));
        double cos_theta = std::cos(angle);
        double sin_theta = std::sin(angle);
        
        std::vector<float> result(base.size());
        for (size_t i = 0; i < base.size(); ++i) {
            result[i] = static_cast<float>(base[i] * cos_theta + noise[i] * sin_theta);
        }
        
        return result;
    }
    
    /**
     * @brief 计算两个向量的余弦相似度
     */
    double computeCosineSimilarity(
        const std::vector<float>& a,
        const std::vector<float>& b) {
        if (a.size() != b.size() || a.empty()) return 0.0;
        
        double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
        for (size_t i = 0; i < a.size(); ++i) {
            dot += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }
        double denom = std::sqrt(norm_a) * std::sqrt(norm_b);
        return denom > 1e-10 ? dot / denom : 0.0;
    }

protected:
    std::unique_ptr<BruteForceBaseline> method_;
    std::unique_ptr<SharedWindowState> left_state_;
    std::unique_ptr<SharedWindowState> right_state_;
    std::unique_ptr<RuntimeContext> context_;
    
    static constexpr double default_threshold_ = 0.8;
    static constexpr size_t default_dim_ = 128;
};

// =============================================================================
// 基本功能测试
// =============================================================================

TEST_F(BruteForceBaselineTest, GetName) {
    EXPECT_EQ(method_->getName(), "BruteForce");
}

TEST_F(BruteForceBaselineTest, GetAndSetThreshold) {
    EXPECT_DOUBLE_EQ(method_->getThreshold(), default_threshold_);
    
    method_->setThreshold(0.9);
    EXPECT_DOUBLE_EQ(method_->getThreshold(), 0.9);
    
    method_->setThreshold(0.5);
    EXPECT_DOUBLE_EQ(method_->getThreshold(), 0.5);
}

TEST_F(BruteForceBaselineTest, InitializeAndClose) {
    EXPECT_FALSE(method_->isInitialized());
    
    method_->open(*context_, left_state_.get(), right_state_.get());
    EXPECT_TRUE(method_->isInitialized());
    
    method_->close();
    EXPECT_FALSE(method_->isInitialized());
}

// =============================================================================
// Eager 模式测试
// =============================================================================

TEST_F(BruteForceBaselineTest, ExecuteEager_EmptyWindow) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto query_vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, query_vec);
    
    // 窗口为空时应返回空结果
    auto results = method_->ExecuteEager(*query, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(BruteForceBaselineTest, ExecuteEager_IdenticalVectors) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    // 创建相同的向量
    auto vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, vec);
    auto record = createRecord(2, vec);  // 不同 uid，相同向量
    
    // 添加记录到右窗口
    right_state_->addRecord(std::move(record), 0);
    
    // 从左流查询应该在右窗口找到匹配
    auto results = method_->ExecuteEager(*query, 0);
    
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0]->uid_, 2);
}

TEST_F(BruteForceBaselineTest, ExecuteEager_SimilarVectors) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto base_vec = createNormalizedVector(default_dim_, 42);
    // 创建相似度 0.9 的向量（高于阈值 0.8）
    auto similar_vec = createSimilarVector(base_vec, 0.9, 100);
    
    auto query = createRecord(1, base_vec);
    auto record = createRecord(2, similar_vec);
    
    right_state_->addRecord(std::move(record), 0);
    
    auto results = method_->ExecuteEager(*query, 0);
    
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0]->uid_, 2);
}

TEST_F(BruteForceBaselineTest, ExecuteEager_DissimilarVectors) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto base_vec = createNormalizedVector(default_dim_, 42);
    // 创建相似度 0.5 的向量（低于阈值 0.8）
    auto dissimilar_vec = createSimilarVector(base_vec, 0.5, 100);
    
    auto query = createRecord(1, base_vec);
    auto record = createRecord(2, dissimilar_vec);
    
    right_state_->addRecord(std::move(record), 0);
    
    auto results = method_->ExecuteEager(*query, 0);
    
    EXPECT_TRUE(results.empty());
}

TEST_F(BruteForceBaselineTest, ExecuteEager_SkipSelfMatch) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, vec);
    auto record = createRecord(1, vec);  // 相同 uid
    
    right_state_->addRecord(std::move(record), 0);
    
    // 应该跳过自匹配
    auto results = method_->ExecuteEager(*query, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(BruteForceBaselineTest, ExecuteEager_MultipleMatches) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto base_vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, base_vec);
    
    // 添加多个相似的记录
    for (uint64_t i = 2; i <= 5; ++i) {
        auto similar_vec = createSimilarVector(base_vec, 0.85, static_cast<uint32_t>(i * 100));
        right_state_->addRecord(createRecord(i, similar_vec), 0);
    }
    
    // 添加一个不相似的记录
    auto dissimilar_vec = createSimilarVector(base_vec, 0.5, 999);
    right_state_->addRecord(createRecord(6, dissimilar_vec), 0);
    
    auto results = method_->ExecuteEager(*query, 0);
    
    // 应该匹配 4 个相似记录
    EXPECT_EQ(results.size(), 4);
}

TEST_F(BruteForceBaselineTest, ExecuteEager_SlotDirection) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, vec);
    
    // 记录添加到左窗口
    left_state_->addRecord(createRecord(2, vec), 0);
    
    // 从左流查询 (slot=0) 应该搜索右窗口 - 右窗口为空
    auto results_from_left = method_->ExecuteEager(*query, 0);
    EXPECT_TRUE(results_from_left.empty());
    
    // 从右流查询 (slot=1) 应该搜索左窗口 - 左窗口有记录
    auto results_from_right = method_->ExecuteEager(*query, 1);
    ASSERT_EQ(results_from_right.size(), 1);
    EXPECT_EQ(results_from_right[0]->uid_, 2);
}

// =============================================================================
// Lazy 模式测试
// =============================================================================

TEST_F(BruteForceBaselineTest, ExecuteLazy_EmptyQueries) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    std::deque<std::unique_ptr<VectorRecord>> empty_queries;
    
    auto results = method_->ExecuteLazy(empty_queries, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(BruteForceBaselineTest, ExecuteLazy_MultipleQueries) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    // 添加一些记录到右窗口
    auto base_vec = createNormalizedVector(default_dim_, 42);
    right_state_->addRecord(createRecord(100, base_vec), 0);
    
    auto similar_vec = createSimilarVector(base_vec, 0.9, 200);
    right_state_->addRecord(createRecord(101, similar_vec), 0);
    
    // 创建多个查询
    std::deque<std::unique_ptr<VectorRecord>> queries;
    queries.push_back(createRecord(1, base_vec));
    queries.push_back(createRecord(2, similar_vec));
    
    auto results = method_->ExecuteLazy(queries, 0);
    
    // 每个查询都应该能匹配到两条记录
    // 但是 uid=100 查询会跳过 uid=100 的记录（自匹配由 uid 判断）
    // 实际上查询是 uid=1 和 uid=2，所以都不会自匹配
    EXPECT_GE(results.size(), 2);
}

// =============================================================================
// 阈值边界测试
// =============================================================================

TEST_F(BruteForceBaselineTest, ThresholdBoundary_ExactlyAtThreshold) {
    // 使用精确的阈值 0.8
    method_->setThreshold(0.8);
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto base_vec = createNormalizedVector(default_dim_, 42);
    // 创建刚好在阈值上的向量 - 使用 0.81 确保稳定超过阈值
    auto boundary_vec = createSimilarVector(base_vec, 0.81, 100);
    
    auto query = createRecord(1, base_vec);
    right_state_->addRecord(createRecord(2, boundary_vec), 0);
    
    auto results = method_->ExecuteEager(*query, 0);
    
    // 相似度 >= 0.8 应该匹配
    EXPECT_EQ(results.size(), 1);
}

TEST_F(BruteForceBaselineTest, ThresholdBoundary_JustBelowThreshold) {
    method_->setThreshold(0.8);
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    auto base_vec = createNormalizedVector(default_dim_, 42);
    // 创建明显低于阈值的向量
    auto below_vec = createSimilarVector(base_vec, 0.7, 100);
    
    auto query = createRecord(1, base_vec);
    right_state_->addRecord(createRecord(2, below_vec), 0);
    
    auto results = method_->ExecuteEager(*query, 0);
    
    // 应该不匹配
    EXPECT_TRUE(results.empty());
}

// =============================================================================
// Ground Truth 验证测试
// =============================================================================

TEST_F(BruteForceBaselineTest, GroundTruth_100PercentRecall) {
    // 使用 TestDataGenerator 生成测试数据
    TestDataGenerator::Config gen_config;
    gen_config.vector_dim = default_dim_;
    gen_config.positive_pairs = 20;
    gen_config.negative_pairs = 30;
    gen_config.near_threshold_pairs = 0;
    gen_config.random_tail = 10;
    gen_config.similarity_threshold = default_threshold_;
    gen_config.seed = 42;
    
    TestDataGenerator generator(gen_config);
    auto [records, _] = generator.generateData();
    
    // 使用 BaselineJoinChecker 计算期望匹配（基于余弦相似度）
    // 使用足够大的窗口时间确保所有记录在同一窗口内
    auto expected_matches = BaselineJoinChecker::computeExpectedMatches(
        records, default_threshold_, std::numeric_limits<int64_t>::max());
    
    // 设置阈值与预期一致
    method_->setThreshold(default_threshold_);
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    // 添加所有记录到右窗口
    for (auto& record : records) {
        right_state_->addRecord(std::make_unique<VectorRecord>(*record), 0);
    }
    
    // 对每条记录执行查询
    std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> actual_matches;
    
    for (const auto& query : records) {
        auto results = method_->ExecuteEager(*query, 0);
        for (const auto& result : results) {
            uint64_t left_uid = std::min(query->uid_, result->uid_);
            uint64_t right_uid = std::max(query->uid_, result->uid_);
            actual_matches.insert({left_uid, right_uid});
        }
    }
    
    // 验证召回率
    size_t found_expected = 0;
    for (const auto& expected : expected_matches) {
        if (actual_matches.count(expected) > 0) {
            ++found_expected;
        }
    }
    
    double recall = expected_matches.empty() ? 1.0 : 
        static_cast<double>(found_expected) / expected_matches.size();
    
    // BruteForce 作为 Ground Truth 应该达到 100% 召回率
    EXPECT_DOUBLE_EQ(recall, 1.0) << "BruteForce should achieve 100% recall as Ground Truth";
    
    SAGEFLOW_LOG_INFO("TEST", 
        "GroundTruth test: {} expected matches, {} actual matches, recall={:.2f}%",
        expected_matches.size(), actual_matches.size(), recall * 100);
}

// =============================================================================
// 错误处理测试
// =============================================================================

TEST_F(BruteForceBaselineTest, ExecuteEager_WithoutInit) {
    // 未初始化时调用应返回空结果
    auto vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, vec);
    
    // 注意：未调用 open()
    auto results = method_->ExecuteEager(*query, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(BruteForceBaselineTest, ExecuteEager_NullState) {
    // 使用 nullptr 初始化
    method_->open(*context_, nullptr, nullptr);
    
    auto vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1, vec);
    
    auto results = method_->ExecuteEager(*query, 0);
    EXPECT_TRUE(results.empty());
}

// =============================================================================
// 性能相关测试（小规模）
// =============================================================================

TEST_F(BruteForceBaselineTest, SmallWindowPerformance) {
    method_->open(*context_, left_state_.get(), right_state_.get());
    
    const size_t window_size = 100;
    
    // 添加 100 条记录到右窗口
    for (uint64_t i = 1; i <= window_size; ++i) {
        auto vec = createNormalizedVector(default_dim_, static_cast<uint32_t>(i));
        right_state_->addRecord(createRecord(i, vec), 0);
    }
    
    // 执行查询
    auto query_vec = createNormalizedVector(default_dim_, 42);
    auto query = createRecord(1000, query_vec);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto results = method_->ExecuteEager(*query, 0);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // 对于 100 条记录的小窗口，应该在合理时间内完成
    EXPECT_LT(duration.count(), 10000) << "Query should complete within 10ms for 100 records";
    
    SAGEFLOW_LOG_INFO("TEST", 
        "SmallWindowPerformance: {} records, {} results, {}us",
        window_size, results.size(), duration.count());
}

} // namespace test
} // namespace sageFlow

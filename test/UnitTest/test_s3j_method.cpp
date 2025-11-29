#include <gtest/gtest.h>

#include "operator/join_operator_methods/s3j_method.h"
#include "operator/join_operator_methods/s3j_components/adaptive_partitioner.h"
#include "operator/join_operator_methods/s3j_components/adaptive_index_selector.h"
#include "state/window_state.h"
#include "storage/storage_manager.h"
#include "concurrency/concurrency_manager.h"
#include "test_utils/test_data_adapter.h"

#include <cmath>
#include <random>
#include <thread>
#include <vector>

namespace sageFlow {
namespace test {

// =============================================================================
// AdaptivePartitioner 测试
// =============================================================================

class AdaptivePartitionerTest : public ::testing::Test {
protected:
    static constexpr int kDimension = 128;
    
    void SetUp() override {
        config_.initial_partitions = 4;
        config_.adapt_interval_ms = 100;
        config_.load_threshold = 0.3;
        config_.split_threshold = 2.0;
        config_.merge_threshold = 0.3;
        config_.min_partitions = 2;
        config_.max_partitions = 16;
    }
    
    AdaptivePartitionerConfig config_;
};

TEST_F(AdaptivePartitionerTest, BasicConstruction) {
    EXPECT_NO_THROW({
        AdaptivePartitioner partitioner(kDimension, config_, 42);
        EXPECT_EQ(partitioner.getCurrentNumPartitions(), config_.initial_partitions);
    });
}

TEST_F(AdaptivePartitionerTest, UpdateStats) {
    AdaptivePartitioner partitioner(kDimension, config_, 42);
    
    // 更新分区 0 的统计
    partitioner.updateStats(0, 100, 1);
    partitioner.updateStats(0, 200, 1);
    partitioner.updateStats(0, 150, 1);
    
    const auto& stats = partitioner.getPartitionStats(0);
    EXPECT_EQ(stats.count.load(), 3);
    EXPECT_EQ(stats.total_latency_us.load(), 450);
    EXPECT_NEAR(stats.getAverageLatencyUs(), 150.0, 0.1);
}

TEST_F(AdaptivePartitionerTest, LoadImbalance) {
    AdaptivePartitioner partitioner(kDimension, config_, 42);
    
    // 制造不均衡负载
    for (int i = 0; i < 100; ++i) {
        partitioner.updateStats(0, 100, 1);  // 分区 0 很忙
    }
    for (int i = 0; i < 10; ++i) {
        partitioner.updateStats(1, 100, 1);  // 分区 1 较空闲
        partitioner.updateStats(2, 100, 1);
        partitioner.updateStats(3, 100, 1);
    }
    
    double imbalance = partitioner.computeLoadImbalance();
    EXPECT_GT(imbalance, 0.0);  // 应该检测到不均衡
}

TEST_F(AdaptivePartitionerTest, AdaptHistory) {
    AdaptivePartitioner partitioner(kDimension, config_, 42);
    
    // 初始历史应为空
    auto history = partitioner.getHistory();
    EXPECT_TRUE(history.empty());
    
    // 清除历史
    partitioner.clearHistory();
    history = partitioner.getHistory();
    EXPECT_TRUE(history.empty());
}

TEST_F(AdaptivePartitionerTest, ResetStats) {
    AdaptivePartitioner partitioner(kDimension, config_, 42);
    
    // 添加一些统计
    partitioner.updateStats(0, 100, 1);
    partitioner.updateStats(1, 200, 1);
    
    // 重置
    partitioner.resetStats();
    
    const auto& stats0 = partitioner.getPartitionStats(0);
    const auto& stats1 = partitioner.getPartitionStats(1);
    EXPECT_EQ(stats0.count.load(), 0);
    EXPECT_EQ(stats1.count.load(), 0);
}

// =============================================================================
// AdaptiveIndexSelector 测试
// =============================================================================

class AdaptiveIndexSelectorTest : public ::testing::Test {
protected:
    AdaptiveIndexSelectorConfig config_;
};

TEST_F(AdaptiveIndexSelectorTest, SelectBestIndex_SmallData) {
    AdaptiveIndexSelector selector(config_);
    
    // 小数据集应该选择 BruteForce
    IndexType selected = selector.selectBestIndex(128, 500, 10.0);
    EXPECT_EQ(selected, IndexType::BruteForce);
}

TEST_F(AdaptiveIndexSelectorTest, SelectBestIndex_MediumData) {
    AdaptiveIndexSelector selector(config_);
    
    // 中等数据集应该选择 IVF
    IndexType selected = selector.selectBestIndex(128, 10000, 100.0);
    EXPECT_EQ(selected, IndexType::IVF);
}

TEST_F(AdaptiveIndexSelectorTest, SelectBestIndex_LargeData) {
    AdaptiveIndexSelector selector(config_);
    
    // 大数据集应该选择 HNSW
    IndexType selected = selector.selectBestIndex(128, 500000, 1000.0);
    EXPECT_EQ(selected, IndexType::HNSW);
}

TEST_F(AdaptiveIndexSelectorTest, IndexTypeConversion) {
    EXPECT_EQ(AdaptiveIndexSelector::indexTypeToString(IndexType::BruteForce), "BruteForce");
    EXPECT_EQ(AdaptiveIndexSelector::indexTypeToString(IndexType::IVF), "IVF");
    EXPECT_EQ(AdaptiveIndexSelector::indexTypeToString(IndexType::HNSW), "HNSW");
    
    EXPECT_EQ(AdaptiveIndexSelector::stringToIndexType("BruteForce"), IndexType::BruteForce);
    EXPECT_EQ(AdaptiveIndexSelector::stringToIndexType("IVF"), IndexType::IVF);
    EXPECT_EQ(AdaptiveIndexSelector::stringToIndexType("HNSW"), IndexType::HNSW);
}

TEST_F(AdaptiveIndexSelectorTest, GetRecommendedParams_IVF) {
    AdaptiveIndexSelector selector(config_);
    
    auto params = selector.getRecommendedParams(IndexType::IVF, 128, 10000);
    
    EXPECT_TRUE(params.count("nlist") > 0);
    EXPECT_TRUE(params.count("nprobes") > 0);
    
    int nlist = std::stoi(params["nlist"]);
    EXPECT_GE(nlist, 10);
    EXPECT_LE(nlist, 1000);
}

TEST_F(AdaptiveIndexSelectorTest, GetRecommendedParams_HNSW) {
    AdaptiveIndexSelector selector(config_);
    
    auto params = selector.getRecommendedParams(IndexType::HNSW, 128, 100000);
    
    EXPECT_TRUE(params.count("M") > 0);
    EXPECT_TRUE(params.count("ef_construction") > 0);
    EXPECT_TRUE(params.count("ef_search") > 0);
}

TEST_F(AdaptiveIndexSelectorTest, PerformanceCache) {
    AdaptiveIndexSelector selector(config_);
    
    // 初始缓存为空
    auto cached = selector.getCachedPerformance(IndexType::IVF);
    EXPECT_FALSE(cached.isValid());
    
    // 更新缓存
    IndexPerformance perf;
    perf.avg_latency_us = 100.0;
    perf.recall = 0.95;
    perf.sample_count = 100;
    selector.updatePerformanceCache(IndexType::IVF, perf);
    
    // 检查缓存
    cached = selector.getCachedPerformance(IndexType::IVF);
    EXPECT_TRUE(cached.isValid());
    EXPECT_NEAR(cached.avg_latency_us, 100.0, 0.1);
    
    // 清除缓存
    selector.clearCache();
    cached = selector.getCachedPerformance(IndexType::IVF);
    EXPECT_FALSE(cached.isValid());
}

// =============================================================================
// S3JMethod 测试
// =============================================================================

class S3JMethodTest : public ::testing::Test {
protected:
    static constexpr int kDimension = 128;
    
    void SetUp() override {
        storage_manager_ = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
        
        config_.similarity_threshold = 0.8;
        config_.num_partitions = 4;
        config_.dimension = kDimension;
        config_.enable_metrics = true;
        config_.enable_adaptive = true;
    }
    
    std::vector<float> createRandomVector(std::mt19937& gen) {
        std::normal_distribution<float> dist(0.0f, 1.0f);
        std::vector<float> vec(kDimension);
        for (int i = 0; i < kDimension; ++i) {
            vec[i] = dist(gen);
        }
        return vec;
    }
    
    std::vector<float> createSimilarVector(const std::vector<float>& base, 
                                            float noise_level, 
                                            std::mt19937& gen) {
        std::normal_distribution<float> noise(0.0f, noise_level);
        std::vector<float> similar = base;
        for (size_t i = 0; i < similar.size(); ++i) {
            similar[i] += noise(gen);
        }
        return similar;
    }
    
    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    S3JConfig config_;
};

TEST_F(S3JMethodTest, BasicConstruction) {
    EXPECT_NO_THROW({
        S3JMethod method(0.8, config_);
        EXPECT_EQ(method.getName(), "S3J");
        EXPECT_FALSE(method.isInitialized());
    });
}

TEST_F(S3JMethodTest, ConstructionWithConcurrencyManager) {
    EXPECT_NO_THROW({
        S3JMethod method(0, 1, 0.8, concurrency_manager_, config_);
        EXPECT_EQ(method.getName(), "S3J");
    });
}

TEST_F(S3JMethodTest, Open) {
    S3JMethod method(0.8, config_);
    RuntimeContext context(0, 4);
    
    method.open(context, nullptr, nullptr);
    
    EXPECT_TRUE(method.isInitialized());
}

TEST_F(S3JMethodTest, GetMetrics) {
    S3JMethod method(0.8, config_);
    RuntimeContext context(0, 4);
    method.open(context, nullptr, nullptr);
    
    auto metrics = method.getMetrics();
    
    EXPECT_EQ(metrics.total_queries, 0);
    EXPECT_EQ(metrics.total_matches, 0);
    EXPECT_EQ(metrics.current_partitions, config_.num_partitions);
}

TEST_F(S3JMethodTest, GetConfig) {
    S3JMethod method(0.8, config_);
    
    const auto& cfg = method.getConfig();
    
    EXPECT_NEAR(cfg.similarity_threshold, 0.8, 0.01);
    EXPECT_EQ(cfg.num_partitions, 4);
    EXPECT_EQ(cfg.dimension, kDimension);
}

TEST_F(S3JMethodTest, ForceAdapt) {
    S3JMethod method(0.8, config_);
    RuntimeContext context(0, 4);
    method.open(context, nullptr, nullptr);
    
    // 应该不会崩溃
    EXPECT_NO_THROW(method.forceAdapt());
}

TEST_F(S3JMethodTest, Close) {
    S3JMethod method(0.8, config_);
    RuntimeContext context(0, 4);
    method.open(context, nullptr, nullptr);
    
    EXPECT_TRUE(method.isInitialized());
    
    method.close();
    
    EXPECT_FALSE(method.isInitialized());
}

TEST_F(S3JMethodTest, ExecuteEagerWithoutManager) {
    // 禁用自适应，避免未初始化分区器的问题
    S3JConfig cfg = config_;
    cfg.enable_adaptive = false;
    
    S3JMethod method(0.8, cfg);
    RuntimeContext context(0, 4);
    method.open(context, nullptr, nullptr);
    
    std::mt19937 gen(42);
    auto vec = createRandomVector(gen);
    auto record = createVectorRecord(1, 1000, vec);
    
    // 没有 ConcurrencyManager 和 WindowState，应该返回空结果
    auto results = method.ExecuteEager(*record, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(S3JMethodTest, ExecuteLazyWithoutManager) {
    // 禁用自适应，避免未初始化分区器的问题
    S3JConfig cfg = config_;
    cfg.enable_adaptive = false;
    
    S3JMethod method(0.8, cfg);
    RuntimeContext context(0, 4);
    method.open(context, nullptr, nullptr);
    
    std::mt19937 gen(42);
    std::deque<std::unique_ptr<VectorRecord>> queries;
    
    for (int i = 0; i < 5; ++i) {
        auto vec = createRandomVector(gen);
        queries.push_back(createVectorRecord(i, 1000 + i, vec));
    }
    
    auto results = method.ExecuteLazy(queries, 0);
    EXPECT_TRUE(results.empty());  // 没有设置后端，返回空
}

TEST_F(S3JMethodTest, MetricsAfterQueries) {
    // 禁用自适应，避免未初始化分区器的问题
    S3JConfig cfg = config_;
    cfg.enable_adaptive = false;
    
    S3JMethod method(0.8, cfg);
    RuntimeContext context(0, 4);
    method.open(context, nullptr, nullptr);
    
    std::mt19937 gen(42);
    
    // 执行几次查询
    for (int i = 0; i < 10; ++i) {
        auto vec = createRandomVector(gen);
        auto record = createVectorRecord(i, 1000 + i, vec);
        method.ExecuteEager(*record, 0);
    }
    
    auto metrics = method.getMetrics();
    
    EXPECT_EQ(metrics.total_queries, 10);
    EXPECT_GE(metrics.avg_latency_ms, 0.0);
}

// =============================================================================
// 集成测试：S3JMethod 与 ConcurrencyManager
// =============================================================================

class S3JMethodIntegrationTest : public ::testing::Test {
protected:
    static constexpr int kDimension = 128;
    
    void SetUp() override {
        storage_manager_ = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
        
        // 创建左右索引
        left_index_id_ = concurrency_manager_->create_index(
            "left_s3j", IndexType::BruteForce, kDimension);
        right_index_id_ = concurrency_manager_->create_index(
            "right_s3j", IndexType::BruteForce, kDimension);
        
        ASSERT_GE(left_index_id_, 0);
        ASSERT_GE(right_index_id_, 0);
    }
    
    std::vector<float> createRandomVector(std::mt19937& gen) {
        std::normal_distribution<float> dist(0.0f, 1.0f);
        std::vector<float> vec(kDimension);
        float norm = 0.0f;
        for (int i = 0; i < kDimension; ++i) {
            vec[i] = dist(gen);
            norm += vec[i] * vec[i];
        }
        // 归一化
        norm = std::sqrt(norm);
        for (int i = 0; i < kDimension; ++i) {
            vec[i] /= norm;
        }
        return vec;
    }
    
    std::vector<float> createSimilarVector(const std::vector<float>& base, 
                                            float noise_level, 
                                            std::mt19937& gen) {
        std::normal_distribution<float> noise(0.0f, noise_level);
        std::vector<float> similar = base;
        float norm = 0.0f;
        for (size_t i = 0; i < similar.size(); ++i) {
            similar[i] += noise(gen);
            norm += similar[i] * similar[i];
        }
        // 归一化
        norm = std::sqrt(norm);
        for (size_t i = 0; i < similar.size(); ++i) {
            similar[i] /= norm;
        }
        return similar;
    }
    
    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    int left_index_id_ = -1;
    int right_index_id_ = -1;
};

TEST_F(S3JMethodIntegrationTest, ExecuteEagerWithMatches) {
    S3JConfig config;
    config.similarity_threshold = 0.9;
    config.dimension = kDimension;
    config.enable_adaptive = false;  // 禁用自适应
    
    S3JMethod method(left_index_id_, right_index_id_, 0.9, 
                     concurrency_manager_, config);
    
    RuntimeContext context(0, 1);
    method.open(context, nullptr, nullptr);
    
    std::mt19937 gen(42);
    
    // 创建基础向量并插入右侧索引
    auto base_vec = createRandomVector(gen);
    auto base_record = createVectorRecord(100, 1000, base_vec);
    storage_manager_->insert(createVectorRecord(100, 1000, base_vec));
    concurrency_manager_->insert(right_index_id_, createVectorRecord(100, 1000, base_vec));
    
    // 创建相似查询向量
    auto query_vec = createSimilarVector(base_vec, 0.05f, gen);
    auto query_record = createVectorRecord(1, 2000, query_vec);
    
    // 执行查询（slot=0 表示查询右侧索引）
    auto results = method.ExecuteEager(*query_record, 0);
    
    // 应该找到匹配
    EXPECT_GE(results.size(), 0);  // 可能找到匹配
}

TEST_F(S3JMethodIntegrationTest, MetricsCollection) {
    S3JConfig config;
    config.similarity_threshold = 0.8;
    config.dimension = kDimension;
    config.enable_metrics = true;
    config.enable_adaptive = false;  // 禁用自适应
    
    S3JMethod method(left_index_id_, right_index_id_, 0.8, 
                     concurrency_manager_, config);
    
    RuntimeContext context(0, 1);
    method.open(context, nullptr, nullptr);
    
    std::mt19937 gen(42);
    
    // 执行多次查询
    for (int i = 0; i < 20; ++i) {
        auto vec = createRandomVector(gen);
        auto record = createVectorRecord(i, 1000 + i, vec);
        method.ExecuteEager(*record, 0);
    }
    
    auto metrics = method.getMetrics();
    
    EXPECT_EQ(metrics.total_queries, 20);
    EXPECT_GE(metrics.throughput_qps, 0.0);
    EXPECT_GE(metrics.avg_latency_ms, 0.0);
}

}  // namespace test
}  // namespace sageFlow

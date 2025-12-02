//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-04: WindowStateFactory 单元测试
//

#include <gtest/gtest.h>

#include "state/window_state_factory.h"
#include "state/window_state.h"
#include "state/shared_window_state.h"
#include "state/partitioned_window_state.h"
#include "state/two_tier_window_state.h"
#include "state/partitioned_vector_state.h"
#include "execution/vector_space_partitioner.h"
#include "operator/join_strategy_config.h"

namespace sageFlow {
namespace {

// ============================================================
// WindowStateFactory 测试
// ============================================================

class WindowStateFactoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        default_parallelism_ = 4;
    }

    // 创建 LSH 分区器用于 PartitionedVectorState 测试
    std::shared_ptr<VectorSpacePartitioner> createLSHPartitioner(
        int dimension = 128, int num_hash = 8) {
        return std::make_shared<LSHPartitioner>(
            dimension, num_hash, 42, 0.1);  // seed=42, boundary_threshold=0.1
    }

    size_t default_parallelism_;
};

// ============================================================
// 基本创建测试
// ============================================================

TEST_F(WindowStateFactoryTest, CreateSharedState) {
    JoinStrategyConfig config;
    auto state = WindowStateFactory::create(
        WindowStateType::SHARED, default_parallelism_, config);

    ASSERT_NE(state, nullptr);
    EXPECT_TRUE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreatePartitionedState) {
    JoinStrategyConfig config;
    auto state = WindowStateFactory::create(
        WindowStateType::PARTITIONED, default_parallelism_, config);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreateTwoTierState) {
    JoinStrategyConfig config;
    config.two_tier_compact_threshold = 50;

    auto state = WindowStateFactory::create(
        WindowStateType::TWO_TIER, default_parallelism_, config);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreatePartitionedVectorStateWithoutPartitioner) {
    JoinStrategyConfig config;

    // 没有提供 partitioner 应该抛异常
    EXPECT_THROW(
        WindowStateFactory::create(
            WindowStateType::PARTITIONED_VECTOR, default_parallelism_, config, nullptr),
        std::runtime_error);
}

TEST_F(WindowStateFactoryTest, CreatePartitionedVectorStateWithPartitioner) {
    JoinStrategyConfig config;
    config.num_partitions = 4;
    config.two_tier_compact_threshold = 100;
    config.two_tier_enable_boundary_tracking = true;

    auto partitioner = createLSHPartitioner(128, 8);

    auto state = WindowStateFactory::create(
        WindowStateType::PARTITIONED_VECTOR, default_parallelism_, config, partitioner);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

// ============================================================
// createFromConfig 测试
// ============================================================

TEST_F(WindowStateFactoryTest, CreateFromConfigBruteforce) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.window_state_type = WindowStateType::SHARED;

    auto state = WindowStateFactory::createFromConfig(config, default_parallelism_);

    ASSERT_NE(state, nullptr);
    EXPECT_TRUE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreateFromConfigVSJoin) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.window_state_type = WindowStateType::PARTITIONED_VECTOR;
    config.num_partitions = 4;

    auto partitioner = createLSHPartitioner(128, 8);

    auto state = WindowStateFactory::createFromConfig(
        config, default_parallelism_, partitioner);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreateFromConfigS3J) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::S3J;
    config.window_state_type = WindowStateType::PARTITIONED;

    auto state = WindowStateFactory::createFromConfig(config, default_parallelism_);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

// ============================================================
// createWithDefaults 测试
// ============================================================

TEST_F(WindowStateFactoryTest, CreateWithDefaultsShared) {
    auto state = WindowStateFactory::createWithDefaults(
        WindowStateType::SHARED, default_parallelism_);

    ASSERT_NE(state, nullptr);
    EXPECT_TRUE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreateWithDefaultsPartitioned) {
    auto state = WindowStateFactory::createWithDefaults(
        WindowStateType::PARTITIONED, default_parallelism_);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

TEST_F(WindowStateFactoryTest, CreateWithDefaultsTwoTier) {
    auto state = WindowStateFactory::createWithDefaults(
        WindowStateType::TWO_TIER, default_parallelism_);

    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

// ============================================================
// 兼容性检查测试
// ============================================================

TEST_F(WindowStateFactoryTest, CompatibilityRoundRobin) {
    // RoundRobin 只与 Shared 兼容
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::SHARED, PartitionStrategy::ROUND_ROBIN));
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED, PartitionStrategy::ROUND_ROBIN));
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::TWO_TIER, PartitionStrategy::ROUND_ROBIN));
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED_VECTOR, PartitionStrategy::ROUND_ROBIN));
}

TEST_F(WindowStateFactoryTest, CompatibilityKeyHash) {
    // KeyHash 与 Shared/Partitioned/TwoTier 兼容
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::SHARED, PartitionStrategy::KEY_HASH));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED, PartitionStrategy::KEY_HASH));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::TWO_TIER, PartitionStrategy::KEY_HASH));
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED_VECTOR, PartitionStrategy::KEY_HASH));
}

TEST_F(WindowStateFactoryTest, CompatibilityVectorHash) {
    // VectorHash 与 Partitioned/TwoTier 兼容
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::SHARED, PartitionStrategy::VECTOR_HASH));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED, PartitionStrategy::VECTOR_HASH));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::TWO_TIER, PartitionStrategy::VECTOR_HASH));
}

TEST_F(WindowStateFactoryTest, CompatibilityLSH) {
    // LSH 只与 PartitionedVector 兼容
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::SHARED, PartitionStrategy::LSH));
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED, PartitionStrategy::LSH));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED_VECTOR, PartitionStrategy::LSH));
}

TEST_F(WindowStateFactoryTest, CompatibilityCentroid) {
    // Centroid 与 Partitioned/TwoTier 兼容
    EXPECT_FALSE(WindowStateFactory::isCompatible(
        WindowStateType::SHARED, PartitionStrategy::CENTROID));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::PARTITIONED, PartitionStrategy::CENTROID));
    EXPECT_TRUE(WindowStateFactory::isCompatible(
        WindowStateType::TWO_TIER, PartitionStrategy::CENTROID));
}

// ============================================================
// recommendStateType 测试
// ============================================================

TEST_F(WindowStateFactoryTest, RecommendStateTypeForPartitionStrategies) {
    EXPECT_EQ(WindowStateFactory::recommendStateType(PartitionStrategy::ROUND_ROBIN),
              WindowStateType::SHARED);
    EXPECT_EQ(WindowStateFactory::recommendStateType(PartitionStrategy::KEY_HASH),
              WindowStateType::PARTITIONED);
    EXPECT_EQ(WindowStateFactory::recommendStateType(PartitionStrategy::VECTOR_HASH),
              WindowStateType::PARTITIONED);
    EXPECT_EQ(WindowStateFactory::recommendStateType(PartitionStrategy::LSH),
              WindowStateType::PARTITIONED_VECTOR);
    EXPECT_EQ(WindowStateFactory::recommendStateType(PartitionStrategy::CENTROID),
              WindowStateType::PARTITIONED);
}

// ============================================================
// 功能集成测试
// ============================================================

TEST_F(WindowStateFactoryTest, CreatedStateCanAddAndRetrieveRecords) {
    JoinStrategyConfig config;
    auto state = WindowStateFactory::create(
        WindowStateType::PARTITIONED, 2, config);

    ASSERT_NE(state, nullptr);

    // 创建测试记录
    char* data = new char[128 * sizeof(float)];
    float* float_data = reinterpret_cast<float*>(data);
    for (int i = 0; i < 128; ++i) {
        float_data[i] = static_cast<float>(i);
    }

    auto record = std::make_unique<VectorRecord>(
        1, 1000, 128, DataType::Float32, data);

    // 添加记录
    state->addRecord(std::move(record), 0);

    // 验证记录
    EXPECT_EQ(state->size(0), 1);
    const auto& records = state->getRecords(0);
    EXPECT_EQ(records.size(), 1);
    EXPECT_EQ(records[0]->uid_, 1);
}

TEST_F(WindowStateFactoryTest, CreatedStateEvictionWorks) {
    JoinStrategyConfig config;
    auto state = WindowStateFactory::create(
        WindowStateType::SHARED, 1, config);

    ASSERT_NE(state, nullptr);

    // 添加多个记录
    for (int i = 0; i < 5; ++i) {
        char* data = new char[64 * sizeof(float)];
        auto record = std::make_unique<VectorRecord>(
            i, (i + 1) * 1000, 64, DataType::Float32, data);
        state->addRecord(std::move(record), 0);
    }

    EXPECT_EQ(state->size(0), 5);

    // 清理过期记录（窗口大小 2000ms，当前时间 5000ms）
    // 应该保留 timestamp >= 3000 的记录（即 uid 2, 3, 4）
    state->evictExpired(5000, 2000, 0);

    EXPECT_EQ(state->size(0), 3);  // uid 2, 3, 4 (timestamps 3000, 4000, 5000)
}

TEST_F(WindowStateFactoryTest, TwoTierStateCompactThreshold) {
    JoinStrategyConfig config;
    config.two_tier_compact_threshold = 5;

    auto state = WindowStateFactory::create(
        WindowStateType::TWO_TIER, 2, config);

    ASSERT_NE(state, nullptr);

    // 添加多个记录，超过压缩阈值
    for (int i = 0; i < 10; ++i) {
        char* data = new char[64 * sizeof(float)];
        auto record = std::make_unique<VectorRecord>(
            i, (i + 1) * 100, 64, DataType::Float32, data);
        state->addRecord(std::move(record), 0);
    }

    EXPECT_EQ(state->size(0), 10);
}

// ============================================================
// 错误处理测试
// ============================================================

TEST_F(WindowStateFactoryTest, InvalidWindowStateTypeThrows) {
    JoinStrategyConfig config;

    // 使用无效的枚举值（通过强制转换）
    WindowStateType invalid_type = static_cast<WindowStateType>(999);

    EXPECT_THROW(
        WindowStateFactory::create(invalid_type, default_parallelism_, config),
        std::runtime_error);
}

}  // namespace
}  // namespace sageFlow

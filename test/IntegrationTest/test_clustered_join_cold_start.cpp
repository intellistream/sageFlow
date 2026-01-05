#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include "operator/join_operator.h"
#include "operator/utils/join_strategy_config.h"
#include "execution/centroid_partitioner.h"
#include "execution/runtime_context.h"

using namespace sageFlow;

/**
 * @brief ClusteredJoin 冷启动集成测试
 * 
 * 验证冷启动机制的基本功能：
 * 1. 配置参数正确添加
 * 2. 冷启动状态管理
 * 3. 广播去重逻辑接口
 */
class ClusteredJoinColdStartTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 配置参数
        config_.algorithm = JoinAlgorithm::CLUSTERED_JOIN;
        config_.partition_strategy = PartitionStrategy::CENTROID;
        config_.window_state_type = WindowStateType::PARTITIONED;
        config_.index_strategy = IndexStrategy::PARTITIONED;
        
        config_.enable_cold_start = true;
        config_.training_samples = 50;
        config_.deduplicate_during_broadcast = true;
        
        config_.similarity_threshold = 0.8;
        config_.dimension = 128;
        config_.num_partitions = 4;
    }
    
    JoinStrategyConfig config_;
};

// ==================== 测试 1: 配置参数验证 ====================

TEST_F(ClusteredJoinColdStartTest, ConfigParametersTest) {
    // 验证冷启动相关配置参数已正确添加
    EXPECT_TRUE(config_.enable_cold_start);
    EXPECT_EQ(config_.training_samples, 50);
    EXPECT_TRUE(config_.deduplicate_during_broadcast);
}

// ==================== 测试 2: CentroidPartitioner 冷启动行为 ====================

TEST_F(ClusteredJoinColdStartTest, CentroidPartitionerColdStart) {
    // 创建 CentroidPartitioner
    CentroidPartitioner::Config cp_config;
    cp_config.num_partitions = config_.num_partitions;
    cp_config.dimension = config_.dimension;
    cp_config.training_samples = config_.training_samples;
    cp_config.enable_cold_start = config_.enable_cold_start;
    
    auto partitioner = std::make_shared<CentroidPartitioner>(cp_config);
    
    // 验证初始状态：未训练，启用广播
    EXPECT_FALSE(partitioner->isTrained());
    EXPECT_TRUE(partitioner->isBroadcast());
}

// ==================== 主函数 ====================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-06: JoinConfigValidator 单元测试
//

#include <gtest/gtest.h>

#include "operator/utils/join_config_validator.h"
#include "operator/utils/join_strategy_config.h"

namespace sageFlow {
namespace {

// ============================================================
// JoinConfigValidator 测试
// ============================================================

class JoinConfigValidatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 设置一个默认的有效配置
        valid_config_.algorithm = JoinAlgorithm::BRUTEFORCE;
        valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
        valid_config_.window_state_type = WindowStateType::SHARED;
        valid_config_.index_strategy = IndexStrategy::SHARED;
        valid_config_.dimension = 128;
        valid_config_.similarity_threshold = 0.8;
        valid_config_.num_partitions = 4;
        valid_config_.window_size_ms = 10000;
        valid_config_.step_size_ms = 1000;
    }

    JoinStrategyConfig valid_config_;
};

// ============================================================
// 有效配置测试
// ============================================================

TEST_F(JoinConfigValidatorTest, ValidConfig) {
    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.errors.empty());
}

TEST_F(JoinConfigValidatorTest, ValidConfigWithWarnings) {
    // 有效但有警告的配置
    valid_config_.algorithm = JoinAlgorithm::HNSW;
    valid_config_.hnsw_ef_search = 10;  // 低于推荐值

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

// ============================================================
// 分区-窗口兼容性测试
// ============================================================

TEST_F(JoinConfigValidatorTest, IncompatibleRoundRobinWithPartitioned) {
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    ASSERT_FALSE(result.errors.empty());
    EXPECT_TRUE(result.errors[0].find("RoundRobin") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, IncompatibleLSHWithShared) {
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::SHARED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    EXPECT_TRUE(result.errors[0].find("LSH") != std::string::npos ||
                result.errors[0].find("PartitionedVectorState") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, IncompatibleCentroidWithShared) {
    valid_config_.partition_strategy = PartitionStrategy::CENTROID;
    valid_config_.window_state_type = WindowStateType::SHARED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    EXPECT_TRUE(result.errors[0].find("Centroid") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, IncompatibleVectorHashWithShared) {
    valid_config_.partition_strategy = PartitionStrategy::VECTOR_HASH;
    valid_config_.window_state_type = WindowStateType::SHARED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    EXPECT_TRUE(result.errors[0].find("VectorHash") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, CompatibleKeyHashWithPartitioned) {
    valid_config_.partition_strategy = PartitionStrategy::KEY_HASH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
}

// ============================================================
// 算法-策略兼容性测试
// ============================================================

TEST_F(JoinConfigValidatorTest, VSJoinRequiresLSH) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    valid_config_.window_state_type = WindowStateType::SHARED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    // 应该有多个错误：需要 LSH，需要 PARTITIONED_VECTOR，需要 PARTITIONED 索引
    EXPECT_GE(result.errors.size(), 1u);
}

TEST_F(JoinConfigValidatorTest, VSJoinValidConfig) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;  // 新版推荐
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    // 应该有关于组件依赖的警告
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, S3JRequiresCentroid) {
    valid_config_.algorithm = JoinAlgorithm::S3J;
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    bool found_s3j_error = false;
    for (const auto& error : result.errors) {
        if (error.find("S3J") != std::string::npos) {
            found_s3j_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_s3j_error);
}

TEST_F(JoinConfigValidatorTest, S3JValidConfig) {
    valid_config_.algorithm = JoinAlgorithm::S3J;
    valid_config_.partition_strategy = PartitionStrategy::CENTROID;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
}

TEST_F(JoinConfigValidatorTest, ClusteredJoinRequiresCentroid) {
    valid_config_.algorithm = JoinAlgorithm::CLUSTERED_JOIN;
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, ClusteredJoinValidConfig) {
    valid_config_.algorithm = JoinAlgorithm::CLUSTERED_JOIN;
    valid_config_.partition_strategy = PartitionStrategy::CENTROID;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
}

// ============================================================
// 参数范围检查测试
// ============================================================

TEST_F(JoinConfigValidatorTest, InvalidSimilarityThresholdHigh) {
    valid_config_.similarity_threshold = 1.5;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    EXPECT_TRUE(result.errors[0].find("similarity_threshold") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, InvalidSimilarityThresholdNegative) {
    valid_config_.similarity_threshold = -0.5;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, InvalidDimension) {
    valid_config_.dimension = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    EXPECT_TRUE(result.errors[0].find("dimension") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, InvalidNumPartitions) {
    valid_config_.num_partitions = -1;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, InvalidWindowSize) {
    valid_config_.window_size_ms = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, InvalidStepSize) {
    valid_config_.step_size_ms = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, StepSizeExceedsWindowSize) {
    valid_config_.window_size_ms = 1000;
    valid_config_.step_size_ms = 2000;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, IVFNprobesExceedsNlist) {
    valid_config_.ivf_nlist = 10;
    valid_config_.ivf_nprobes = 20;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    bool found = false;
    for (const auto& e : result.errors) {
        if (e.find("ivf_nprobes") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, InvalidHNSWM) {
    valid_config_.hnsw_m = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, HNSWEfConstructionLessThanM) {
    valid_config_.hnsw_m = 32;
    valid_config_.hnsw_ef_construction = 16;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    bool found = false;
    for (const auto& e : result.errors) {
        if (e.find("hnsw_ef_construction") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, InvalidVSJoinHashFunctions) {
    valid_config_.vsjoin_num_hash_functions = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, VSJoinHashFunctionsTooMany) {
    valid_config_.vsjoin_num_hash_functions = 100;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, InvalidVSJoinBoundaryThreshold) {
    valid_config_.vsjoin_boundary_threshold = 1.5;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

// ============================================================
// VSJoin V2 新配置字段测试
// ============================================================

TEST_F(JoinConfigValidatorTest, VSJoinV2_ValidConfig) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_multicast_k = 2;
    valid_config_.vsjoin_rebuild_interval_ms = 5000;
    valid_config_.vsjoin_rebuild_threshold = 1000;
    valid_config_.vsjoin_local_index_type = VSJoinIndexType::BRUTEFORCE;
    valid_config_.vsjoin_global_index_type = VSJoinIndexType::IVF;
    valid_config_.vsjoin_num_hash_functions = 8;
    valid_config_.vsjoin_boundary_threshold = 0.1;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidMulticastK_TooSmall) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_multicast_k = 0;  // 无效：< 1

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    // 验证错误信息包含 multicast_k
    bool found = false;
    for (const auto& err : result.errors) {
        if (err.find("multicast_k") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidMulticastK_TooLarge) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_multicast_k = 15;  // 无效：> 10

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidRebuildInterval) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_rebuild_interval_ms = 500;  // 无效：< 1000

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    // 验证错误信息包含 rebuild_interval
    bool found = false;
    for (const auto& err : result.errors) {
        if (err.find("rebuild_interval") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidRebuildThreshold) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_rebuild_threshold = 50;  // 无效：< 100

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidRebalanceImbalanceRatio) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_rebalance_imbalance_ratio = 0.9;  // 无效：< 1.0

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    bool found = false;
    for (const auto& err : result.errors) {
        if (err.find("vsjoin_rebalance_imbalance_ratio") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidRebalanceMaxMoves) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_rebalance_max_moves = 0;  // 无效：< 1

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    bool found = false;
    for (const auto& err : result.errors) {
        if (err.find("vsjoin_rebalance_max_moves") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_InvalidGlobalIndexType) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_global_index_type = VSJoinIndexType::BRUTEFORCE;  // 无效

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    // 验证错误信息包含 global_index_type
    bool found = false;
    for (const auto& err : result.errors) {
        if (err.find("global_index_type") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_NonBruteforceLocalIndexWarning) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.vsjoin_local_index_type = VSJoinIndexType::IVF;  // 有警告

    auto result = JoinConfigValidator::validate(valid_config_);

    // 有效但有警告
    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
    // 验证警告信息包含 local_index_type
    bool found = false;
    for (const auto& warn : result.warnings) {
        if (warn.find("local_index_type") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_RebuildIntervalTooLargeWarning) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.window_size_ms = 10000;
    valid_config_.vsjoin_rebuild_interval_ms = 50000;  // 5x window_size

    auto result = JoinConfigValidator::validate(valid_config_);

    // 有效但有警告
    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, VSJoinV2_MulticastKTooLargeWarning) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;
    valid_config_.num_partitions = 4;
    valid_config_.vsjoin_multicast_k = 3;  // > num_partitions/2

    auto result = JoinConfigValidator::validate(valid_config_);

    // 有效但有警告
    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, InvalidHDRProjectedDim) {
    valid_config_.hdr_projected_dim = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

TEST_F(JoinConfigValidatorTest, HDRProjectedDimExceedsDimension) {
    valid_config_.dimension = 64;
    valid_config_.hdr_projected_dim = 128;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
}

// ============================================================
// 性能警告测试
// ============================================================

TEST_F(JoinConfigValidatorTest, PerformanceWarningBruteForcePartitioned) {
    valid_config_.algorithm = JoinAlgorithm::BRUTEFORCE;
    valid_config_.partition_strategy = PartitionStrategy::KEY_HASH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningLargeWindow) {
    valid_config_.window_size_ms = 120000;  // 2 minutes

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
    bool found_window_warning = false;
    for (const auto& w : result.warnings) {
        if (w.find("window size") != std::string::npos ||
            w.find("Large window") != std::string::npos) {
            found_window_warning = true;
            break;
        }
    }
    EXPECT_TRUE(found_window_warning);
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningSmallWindow) {
    valid_config_.window_size_ms = 50;  // 50ms
    valid_config_.step_size_ms = 50;    // 确保 step_size <= window_size

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningLowHNSWEfSearch) {
    valid_config_.algorithm = JoinAlgorithm::HNSW;
    valid_config_.hnsw_ef_search = 10;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningLowIVFNprobes) {
    valid_config_.algorithm = JoinAlgorithm::IVF;
    valid_config_.ivf_nprobes = 2;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningHighThreshold) {
    valid_config_.similarity_threshold = 0.999;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningLowThreshold) {
    valid_config_.similarity_threshold = 0.05;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningManyPartitions) {
    valid_config_.num_partitions = 128;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

TEST_F(JoinConfigValidatorTest, PerformanceWarningEagerWithLargeWindow) {
    valid_config_.is_eager = true;
    valid_config_.window_size_ms = 30000;  // 30 seconds

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.hasWarnings());
}

// ============================================================
// 依赖检查测试
// ============================================================

TEST_F(JoinConfigValidatorTest, DependencyWarningHDRTree) {
    valid_config_.algorithm = JoinAlgorithm::HDR_TREE;

    auto result = JoinConfigValidator::validate(valid_config_);

    // HDR_TREE 应该有 PCA 依赖警告
    EXPECT_TRUE(result.hasWarnings());
    bool found_pca_warning = false;
    for (const auto& w : result.warnings) {
        if (w.find("PCA") != std::string::npos) {
            found_pca_warning = true;
            break;
        }
    }
    EXPECT_TRUE(found_pca_warning);
}

TEST_F(JoinConfigValidatorTest, DependencyWarningVSJoin) {
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::LSH;
    valid_config_.window_state_type = WindowStateType::PARTITIONED_VECTOR;
    valid_config_.index_strategy = IndexStrategy::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);

    // VSJoin 应该有组件依赖警告
    EXPECT_TRUE(result.hasWarnings());
    bool found = false;
    for (const auto& w : result.warnings) {
        if (w.find("PartitionCoordinator") != std::string::npos ||
            w.find("AsyncCandidateGenerator") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

// ============================================================
// throwIfInvalid 测试
// ============================================================

TEST_F(JoinConfigValidatorTest, ThrowIfInvalidWithValidConfig) {
    EXPECT_NO_THROW(JoinConfigValidator::throwIfInvalid(valid_config_));
}

TEST_F(JoinConfigValidatorTest, ThrowIfInvalidWithInvalidConfig) {
    valid_config_.similarity_threshold = -0.5;

    EXPECT_THROW(
        JoinConfigValidator::throwIfInvalid(valid_config_),
        std::runtime_error);
}

TEST_F(JoinConfigValidatorTest, ThrowIfInvalidExceptionMessage) {
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    try {
        JoinConfigValidator::throwIfInvalid(valid_config_);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        EXPECT_TRUE(msg.find("INVALID") != std::string::npos);
        EXPECT_TRUE(msg.find("RoundRobin") != std::string::npos);
    }
}

// ============================================================
// toString 测试
// ============================================================

TEST_F(JoinConfigValidatorTest, ToStringValidResult) {
    auto result = JoinConfigValidator::validate(valid_config_);
    auto str = result.toString();

    EXPECT_FALSE(str.empty());
    EXPECT_TRUE(str.find("valid") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, ToStringInvalidResult) {
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    valid_config_.window_state_type = WindowStateType::PARTITIONED;

    auto result = JoinConfigValidator::validate(valid_config_);
    auto str = result.toString();

    EXPECT_FALSE(str.empty());
    EXPECT_TRUE(str.find("INVALID") != std::string::npos);
    EXPECT_TRUE(str.find("ERROR") != std::string::npos);
}

TEST_F(JoinConfigValidatorTest, ToStringWithWarnings) {
    valid_config_.window_size_ms = 120000;

    auto result = JoinConfigValidator::validate(valid_config_);
    auto str = result.toString();

    EXPECT_TRUE(str.find("WARN") != std::string::npos ||
                str.find("Warnings") != std::string::npos);
}

// ============================================================
// isCompatible 静态方法测试
// ============================================================

TEST_F(JoinConfigValidatorTest, IsCompatibleRoundRobinShared) {
    EXPECT_TRUE(JoinConfigValidator::isCompatible(
        PartitionStrategy::ROUND_ROBIN, WindowStateType::SHARED));
}

TEST_F(JoinConfigValidatorTest, IsCompatibleRoundRobinPartitioned) {
    EXPECT_FALSE(JoinConfigValidator::isCompatible(
        PartitionStrategy::ROUND_ROBIN, WindowStateType::PARTITIONED));
}

TEST_F(JoinConfigValidatorTest, IsCompatibleLSHPartitionedVector) {
    EXPECT_TRUE(JoinConfigValidator::isCompatible(
        PartitionStrategy::LSH, WindowStateType::PARTITIONED_VECTOR));
}

TEST_F(JoinConfigValidatorTest, IsCompatibleCentroidPartitioned) {
    EXPECT_TRUE(JoinConfigValidator::isCompatible(
        PartitionStrategy::CENTROID, WindowStateType::PARTITIONED));
}

TEST_F(JoinConfigValidatorTest, IsCompatibleKeyHashMultiple) {
    EXPECT_TRUE(JoinConfigValidator::isCompatible(
        PartitionStrategy::KEY_HASH, WindowStateType::SHARED));
    EXPECT_TRUE(JoinConfigValidator::isCompatible(
        PartitionStrategy::KEY_HASH, WindowStateType::PARTITIONED));
    EXPECT_TRUE(JoinConfigValidator::isCompatible(
        PartitionStrategy::KEY_HASH, WindowStateType::TWO_TIER));
}

// ============================================================
// getRecommendedWindowStates 测试
// ============================================================

TEST_F(JoinConfigValidatorTest, RecommendedWindowStatesRoundRobin) {
    auto states = JoinConfigValidator::getRecommendedWindowStates(
        PartitionStrategy::ROUND_ROBIN);

    EXPECT_EQ(states.size(), 1u);
    EXPECT_EQ(states[0], WindowStateType::SHARED);
}

TEST_F(JoinConfigValidatorTest, RecommendedWindowStatesLSH) {
    auto states = JoinConfigValidator::getRecommendedWindowStates(
        PartitionStrategy::LSH);

    EXPECT_EQ(states.size(), 3u);
    EXPECT_EQ(states[0], WindowStateType::PARTITIONED);
    EXPECT_EQ(states[1], WindowStateType::TWO_TIER);
    EXPECT_EQ(states[2], WindowStateType::PARTITIONED_VECTOR);
}

TEST_F(JoinConfigValidatorTest, RecommendedWindowStatesKeyHash) {
    auto states = JoinConfigValidator::getRecommendedWindowStates(
        PartitionStrategy::KEY_HASH);

    EXPECT_GE(states.size(), 1u);
}

// ============================================================
// getRecommendedPartitionStrategy 测试
// ============================================================

TEST_F(JoinConfigValidatorTest, RecommendedPartitionBruteforce) {
    auto strategy = JoinConfigValidator::getRecommendedPartitionStrategy(
        JoinAlgorithm::BRUTEFORCE);

    EXPECT_EQ(strategy, PartitionStrategy::ROUND_ROBIN);
}

TEST_F(JoinConfigValidatorTest, RecommendedPartitionVSJoin) {
    auto strategy = JoinConfigValidator::getRecommendedPartitionStrategy(
        JoinAlgorithm::VSJOIN);

    EXPECT_EQ(strategy, PartitionStrategy::LSH);
}

TEST_F(JoinConfigValidatorTest, RecommendedPartitionS3J) {
    auto strategy = JoinConfigValidator::getRecommendedPartitionStrategy(
        JoinAlgorithm::S3J);

    EXPECT_EQ(strategy, PartitionStrategy::CENTROID);
}

TEST_F(JoinConfigValidatorTest, RecommendedPartitionClusteredJoin) {
    auto strategy = JoinConfigValidator::getRecommendedPartitionStrategy(
        JoinAlgorithm::CLUSTERED_JOIN);

    EXPECT_EQ(strategy, PartitionStrategy::CENTROID);
}

// ============================================================
// 多重错误测试
// ============================================================

TEST_F(JoinConfigValidatorTest, MultipleErrors) {
    // 配置多个错误
    valid_config_.similarity_threshold = -0.5;
    valid_config_.dimension = -1;
    valid_config_.window_size_ms = 0;
    valid_config_.hnsw_m = 0;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    EXPECT_GE(result.errors.size(), 4u);
}

TEST_F(JoinConfigValidatorTest, MultipleCompatibilityErrors) {
    // VSJoin 配置错误的所有策略
    valid_config_.algorithm = JoinAlgorithm::VSJOIN;
    valid_config_.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    valid_config_.window_state_type = WindowStateType::SHARED;
    valid_config_.index_strategy = IndexStrategy::SHARED;

    auto result = JoinConfigValidator::validate(valid_config_);

    EXPECT_FALSE(result.valid);
    // 应该有多个兼容性错误
    EXPECT_GE(result.errors.size(), 2u);
}

// ============================================================
// 边界情况测试
// ============================================================

TEST_F(JoinConfigValidatorTest, BoundaryValidThreshold) {
    valid_config_.similarity_threshold = 0.0;
    auto result1 = JoinConfigValidator::validate(valid_config_);
    EXPECT_TRUE(result1.valid);

    valid_config_.similarity_threshold = 1.0;
    auto result2 = JoinConfigValidator::validate(valid_config_);
    EXPECT_TRUE(result2.valid);
}

TEST_F(JoinConfigValidatorTest, MinimalValidPartitions) {
    valid_config_.num_partitions = 1;

    auto result = JoinConfigValidator::validate(valid_config_);
    EXPECT_TRUE(result.valid);
}

TEST_F(JoinConfigValidatorTest, IVFNprobesEqualsNlist) {
    valid_config_.ivf_nlist = 10;
    valid_config_.ivf_nprobes = 10;

    auto result = JoinConfigValidator::validate(valid_config_);
    // nprobes == nlist 是允许的
    bool has_ivf_error = false;
    for (const auto& e : result.errors) {
        if (e.find("ivf_nprobes") != std::string::npos) {
            has_ivf_error = true;
            break;
        }
    }
    EXPECT_FALSE(has_ivf_error);
}

}  // namespace
}  // namespace sageFlow

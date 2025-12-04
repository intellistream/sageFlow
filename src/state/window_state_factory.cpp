//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-04: WindowStateFactory 窗口状态自适应选择
//

#include "state/window_state_factory.h"
#include "state/shared_window_state.h"
#include "state/partitioned_window_state.h"
#include "state/two_tier_window_state.h"
#include "state/partitioned_vector_state.h"
#include "execution/vector_space_partitioner.h"
#include "utils/logger.h"

#include <stdexcept>

namespace sageFlow {

std::unique_ptr<WindowState> WindowStateFactory::create(
    WindowStateType type,
    size_t parallelism,
    const JoinStrategyConfig& config,
    std::shared_ptr<VectorSpacePartitioner> partitioner) {

    switch (type) {
        case WindowStateType::SHARED:
            SAGEFLOW_LOG_DEBUG("WINDOW_STATE_FACTORY", 
                              "Creating SharedWindowState");
            return std::make_unique<SharedWindowState>();

        case WindowStateType::PARTITIONED:
            SAGEFLOW_LOG_DEBUG("WINDOW_STATE_FACTORY", 
                              "Creating PartitionedWindowState with parallelism={}",
                              parallelism);
            return std::make_unique<PartitionedWindowState>(parallelism);

        case WindowStateType::TWO_TIER:
            SAGEFLOW_LOG_DEBUG("WINDOW_STATE_FACTORY", 
                              "Creating TwoTierWindowState with parallelism={}, "
                              "compact_threshold={}",
                              parallelism, config.two_tier_compact_threshold);
            return std::make_unique<TwoTierWindowState>(
                parallelism,
                config.two_tier_compact_threshold);

        case WindowStateType::PARTITIONED_VECTOR: {
            if (!partitioner) {
                throw std::runtime_error(
                    "PartitionedVectorState requires a VectorSpacePartitioner, "
                    "but none was provided");
            }
            SAGEFLOW_LOG_DEBUG("WINDOW_STATE_FACTORY", 
                              "Creating PartitionedVectorState with num_partitions={}, "
                              "compact_threshold={}, boundary_tracking={}",
                              config.num_partitions,
                              config.two_tier_compact_threshold,
                              config.two_tier_enable_boundary_tracking);
            return std::make_unique<PartitionedVectorState>(
                static_cast<size_t>(config.num_partitions),
                partitioner,
                config.two_tier_compact_threshold,
                config.two_tier_enable_boundary_tracking);
        }

        default:
            throw std::runtime_error(
                "Unknown WindowStateType: " + toString(type));
    }
}

std::unique_ptr<WindowState> WindowStateFactory::createFromConfig(
    const JoinStrategyConfig& config,
    size_t parallelism,
    std::shared_ptr<VectorSpacePartitioner> partitioner) {

    SAGEFLOW_LOG_DEBUG("WINDOW_STATE_FACTORY", 
                      "Creating window state from config: type={}, algorithm={}",
                      toString(config.window_state_type),
                      toString(config.algorithm));

    return create(config.window_state_type, parallelism, config, partitioner);
}

std::unique_ptr<WindowState> WindowStateFactory::createWithDefaults(
    WindowStateType type,
    size_t parallelism) {

    JoinStrategyConfig default_config;
    return create(type, parallelism, default_config, nullptr);
}

bool WindowStateFactory::isCompatible(
    WindowStateType state_type,
    PartitionStrategy partition_strategy) {

    // 兼容性规则：
    // | 分区策略     | 兼容的窗口状态                    |
    // |-------------|--------------------------------|
    // | RoundRobin  | SharedWindowState              |
    // | KeyHash     | Partitioned/Shared             |
    // | VectorHash  | Partitioned                    |
    // | LSH         | PartitionedVectorState         |
    // | Centroid    | Partitioned                    |

    switch (partition_strategy) {
        case PartitionStrategy::ROUND_ROBIN:
            // RoundRobin 必须使用共享状态，否则跨分区匹配会丢失
            return state_type == WindowStateType::SHARED;

        case PartitionStrategy::KEY_HASH:
            // Key 分区可以使用分区状态或共享状态
            return state_type == WindowStateType::PARTITIONED ||
                   state_type == WindowStateType::SHARED ||
                   state_type == WindowStateType::TWO_TIER;

        case PartitionStrategy::VECTOR_HASH:
            // 向量哈希分区应使用分区状态
            return state_type == WindowStateType::PARTITIONED ||
                   state_type == WindowStateType::TWO_TIER;

        case PartitionStrategy::LSH:
            // LSH 分区（VSJoin）必须使用 PartitionedVectorState
            return state_type == WindowStateType::PARTITIONED_VECTOR;

        case PartitionStrategy::CENTROID:
            // 质心分区应使用分区状态
            return state_type == WindowStateType::PARTITIONED ||
                   state_type == WindowStateType::TWO_TIER;

        default:
            // 未知分区策略，保守返回 false
            return false;
    }
}

WindowStateType WindowStateFactory::recommendStateType(
    PartitionStrategy partition_strategy) {

    switch (partition_strategy) {
        case PartitionStrategy::ROUND_ROBIN:
            return WindowStateType::SHARED;

        case PartitionStrategy::KEY_HASH:
            return WindowStateType::PARTITIONED;

        case PartitionStrategy::VECTOR_HASH:
            return WindowStateType::PARTITIONED;

        case PartitionStrategy::LSH:
            return WindowStateType::PARTITIONED_VECTOR;

        case PartitionStrategy::CENTROID:
            return WindowStateType::PARTITIONED;

        default:
            // 默认使用共享状态
            return WindowStateType::SHARED;
    }
}

}  // namespace sageFlow

//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-06: JoinConfigValidator 配置验证与错误处理
//

#include "operator/join_config_validator.h"
#include "utils/logger.h"

#include <sstream>
#include <stdexcept>

namespace sageFlow {

// ==================== 辅助函数声明 ====================
// 这些函数在 join_strategy_config.cpp 中定义
std::string toString(JoinAlgorithm algo);
std::string toString(PartitionStrategy ps);
std::string toString(WindowStateType ws);
std::string toString(IndexStrategy is);

// ==================== ValidationResult 方法实现 ====================

std::string JoinConfigValidator::ValidationResult::toString() const {
    std::ostringstream oss;

    if (!valid) {
        oss << "Configuration is INVALID:\n";
        for (const auto& error : errors) {
            oss << "  [ERROR] " << error << "\n";
        }
    } else {
        oss << "Configuration is valid.\n";
    }

    if (!warnings.empty()) {
        oss << "Warnings:\n";
        for (const auto& warning : warnings) {
            oss << "  [WARN] " << warning << "\n";
        }
    }

    return oss.str();
}

void JoinConfigValidator::ValidationResult::addError(const std::string& error) {
    errors.push_back(error);
    valid = false;
}

void JoinConfigValidator::ValidationResult::addWarning(const std::string& warning) {
    warnings.push_back(warning);
}

// ==================== JoinConfigValidator 静态方法实现 ====================

JoinConfigValidator::ValidationResult JoinConfigValidator::validate(
    const JoinStrategyConfig& config) {

    ValidationResult result;
    result.valid = true;

    // 依次执行各项检查
    checkPartitionWindowCompatibility(config, result);
    checkAlgorithmStrategyCompatibility(config, result);
    checkParameterRanges(config, result);
    checkDependencies(config, result);
    checkPerformanceHints(config, result);

    return result;
}

void JoinConfigValidator::throwIfInvalid(const JoinStrategyConfig& config) {
    auto result = validate(config);
    if (!result.valid) {
        throw std::runtime_error(result.toString());
    }
}

bool JoinConfigValidator::validateAndLog(const JoinStrategyConfig& config) {
    auto result = validate(config);

    if (!result.valid) {
        SAGEFLOW_LOG_ERROR("JoinConfigValidator", "{}", result.toString());
        return false;
    }

    if (result.hasWarnings()) {
        for (const auto& warning : result.warnings) {
            SAGEFLOW_LOG_WARN("JoinConfigValidator", "{}", warning);
        }
    }

    return true;
}

bool JoinConfigValidator::isCompatible(PartitionStrategy partition_strategy,
                                        WindowStateType window_state_type) {
    // 兼容性规则表
    switch (partition_strategy) {
        case PartitionStrategy::ROUND_ROBIN:
            // RoundRobin 只兼容 SHARED
            return window_state_type == WindowStateType::SHARED;

        case PartitionStrategy::KEY_HASH:
            // KEY_HASH 兼容 SHARED 和 PARTITIONED
            return window_state_type == WindowStateType::SHARED ||
                   window_state_type == WindowStateType::PARTITIONED ||
                   window_state_type == WindowStateType::TWO_TIER;

        case PartitionStrategy::VECTOR_HASH:
            // VECTOR_HASH 兼容 PARTITIONED 和 TWO_TIER
            return window_state_type == WindowStateType::PARTITIONED ||
                   window_state_type == WindowStateType::TWO_TIER;

        case PartitionStrategy::LSH:
            // LSH 需要 PARTITIONED_VECTOR（VSJoin 专用）
            return window_state_type == WindowStateType::PARTITIONED_VECTOR;

        case PartitionStrategy::CENTROID:
            // CENTROID 兼容 PARTITIONED 和 TWO_TIER
            return window_state_type == WindowStateType::PARTITIONED ||
                   window_state_type == WindowStateType::TWO_TIER;

        default:
            return false;
    }
}

std::vector<WindowStateType> JoinConfigValidator::getRecommendedWindowStates(
    PartitionStrategy partition_strategy) {

    switch (partition_strategy) {
        case PartitionStrategy::ROUND_ROBIN:
            return {WindowStateType::SHARED};

        case PartitionStrategy::KEY_HASH:
            return {WindowStateType::PARTITIONED, WindowStateType::SHARED};

        case PartitionStrategy::VECTOR_HASH:
            return {WindowStateType::PARTITIONED};

        case PartitionStrategy::LSH:
            return {WindowStateType::PARTITIONED_VECTOR};

        case PartitionStrategy::CENTROID:
            return {WindowStateType::PARTITIONED};

        default:
            return {WindowStateType::SHARED};
    }
}

PartitionStrategy JoinConfigValidator::getRecommendedPartitionStrategy(
    JoinAlgorithm algorithm) {

    switch (algorithm) {
        case JoinAlgorithm::BRUTEFORCE:
        case JoinAlgorithm::IVF:
        case JoinAlgorithm::HNSW:
        case JoinAlgorithm::HDR_TREE:
            // 通用算法推荐 ROUND_ROBIN
            return PartitionStrategy::ROUND_ROBIN;

        case JoinAlgorithm::LSH:
            // 新 LSH 方法暂按共享分区默认值处理
            return PartitionStrategy::ROUND_ROBIN;

        case JoinAlgorithm::VSJOIN:
            return PartitionStrategy::LSH;

        case JoinAlgorithm::S3J:
        case JoinAlgorithm::CLUSTERED_JOIN:
            return PartitionStrategy::CENTROID;

        default:
            return PartitionStrategy::ROUND_ROBIN;
    }
}

// ==================== 私有检查方法实现 ====================

void JoinConfigValidator::checkPartitionWindowCompatibility(
    const JoinStrategyConfig& config,
    ValidationResult& result) {

    // 规则1: RoundRobin 必须配 SHARED
    if (config.partition_strategy == PartitionStrategy::ROUND_ROBIN &&
        config.window_state_type != WindowStateType::SHARED) {
        result.addError(
            "RoundRobin partition strategy requires SharedWindowState. "
            "Using " + sageFlow::toString(config.window_state_type) +
            " with RoundRobin will cause cross-partition matches to be lost, "
            "resulting in reduced recall. Change window_state_type to SHARED.");
    }

    // 规则2: LSH 需要 PARTITIONED_VECTOR
    if (config.partition_strategy == PartitionStrategy::LSH &&
        config.window_state_type != WindowStateType::PARTITIONED_VECTOR) {
        result.addError(
            "LSH partition strategy requires PartitionedVectorState. "
            "Current: " + sageFlow::toString(config.window_state_type) + ". "
            "LSH is designed for VSJoin which uses vector-space partitioned state.");
    }

    // 规则3: CENTROID 不兼容 SHARED
    if (config.partition_strategy == PartitionStrategy::CENTROID &&
        config.window_state_type == WindowStateType::SHARED) {
        result.addError(
            "Centroid partition strategy is incompatible with SharedWindowState. "
            "Centroid-based partitioning requires PartitionedWindowState to maintain "
            "partition-local data for efficient clustering. "
            "Change window_state_type to PARTITIONED.");
    }

    // 规则4: VECTOR_HASH 不应使用 SHARED
    if (config.partition_strategy == PartitionStrategy::VECTOR_HASH &&
        config.window_state_type == WindowStateType::SHARED) {
        result.addError(
            "VectorHash partition strategy is incompatible with SharedWindowState. "
            "VectorHash routes similar vectors to the same partition, which requires "
            "PartitionedWindowState to take advantage of data locality.");
    }
}

void JoinConfigValidator::checkAlgorithmStrategyCompatibility(
    const JoinStrategyConfig& config,
    ValidationResult& result) {

    // VSJoin 必须配 LSH + PARTITIONED_VECTOR + PARTITIONED 索引
    if (config.algorithm == JoinAlgorithm::VSJOIN) {
        if (config.partition_strategy != PartitionStrategy::LSH) {
            result.addError(
                "VSJoin algorithm requires LSH partition strategy. "
                "Current: " + sageFlow::toString(config.partition_strategy) + ". "
                "VSJoin uses locality-sensitive hashing to partition similar vectors.");
        }
        if (config.window_state_type != WindowStateType::PARTITIONED_VECTOR) {
            result.addError(
                "VSJoin algorithm requires PartitionedVectorState. "
                "Current: " + sageFlow::toString(config.window_state_type) + ".");
        }
        if (config.index_strategy != IndexStrategy::PARTITIONED) {
            result.addError(
                "VSJoin algorithm requires partitioned index strategy. "
                "Current: " + sageFlow::toString(config.index_strategy) + ". "
                "Each partition maintains its own index for efficient local search.");
        }
    }

    // S3J 必须配 CENTROID
    if (config.algorithm == JoinAlgorithm::S3J) {
        if (config.partition_strategy != PartitionStrategy::CENTROID) {
            result.addError(
                "S3J algorithm requires Centroid partition strategy. "
                "Current: " + sageFlow::toString(config.partition_strategy) + ". "
                "S3J uses centroid-based clustering for spatial partitioning.");
        }
        if (config.window_state_type == WindowStateType::SHARED) {
            result.addError(
                "S3J algorithm is incompatible with SharedWindowState. "
                "Use PartitionedWindowState instead for proper cluster management.");
        }
    }

    // ClusteredJoin 类似 S3J
    if (config.algorithm == JoinAlgorithm::CLUSTERED_JOIN) {
        if (config.partition_strategy != PartitionStrategy::CENTROID) {
            result.addError(
                "ClusteredJoin algorithm requires Centroid partition strategy. "
                "Current: " + sageFlow::toString(config.partition_strategy) + ".");
        }
        if (config.window_state_type != WindowStateType::PARTITIONED &&
            config.window_state_type != WindowStateType::TWO_TIER) {
            result.addError(
                "ClusteredJoin requires PartitionedWindowState or TwoTierWindowState. "
                "Current: " + sageFlow::toString(config.window_state_type) + ".");
        }
    }
}

void JoinConfigValidator::checkParameterRanges(
    const JoinStrategyConfig& config,
    ValidationResult& result) {

    // similarity_threshold: [0.0, 1.0]
    if (config.similarity_threshold < 0.0 || config.similarity_threshold > 1.0) {
        result.addError(
            "similarity_threshold must be in range [0.0, 1.0], got: " +
            std::to_string(config.similarity_threshold));
    }

    // dimension > 0
    if (config.dimension <= 0) {
        result.addError(
            "dimension must be positive, got: " +
            std::to_string(config.dimension));
    }

    // num_partitions > 0
    if (config.num_partitions <= 0) {
        result.addError(
            "num_partitions must be positive, got: " +
            std::to_string(config.num_partitions));
    }

    // window_size_ms > 0
    if (config.window_size_ms <= 0) {
        result.addError(
            "window_size_ms must be positive, got: " +
            std::to_string(config.window_size_ms));
    }

    // step_size_ms > 0 && <= window_size_ms
    if (config.step_size_ms <= 0) {
        result.addError(
            "step_size_ms must be positive, got: " +
            std::to_string(config.step_size_ms));
    } else if (config.step_size_ms > config.window_size_ms) {
        result.addError(
            "step_size_ms (" + std::to_string(config.step_size_ms) +
            ") cannot exceed window_size_ms (" +
            std::to_string(config.window_size_ms) + ")");
    }

    // IVF 参数验证
    if (config.ivf_nlist <= 0) {
        result.addError(
            "ivf_nlist must be positive, got: " +
            std::to_string(config.ivf_nlist));
    }

    if (config.ivf_nprobes <= 0) {
        result.addError(
            "ivf_nprobes must be positive, got: " +
            std::to_string(config.ivf_nprobes));
    }

    if (config.ivf_nprobes > config.ivf_nlist) {
        result.addError(
            "ivf_nprobes (" + std::to_string(config.ivf_nprobes) +
            ") cannot exceed ivf_nlist (" +
            std::to_string(config.ivf_nlist) + ")");
    }

    // HNSW 参数验证
    if (config.hnsw_m <= 0) {
        result.addError(
            "hnsw_m must be positive, got: " +
            std::to_string(config.hnsw_m));
    }

    if (config.hnsw_ef_construction <= 0) {
        result.addError(
            "hnsw_ef_construction must be positive, got: " +
            std::to_string(config.hnsw_ef_construction));
    }

    if (config.hnsw_ef_construction < config.hnsw_m) {
        result.addError(
            "hnsw_ef_construction (" + std::to_string(config.hnsw_ef_construction) +
            ") should be >= hnsw_m (" + std::to_string(config.hnsw_m) +
            ") for good recall");
    }

    if (config.hnsw_ef_search <= 0) {
        result.addError(
            "hnsw_ef_search must be positive, got: " +
            std::to_string(config.hnsw_ef_search));
    }

    // VSJoin 参数验证
    if (config.vsjoin_num_hash_functions <= 0 || config.vsjoin_num_hash_functions > 64) {
        result.addError(
            "vsjoin_num_hash_functions must be in range [1, 64], got: " +
            std::to_string(config.vsjoin_num_hash_functions));
    }

    if (config.vsjoin_boundary_threshold < 0.0 || config.vsjoin_boundary_threshold > 1.0) {
        result.addError(
            "vsjoin_boundary_threshold must be in range [0.0, 1.0], got: " +
            std::to_string(config.vsjoin_boundary_threshold));
    }

    if (config.vsjoin_async_threads <= 0) {
        result.addError(
            "vsjoin_async_threads must be positive, got: " +
            std::to_string(config.vsjoin_async_threads));
    }

    // S3J 参数验证
    if (config.s3j_num_centroids <= 0) {
        result.addError(
            "s3j_num_centroids must be positive, got: " +
            std::to_string(config.s3j_num_centroids));
    }

    if (config.s3j_load_threshold < 0.0 || config.s3j_load_threshold > 1.0) {
        result.addError(
            "s3j_load_threshold must be in range [0.0, 1.0], got: " +
            std::to_string(config.s3j_load_threshold));
    }

    // ClusteredJoin 参数验证
    if (config.clustered_overlap_ratio < 0.0 || config.clustered_overlap_ratio > 1.0) {
        result.addError(
            "clustered_overlap_ratio must be in range [0.0, 1.0], got: " +
            std::to_string(config.clustered_overlap_ratio));
    }

    if (config.clustered_rebalance_threshold < 0.0 || config.clustered_rebalance_threshold > 1.0) {
        result.addError(
            "clustered_rebalance_threshold must be in range [0.0, 1.0], got: " +
            std::to_string(config.clustered_rebalance_threshold));
    }

    if (config.clustered_training_samples <= 0) {
        result.addError(
            "clustered_training_samples must be positive, got: " +
            std::to_string(config.clustered_training_samples));
    }

    // HDR-Tree 参数验证
    if (config.hdr_projected_dim <= 0) {
        result.addError(
            "hdr_projected_dim must be positive, got: " +
            std::to_string(config.hdr_projected_dim));
    }

    if (config.hdr_projected_dim >= config.dimension && config.dimension > 0) {
        result.addError(
            "hdr_projected_dim (" + std::to_string(config.hdr_projected_dim) +
            ") should be less than dimension (" +
            std::to_string(config.dimension) + ") for dimensionality reduction");
    }

    if (config.hdr_max_node_size <= 0) {
        result.addError(
            "hdr_max_node_size must be positive, got: " +
            std::to_string(config.hdr_max_node_size));
    }

    // Two-tier 参数验证
    if (config.two_tier_compact_threshold == 0) {
        result.addError(
            "two_tier_compact_threshold must be positive");
    }
}

void JoinConfigValidator::checkDependencies(
    const JoinStrategyConfig& config,
    ValidationResult& result) {

    // HDR-Tree 需要 PCA 组件（运行时检查，这里只添加警告）
    if (config.algorithm == JoinAlgorithm::HDR_TREE) {
        result.addWarning(
            "HDR-Tree requires PCA component to be trained before use. "
            "Make sure to call trainPCA() with sample data before processing.");
    }

    // VSJoin 依赖多个组件
    if (config.algorithm == JoinAlgorithm::VSJOIN) {
        result.addWarning(
            "VSJoin requires PartitionCoordinator, AsyncCandidateGenerator, "
            "and DistanceVerifier components. These will be created automatically "
            "by JoinStrategyFactory.");
    }

    // S3J 需要自适应组件
    if (config.algorithm == JoinAlgorithm::S3J && config.s3j_enable_adaptive) {
        result.addWarning(
            "S3J adaptive mode requires CentroidPartitioner with dynamic rebalancing. "
            "Ensure sufficient training samples for stable clustering.");
    }

    // ClusteredJoin 边界复制
    if (config.algorithm == JoinAlgorithm::CLUSTERED_JOIN &&
        config.clustered_border_replication) {
        result.addWarning(
            "ClusteredJoin with border replication enabled may increase memory usage. "
            "Overlap ratio: " + std::to_string(config.clustered_overlap_ratio) + ".");
    }
}

void JoinConfigValidator::checkPerformanceHints(
    const JoinStrategyConfig& config,
    ValidationResult& result) {

    // BruteForce 配 PARTITIONED 会警告
    if (config.algorithm == JoinAlgorithm::BRUTEFORCE &&
        config.window_state_type == WindowStateType::PARTITIONED) {
        result.addWarning(
            "Using BruteForce with PartitionedWindowState may reduce recall "
            "if similar vectors are in different partitions. Consider using "
            "SharedWindowState for 100% recall, or ensure your partitioner "
            "groups similar vectors together.");
    }

    // 大窗口大小警告
    if (config.window_size_ms > 60000) {  // > 1 minute
        result.addWarning(
            "Large window size (" + std::to_string(config.window_size_ms) +
            "ms) may cause high memory usage and processing latency. "
            "Consider using smaller windows with appropriate step size.");
    }

    // 非常小的窗口警告
    if (config.window_size_ms < 100) {  // < 100ms
        result.addWarning(
            "Very small window size (" + std::to_string(config.window_size_ms) +
            "ms) may result in limited matching opportunities. "
            "Consider using larger windows for better coverage.");
    }

    // HNSW ef_search 过小警告
    if ((config.algorithm == JoinAlgorithm::HNSW ||
         config.index_strategy == IndexStrategy::PARTITIONED) &&
        config.hnsw_ef_search < 50) {
        result.addWarning(
            "Low hnsw_ef_search (" + std::to_string(config.hnsw_ef_search) +
            ") may result in lower recall. Consider increasing to 50+ "
            "for better quality results.");
    }

    // IVF nprobes 过小警告
    if ((config.algorithm == JoinAlgorithm::IVF ||
         config.index_strategy == IndexStrategy::PARTITIONED) &&
        config.ivf_nprobes < 5) {
        result.addWarning(
            "Low ivf_nprobes (" + std::to_string(config.ivf_nprobes) +
            ") may result in lower recall. Consider increasing to 5+ "
            "for more accurate search results.");
    }

    // 分区数过多警告
    if (config.num_partitions > 64) {
        result.addWarning(
            "High number of partitions (" + std::to_string(config.num_partitions) +
            ") may introduce coordination overhead. "
            "Consider fewer partitions unless data volume is very large.");
    }

    // 分区数与并行度不匹配提示
    // 这个检查在 JoinStrategyFactory 中更合适，因为需要知道 parallelism

    // 阈值接近边界值警告
    if (config.similarity_threshold > 0.99) {
        result.addWarning(
            "Very high similarity threshold (" +
            std::to_string(config.similarity_threshold) +
            ") may result in very few or no matches. "
            "Consider lowering if you expect sparse results.");
    }

    if (config.similarity_threshold < 0.1) {
        result.addWarning(
            "Very low similarity threshold (" +
            std::to_string(config.similarity_threshold) +
            ") may result in excessive matches and high processing cost. "
            "Consider increasing if you see performance issues.");
    }

    // Eager 模式在高吞吐场景警告
    if (config.is_eager && config.window_size_ms > 10000) {
        result.addWarning(
            "Eager mode with large window (" +
            std::to_string(config.window_size_ms) + "ms) may cause "
            "high processing latency per record. Consider Lazy mode for "
            "batch processing efficiency.");
    }
}

}  // namespace sageFlow

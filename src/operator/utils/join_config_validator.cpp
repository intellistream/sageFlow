//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-06: JoinConfigValidator 配置验证与错误处理
//

#include "operator/utils/join_config_validator.h"
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
std::string toString(ClusteredIndexType cit);

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
    checkColdStartConfig(config, result);  // 添加冷启动配置检查

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
            // LSH 需要 PARTITIONED_VECTOR（LSH/VSJoin 均复用）
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
            // LSH 默认采用 LSH 分区
            return PartitionStrategy::LSH;

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
        
        // 新增：验证 clustered_index_type 相关参数
        if (config.clustered_index_type == ClusteredIndexType::HNSW) {
            // HNSW 需要有效的参数
            if (config.hnsw_m <= 0 || config.hnsw_ef_construction <= 0) {
                result.addError(
                    "ClusteredJoin with HNSW index requires valid hnsw_m and "
                    "hnsw_ef_construction. Current: hnsw_m=" + 
                    std::to_string(config.hnsw_m) + ", hnsw_ef_construction=" +
                    std::to_string(config.hnsw_ef_construction) + ".");
            }
        }
        
        if (config.clustered_index_type == ClusteredIndexType::IVF) {
            // IVF 需要检查 nlist
            if (config.ivf_nlist <= 0) {
                result.addError(
                    "ClusteredJoin with IVF index requires valid ivf_nlist. "
                    "Current: " + std::to_string(config.ivf_nlist) + ".");
            }
        }
        
        // 检查 multicast_k 范围
        if (config.clustered_multicast_k < 0) {
            result.addError(
                "clustered_multicast_k must be >= 0 (0=use overlap_ratio, >=1=fixed k). "
                "Current: " + std::to_string(config.clustered_multicast_k) + ".");
        }
        
        // **关键约束**：ClusteredJoin 的 num_partitions 必须等于运行时 parallelism
        //
        // 原因：CentroidPartitioner 使用 `partition_idx % num_channels` 映射，
        // 若 num_partitions != parallelism，会导致逻辑分区折叠到同一物理 subtask，
        // 从而破坏 multicast_k / overlap_ratio 的语义并导致召回率下降。
        //
        // 但这里无法静态校验（parallelism 来自 RuntimeContext），因此不在 Validator 中产生误导性警告；
        // 运行时由 JoinOperator::initializeWithStrategyConfig() 强制检查并抛错。
        
        // 警告：multicast_k 和 overlap_ratio 的关系
        if (config.clustered_multicast_k > 0 && config.clustered_overlap_ratio != 0.1) {
            result.addWarning(
                "clustered_multicast_k is set to " + std::to_string(config.clustered_multicast_k) + 
                ", overlap_ratio will be ignored. "
                "Remove overlap_ratio from config to avoid confusion.");
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

    // ClusteredJoin multicast 性能提示
    if (config.algorithm == JoinAlgorithm::CLUSTERED_JOIN &&
        config.clustered_multicast_k > 1) {
        result.addWarning(
            "ClusteredJoin with multicast_k=" + std::to_string(config.clustered_multicast_k) + 
            " may increase memory usage and computation due to vector replication.");
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

void JoinConfigValidator::checkColdStartConfig(
    const JoinStrategyConfig& config,
    ValidationResult& result) {

    // 仅在启用冷启动时验证
    if (!config.enable_cold_start) {
        return;
    }

    // 冷启动仅支持 ClusteredJoin
    if (config.algorithm != JoinAlgorithm::CLUSTERED_JOIN) {
        // 对于非 ClusteredJoin 算法，仅给出警告，不报错
        // 这样可以保持向后兼容性
        result.addWarning(
            "enable_cold_start is true but algorithm is not CLUSTERED_JOIN (current: " +
            sageFlow::toString(config.algorithm) + "). "
            "Cold-start training is only effective for ClusteredJoin. "
            "Consider setting enable_cold_start=false for other algorithms.");
        return;  // 不继续检查其他冷启动参数
    }

    // 以下验证仅针对 ClusteredJoin 算法

    // 验证 training_samples
    if (config.clustered_training_samples < 10) {
        result.addError(
            "clustered_training_samples must be >= 10 for meaningful training. "
            "Current: " + std::to_string(config.clustered_training_samples) + ". "
            "Increase training_samples to at least 10.");
    }

    if (config.clustered_training_samples > 100000) {
        result.addWarning(
            "clustered_training_samples is very large (" +
            std::to_string(config.clustered_training_samples) + "). "
            "This may cause high memory usage during training. "
            "Consider limiting to <= 100000 unless necessary.");
    }

    // 验证与分区策略的兼容性
    if (config.partition_strategy != PartitionStrategy::CENTROID) {
        result.addError(
            "Cold-start training requires CENTROID partition strategy. "
            "Current: " + sageFlow::toString(config.partition_strategy) + ". "
            "Change partition_strategy to CENTROID for cold-start support.");
    }

    // 验证 training_samples 与 num_partitions 的关系
    if (config.clustered_training_samples < static_cast<size_t>(config.num_partitions * 5)) {
        result.addWarning(
            "clustered_training_samples (" +
            std::to_string(config.clustered_training_samples) +
            ") is less than 5 * num_partitions (" +
            std::to_string(config.num_partitions * 5) + "). "
            "This may result in poor clustering quality. "
            "Increase training_samples to at least 5 * num_partitions for better results.");
    }

    // 验证 deduplicate_during_broadcast 配置
    // 在高并行度下建议启用去重
    if (!config.deduplicate_during_broadcast && config.num_partitions > 4) {
        result.addWarning(
            "deduplicate_during_broadcast is disabled with high parallelism (" +
            std::to_string(config.num_partitions) + " partitions). "
            "This may cause duplicate outputs during broadcast phase. "
            "Consider enabling deduplicate_during_broadcast for correctness.");
    }

    // 如果 training_samples = 0，警告冷启动未启用
    if (config.clustered_training_samples == 0) {
        result.addWarning(
            "clustered_training_samples is 0, but enable_cold_start is true. "
            "Cold-start will not be effective without training samples. "
            "Either set training_samples > 0 or disable cold-start.");
    }

    // 验证与 multicast 的兼容性
    if (!config.clustered_multicast_enabled) {
        result.addWarning(
            "clustered_multicast_enabled is false. "
            "During cold-start broadcast phase, all subtasks will receive data, "
            "but after training, single-partition routing may cause low recall. "
            "Consider enabling multicast for consistent behavior.");
    }
}

}  // namespace sageFlow

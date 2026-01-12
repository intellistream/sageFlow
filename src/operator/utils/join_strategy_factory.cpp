#include "operator/utils/join_strategy_factory.h"

#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "operator/join_operator_methods/ivf_method.h"
#include "operator/join_operator_methods/hnsw.h"
#include "operator/join_operator_methods/hdr_tree_method.h"
#include "operator/join_operator_methods/clustered_join_method.h"
#include "operator/join_operator_methods/s3j_method.h"
#include "state/shared_window_state.h"
#include "state/partitioned_window_state.h"
#include "state/two_tier_window_state.h"
#include "state/partitioned_vector_state.h"
#include "execution/centroid_partitioner.h"
#include "execution/vector_space_partitioner.h"
#include "index/partitioned_index.h"
#include "utils/logger.h"

#include <sstream>
#include <stdexcept>

namespace sageFlow {

// ==================== StrategyComponents 方法实现 ====================

std::string JoinStrategyFactory::StrategyComponents::summary() const {
    std::ostringstream oss;
    oss << "StrategyComponents {\n"
        << "  join_method: " << (join_method ? "yes" : "no") << "\n"
        << "  left_state: " << (left_state ? "yes" : "no") << "\n"
        << "  right_state: " << (right_state ? "yes" : "no") << "\n"
        << "  partitioner: " << (partitioner ? "yes" : "no") << "\n"
        << "  left_index_id: " << left_index_id << "\n"
        << "  right_index_id: " << right_index_id << "\n"
        << "  left_partitioned_index: " << (left_partitioned_index ? "yes" : "no") << "\n"
        << "  right_partitioned_index: " << (right_partitioned_index ? "yes" : "no") << "\n"
        << "  vector_partitioner: " << (vector_partitioner ? "yes" : "no") << "\n"
        << "  centroid_partitioner: " << (centroid_partitioner ? "yes" : "no") << "\n"
        << "}";
    return oss.str();
}

// ==================== 主工厂方法 ====================

JoinStrategyFactory::StrategyComponents JoinStrategyFactory::create(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> concurrency_manager,
    size_t parallelism) {
    
    // 1. 验证配置
    auto errors = config.validate();
    if (!errors.empty()) {
        std::ostringstream oss;
        oss << "Invalid JoinStrategyConfig: ";
        for (const auto& e : errors) {
            oss << e << "; ";
        }
        throw std::runtime_error(oss.str());
    }

    StrategyComponents components;
    
    // 2. 创建索引对
    // 统一架构：所有使用索引的 Join 方法都通过 ConcurrencyManager 管理共享索引
    // - 共享索引策略：IVF, HNSW, HDR_TREE
    // - BRUTEFORCE 使用 BruteForceBaseline，不依赖索引
    // - 分区索引策略（分区内部使用索引管理）：CLUSTERED_JOIN, S3J
    bool need_index = (config.index_strategy == IndexStrategy::SHARED ||
                      config.algorithm == JoinAlgorithm::CLUSTERED_JOIN ||
                      config.algorithm == JoinAlgorithm::S3J);
    
    if (need_index) {
        if (!createIndexPair(config, concurrency_manager, 
                            components.left_index_id, components.right_index_id)) {
            SAGEFLOW_LOG_WARN("JOIN_FACTORY", "Failed to create index pair, "
                             "will proceed without index");
        }
    }
    
    // 3. 创建 JoinMethod
    components.join_method = createJoinMethod(config, concurrency_manager,
                                             components.left_index_id,
                                             components.right_index_id);
    // 绑定 alpha 到该 pipeline 的 JoinMethod（方案 A：ComputeEngine 纯计算，alpha 由上层传入）
    if (components.join_method) {
        components.join_method->setSimilarityAlpha(config.similarity_alpha);
    }
    
    // 4. 创建 WindowState
    components.left_state = createWindowState(config, parallelism);
    components.right_state = createWindowState(config, parallelism);
    
    // 5. 创建 Partitioner
    components.partitioner = createPartitioner(config);
    
    // 6. 创建算法特定组件
    switch (config.algorithm) {
        case JoinAlgorithm::VSJOIN: {
            // VSJoin 需要 VectorSpacePartitioner
            components.vector_partitioner = createVectorSpacePartitioner(config);
            // TODO: 创建其他 VSJoin 组件（coordinator, async_gen, verifier）
            // Issue URL: https://github.com/intellistream/sageFlow/issues/79
            break;
        }
        case JoinAlgorithm::S3J:
        case JoinAlgorithm::CLUSTERED_JOIN: {
            // S3J 和 ClusteredJoin 需要 CentroidPartitioner
            components.centroid_partitioner = createCentroidPartitioner(config);
            break;
        }
        default:
            break;
    }
    
    SAGEFLOW_LOG_INFO("JOIN_FACTORY", "Created strategy components: algorithm={} partition={} window_state={}",
                     toString(config.algorithm),
                     toString(config.partition_strategy),
                     toString(config.window_state_type));
    
    return components;
}

// ==================== JoinMethod 创建 ====================

std::unique_ptr<BaseMethod> JoinStrategyFactory::createJoinMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> concurrency_manager,
    int left_index_id,
    int right_index_id) {
    
    switch (config.algorithm) {
        case JoinAlgorithm::BRUTEFORCE:
            return createBruteForceMethod(config, concurrency_manager, 
                                         left_index_id, right_index_id);
        case JoinAlgorithm::IVF:
            return createIvfMethod(config, concurrency_manager, 
                                   left_index_id, right_index_id);
        case JoinAlgorithm::HNSW:
            return createHnswMethod(config, concurrency_manager, 
                                    left_index_id, right_index_id);
        case JoinAlgorithm::HDR_TREE:
            return createHdrTreeMethod(config, concurrency_manager, 
                                       left_index_id, right_index_id);
        case JoinAlgorithm::CLUSTERED_JOIN:
            return createClusteredJoinMethod(config, concurrency_manager, 
                                            left_index_id, right_index_id);
        case JoinAlgorithm::S3J:
            return createS3JMethod(config, concurrency_manager, 
                                   left_index_id, right_index_id);
        case JoinAlgorithm::VSJOIN:
            return createVSJoinMethod(config, concurrency_manager, 
                                      left_index_id, right_index_id);
        default:
            throw std::runtime_error("Unknown JoinAlgorithm: " + toString(config.algorithm));
    }
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createBruteForceMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    (void)cm;
    (void)left_idx;
    (void)right_idx;
    // 对 BRUTEFORCE，我们继续使用基于 WindowState 的 BruteForceBaseline：
    // - 这样左右流的数据完全隔离，不会因为共享 StorageManager 的全局扫描而“混流”；
    // - 同时 alpha/similarity_mode 仍由 JoinStrategyConfig 提供（不依赖索引层）。
    return std::make_unique<BruteForceBaseline>(
        config.similarity_threshold,
        config.similarity_mode,
        config.similarity_alpha);
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createIvfMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    IVFMethod::Config ivf_config;
    ivf_config.similarity_threshold = config.similarity_threshold;
    ivf_config.nlist = config.ivf_nlist;
    ivf_config.nprobes = config.ivf_nprobes;
    ivf_config.rebuild_threshold = config.ivf_rebuild_threshold;
    ivf_config.use_existing_index = true;
    
    SAGEFLOW_LOG_INFO("JoinStrategyFactory", "Creating IVFMethod with nlist={} nprobes={} threshold={}",
                      ivf_config.nlist, ivf_config.nprobes, ivf_config.similarity_threshold);
    
    auto method = std::make_unique<IVFMethod>(ivf_config);
    method->setIndexIds(left_idx, right_idx);
    method->setConcurrencyManager(cm);
    return method;
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createHnswMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    HNSWJoinMethod::Config hnsw_config;
    hnsw_config.m = config.hnsw_m;
    hnsw_config.ef_construction = config.hnsw_ef_construction;
    hnsw_config.ef_search = config.hnsw_ef_search;
    hnsw_config.use_existing_index = true;
    
    return std::make_unique<HNSWJoinMethod>(
        left_idx, right_idx,
        config.similarity_threshold,
        cm,
        hnsw_config);
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createHdrTreeMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    HDRTreeMethod::Config hdr_config;
    hdr_config.similarity_threshold = config.similarity_threshold;
    hdr_config.projected_dim = config.hdr_projected_dim;
    hdr_config.pca_sample_size = config.hdr_pca_sample_size;
    
    return std::make_unique<HDRTreeMethod>(
        left_idx, right_idx,
        config.similarity_threshold,
        cm,
        hdr_config);
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createClusteredJoinMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    // 构建 ClusteredJoinMethod::Config
    // 注意：新架构中每个 subtask 通过 initialize() 创建独立索引，
    // left_idx 和 right_idx 参数在新架构中不再使用
    ClusteredJoinMethod::Config cj_config;
    cj_config.similarity_threshold = config.similarity_threshold;
    cj_config.dimension = config.dimension;
    cj_config.window_size_ms = config.window_size_ms;
    cj_config.num_partitions = config.num_partitions;
    cj_config.overlap_ratio = config.clustered_overlap_ratio;
    cj_config.rebalance_threshold = config.clustered_rebalance_threshold;
    cj_config.training_samples = config.clustered_training_samples;
    cj_config.index_type = config.clustered_index_type;
    
    // IVF 参数
    cj_config.ivf_nlist = config.ivf_nlist;
    cj_config.ivf_nprobes = config.ivf_nprobes;
    
    // HNSW 参数
    cj_config.hnsw_m = config.hnsw_m;
    cj_config.hnsw_ef_construction = config.hnsw_ef_construction;
    cj_config.hnsw_ef_search = config.hnsw_ef_search;
    
    return std::make_unique<ClusteredJoinMethod>(cj_config);
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createS3JMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    S3JConfig s3j_config;
    s3j_config.similarity_threshold = config.similarity_threshold;
    s3j_config.num_partitions = config.s3j_num_centroids;
    s3j_config.adapt_interval_ms = config.s3j_adapt_interval_ms;
    s3j_config.load_threshold = config.s3j_load_threshold;
    s3j_config.enable_adaptive = config.s3j_enable_adaptive;
    s3j_config.dimension = config.dimension;
    s3j_config.nlist = config.ivf_nlist;
    s3j_config.nprobes = config.ivf_nprobes;
    
    return std::make_unique<S3JMethod>(
        left_idx, right_idx,
        config.similarity_threshold,
        cm,
        s3j_config);
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createVSJoinMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    // VSJoin 暂时使用 BruteForce 作为基础
    // TODO: 实现完整的 VSJoin 方法
    // Issue URL: https://github.com/intellistream/sageFlow/issues/78
    SAGEFLOW_LOG_WARN("JOIN_FACTORY", "VSJoin method is not fully implemented yet, "
                     "using BruteForce as fallback");
    return createBruteForceMethod(config, cm, left_idx, right_idx);
}

// ==================== WindowState 创建 ====================

std::unique_ptr<WindowState> JoinStrategyFactory::createWindowState(
    const JoinStrategyConfig& config,
    size_t parallelism) {
    
    switch (config.window_state_type) {
        case WindowStateType::SHARED:
            return std::make_unique<SharedWindowState>();
            
        case WindowStateType::PARTITIONED:
            return std::make_unique<PartitionedWindowState>(parallelism);
            
        case WindowStateType::TWO_TIER:
            return std::make_unique<TwoTierWindowState>(
                parallelism,
                config.two_tier_compact_threshold);
            
        case WindowStateType::PARTITIONED_VECTOR: {
            // 需要先创建 VectorSpacePartitioner
            auto vsp = createVectorSpacePartitioner(config);
            return std::make_unique<PartitionedVectorState>(
                static_cast<size_t>(config.num_partitions),
                vsp,
                config.two_tier_compact_threshold,
                config.two_tier_enable_boundary_tracking);
        }
            
        default:
            throw std::runtime_error("Unknown WindowStateType: " + 
                                    toString(config.window_state_type));
    }
}

// ==================== Partitioner 创建 ====================

std::unique_ptr<IPartitioner> JoinStrategyFactory::createPartitioner(
    const JoinStrategyConfig& config) {
    
    switch (config.partition_strategy) {
        case PartitionStrategy::ROUND_ROBIN:
            return std::make_unique<RoundRobinPartitioner>();
            
        case PartitionStrategy::KEY_HASH:
            return std::make_unique<KeyPartitioner>();
            
        case PartitionStrategy::VECTOR_HASH:
            return std::make_unique<VectorHashPartitioner>();
            
        case PartitionStrategy::LSH: {
            // LSH 分区需要使用基于 VectorSpacePartitioner 的适配器
            // 这里暂时返回 VectorHashPartitioner 作为替代
            // TODO: 实现 LSHPartitionerAdapter
            // Issue URL: https://github.com/intellistream/sageFlow/issues/77
            SAGEFLOW_LOG_WARN("JOIN_FACTORY", 
                             "LSH partitioner adapter not implemented, using VectorHash");
            return std::make_unique<VectorHashPartitioner>();
        }
            
        case PartitionStrategy::CENTROID: {
            // Centroid 分区需要创建 CentroidPartitioner
            // 使用完整的配置参数，包括冷启动训练参数
            CentroidPartitioner::Config cp_config;
            cp_config.num_partitions = config.num_partitions;
            cp_config.overlap_ratio = config.clustered_overlap_ratio;
            cp_config.dimension = config.dimension;
            cp_config.seed = 42;
            // 关键修复：设置冷启动训练参数
            cp_config.training_samples = static_cast<size_t>(config.clustered_training_samples);
            cp_config.enable_cold_start = config.enable_cold_start;
            cp_config.multicast_k = config.clustered_multicast_k;
            
            auto partitioner = std::make_unique<CentroidPartitioner>(cp_config);
            partitioner->setMulticastEnabled(config.clustered_multicast_enabled);
            return partitioner;
        }
            
        default:
            throw std::runtime_error("Unknown PartitionStrategy: " + 
                                    toString(config.partition_strategy));
    }
}

// ==================== VectorSpacePartitioner 创建 ====================

std::shared_ptr<VectorSpacePartitioner> JoinStrategyFactory::createVectorSpacePartitioner(
    const JoinStrategyConfig& config) {
    
    switch (config.partition_strategy) {
        case PartitionStrategy::LSH:
            return std::make_shared<LSHPartitioner>(
                config.dimension,
                config.vsjoin_num_hash_functions,
                42,  // seed
                config.vsjoin_boundary_threshold);
            
        case PartitionStrategy::CENTROID: {
            // 使用 KMeansPartitioner
            return std::make_shared<KMeansPartitioner>(
                config.dimension,
                config.num_partitions,
                42);  // seed
        }
            
        default:
            // 默认使用 LSH
            return std::make_shared<LSHPartitioner>(
                config.dimension,
                8,    // num_hash_functions
                42,   // seed
                0.1); // boundary_threshold
    }
}

// ==================== CentroidPartitioner 创建 ====================

std::shared_ptr<CentroidPartitioner> JoinStrategyFactory::createCentroidPartitioner(
    const JoinStrategyConfig& config) {
    
    CentroidPartitioner::Config cp_config;
    cp_config.num_partitions = config.num_partitions;
    cp_config.overlap_ratio = config.clustered_overlap_ratio;
    cp_config.max_iterations = 100;
    cp_config.init_method = "kmeans++";
    cp_config.rebalance_threshold = config.clustered_rebalance_threshold;
    cp_config.seed = 42;
    cp_config.dimension = config.dimension;
    
    // 添加冷启动配置（修复：之前缺少这些设置）
    cp_config.training_samples = static_cast<size_t>(config.clustered_training_samples);
    cp_config.enable_cold_start = config.enable_cold_start;
    cp_config.multicast_k = config.clustered_multicast_k;
    
    SAGEFLOW_LOG_INFO("JOIN_FACTORY", 
        "Creating CentroidPartitioner: partitions={} cold_start={} training_samples={} multicast_k={}",
        cp_config.num_partitions, cp_config.enable_cold_start, 
        cp_config.training_samples, cp_config.multicast_k);
    
    return std::make_shared<CentroidPartitioner>(cp_config);
}

// ==================== 索引创建 ====================

bool JoinStrategyFactory::createIndexPair(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> concurrency_manager,
    int& out_left_id,
    int& out_right_id) {
    
    if (!concurrency_manager) {
        SAGEFLOW_LOG_ERROR("JOIN_FACTORY", "ConcurrencyManager is null");
        return false;
    }
    
    IndexType index_type = getIndexType(config);
    IndexParameters params = getIndexParameters(config);
    
    // 创建左流索引
    out_left_id = concurrency_manager->create_index(
        "factory_left_index",
        index_type,
        config.dimension,
        params);
    
    if (out_left_id < 0) {
        SAGEFLOW_LOG_ERROR("JOIN_FACTORY", "Failed to create left index");
        return false;
    }
    
    // 创建右流索引
    out_right_id = concurrency_manager->create_index(
        "factory_right_index",
        index_type,
        config.dimension,
        params);
    
    if (out_right_id < 0) {
        SAGEFLOW_LOG_ERROR("JOIN_FACTORY", "Failed to create right index");
        return false;
    }
    
    SAGEFLOW_LOG_INFO("JOIN_FACTORY", "Created index pair: left={} right={} type={}",
                     out_left_id, out_right_id, static_cast<int>(index_type));
    
    return true;
}

IndexType JoinStrategyFactory::getIndexType(const JoinStrategyConfig& config) {
    switch (config.algorithm) {
        case JoinAlgorithm::BRUTEFORCE:
            return IndexType::BruteForce;
        case JoinAlgorithm::IVF:
            return IndexType::IVF;
        case JoinAlgorithm::HNSW:
            return IndexType::HNSW;
        case JoinAlgorithm::HDR_TREE:
            return IndexType::HDRForest;
        case JoinAlgorithm::CLUSTERED_JOIN: {
            // ClusteredJoin 根据配置选择索引类型
            switch (config.clustered_index_type) {
                case ClusteredIndexType::BRUTEFORCE:
                    return IndexType::BruteForce;
                case ClusteredIndexType::IVF:
                    return IndexType::IVF;
                case ClusteredIndexType::HNSW:
                    return IndexType::HNSW;
                default:
                    return IndexType::BruteForce;
            }
        }
        case JoinAlgorithm::VSJOIN:
        case JoinAlgorithm::S3J:
            // 这些算法使用 IVF 索引
            return IndexType::IVF;
        default:
            return IndexType::BruteForce;
    }
}

IndexParameters JoinStrategyFactory::getIndexParameters(const JoinStrategyConfig& config) {
    switch (config.algorithm) {
        case JoinAlgorithm::IVF:
        case JoinAlgorithm::S3J:
        case JoinAlgorithm::VSJOIN: {
            IVFParameters params;
            params.nlist = config.ivf_nlist;
            params.nprobes = config.ivf_nprobes;
            params.rebuild_threshold = config.ivf_rebuild_threshold;
            return params;
        }
        case JoinAlgorithm::CLUSTERED_JOIN: {
            // ClusteredJoin 根据索引类型选择参数
            switch (config.clustered_index_type) {
                case ClusteredIndexType::IVF: {
                    IVFParameters params;
                    params.nlist = config.ivf_nlist;
                    params.nprobes = config.ivf_nprobes;
                    params.rebuild_threshold = config.ivf_rebuild_threshold;
                    return params;
                }
                case ClusteredIndexType::HNSW: {
                    HNSWParameters params;
                    params.m = config.hnsw_m;
                    params.ef_construction = config.hnsw_ef_construction;
                    params.ef_search = config.hnsw_ef_search;
                    return params;
                }
                default:
                    return NoParameters{};
            }
        }
        case JoinAlgorithm::HNSW: {
            HNSWParameters params;
            params.m = config.hnsw_m;
            params.ef_construction = config.hnsw_ef_construction;
            params.ef_search = config.hnsw_ef_search;
            return params;
        }
        case JoinAlgorithm::HDR_TREE: {
            HDRForestParameters params;
            params.n_clusters = config.hdr_projected_dim; // Mapping projected_dim to n_clusters for now as placeholder
            params.f_sections = config.hdr_max_node_size; // Mapping max_node_size to f_sections for now
            return params;
        }
        default:
            return NoParameters{};
    }
}

}  // namespace sageFlow

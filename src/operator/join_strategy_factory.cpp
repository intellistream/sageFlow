#include "operator/join_strategy_factory.h"

#include "operator/join_operator_methods/bruteforce.h"
#include "operator/join_operator_methods/ivf.h"
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

    // 检查混合并行情况（算子并行度 > 1 且 FAISS OMP 已启用）
    // 如果检测到这种情况，我们将强制禁用 FAISS OMP 以避免线程争用。
    // effective_config 是配置的副本，仅用于修改 FAISS 相关的设置（如禁用 OMP），
    // 而原始 config 将用于创建 WindowState 和 Partitioner，以确保不影响其他组件的功能。
    JoinStrategyConfig effective_config = config;
    bool is_faiss_algo = (config.algorithm == JoinAlgorithm::FAISS_IVF || 
                          config.algorithm == JoinAlgorithm::FAISS_HNSW);
    
    if (is_faiss_algo && !config.faiss_disable_omp && parallelism > 1) {
        SAGEFLOW_LOG_WARN("JOIN_FACTORY", 
            "Detected hybrid parallelism: Operator Parallelism={} with FAISS OMP enabled. "
            "Forcing FAISS OMP disabled to avoid thread contention and scheduling chaos. "
            "To use OMP, set parallelism=1.", parallelism);
        effective_config.faiss_disable_omp = true;
    }
    
    StrategyComponents components;
    
    // 2. 创建索引对（如果使用共享索引）
    if (effective_config.index_strategy == IndexStrategy::SHARED) {
        if (!createIndexPair(effective_config, concurrency_manager, 
                            components.left_index_id, components.right_index_id)) {
            SAGEFLOW_LOG_WARN("JOIN_FACTORY", "Failed to create shared index pair, "
                             "will proceed without index");
        }
    }
    
    // 3. 创建 JoinMethod
    components.join_method = createJoinMethod(effective_config, concurrency_manager,
                                             components.left_index_id,
                                             components.right_index_id);
    
    // 4. 创建 WindowState
    components.left_state = createWindowState(config, parallelism);
    components.right_state = createWindowState(config, parallelism);
    
    // 5. 创建 Partitioner
    components.partitioner = createPartitioner(config);
    
    // 6. 创src/operator/join_strategy_factory.cppsrc/operator/join_strategy_factory.cpp
    switch (effective_config.algorithm) {
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
                     toString(effective_config.algorithm),
                     toString(effective_config.partition_strategy),
                     toString(effective_config.window_state_type));
    
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
        case JoinAlgorithm::FAISS_IVF:
            return createIvfMethod(config, concurrency_manager, 
                                   left_index_id, right_index_id);
        case JoinAlgorithm::FAISS_HNSW:
            return createHnswMethod(config, concurrency_manager, 
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
    
    return std::make_unique<BruteForceJoinMethod>(
        left_idx, right_idx,
        config.similarity_threshold,
        cm);
}

std::unique_ptr<BaseMethod> JoinStrategyFactory::createIvfMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int left_idx, int right_idx) {
    
    return std::make_unique<IvfJoinMethod>(
        left_idx, right_idx,
        config.similarity_threshold,
        cm);
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
    
    ClusteredJoinMethod::Config cj_config;
    cj_config.similarity_threshold = config.similarity_threshold;
    cj_config.num_partitions = config.num_partitions;
    cj_config.overlap_ratio = config.clustered_overlap_ratio;
    cj_config.rebalance_threshold = config.clustered_rebalance_threshold;
    cj_config.use_border_replication = config.clustered_border_replication;
    cj_config.dimension = config.dimension;
    cj_config.training_samples = config.clustered_training_samples;
    
    return std::make_unique<ClusteredJoinMethod>(
        left_idx, right_idx,
        cj_config,
        cm);
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
    
    // VSJoin 暂时使用 BruteForce 作为基
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
            auto cp = createCentroidPartitioner(config);
            // CentroidPartitioner 实现了 IPartitioner 接口
            // 但这里需要返回 unique_ptr，所以创建一个包装器
            // 或者直接返回新创建的实例
            CentroidPartitioner::Config cp_config;
            cp_config.num_partitions = config.num_partitions;
            cp_config.overlap_ratio = config.clustered_overlap_ratio;
            cp_config.dimension = config.dimension;
            cp_config.seed = 42;
            return std::make_unique<CentroidPartitioner>(cp_config);
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
        case JoinAlgorithm::FAISS_IVF:
            return IndexType::FaissIVF;
        case JoinAlgorithm::FAISS_HNSW:
            return IndexType::FaissHNSW;
        case JoinAlgorithm::HNSW:
            return IndexType::HNSW;
        case JoinAlgorithm::HDR_TREE:
            return IndexType::HDRForest;
        case JoinAlgorithm::VSJOIN:
        case JoinAlgorithm::S3J:
        case JoinAlgorithm::CLUSTERED_JOIN:
            // 这些算法使用分区索引，内部可能是 IVF
            return IndexType::IVF;
        default:
            return IndexType::BruteForce;
    }
}

IndexParameters JoinStrategyFactory::getIndexParameters(const JoinStrategyConfig& config) {
    switch (config.algorithm) {
        case JoinAlgorithm::IVF:
        case JoinAlgorithm::S3J:
        case JoinAlgorithm::CLUSTERED_JOIN:
        case JoinAlgorithm::VSJOIN: {
            IVFParameters params;
            params.nlist = config.ivf_nlist;
            params.nprobes = config.ivf_nprobes;
            params.rebuild_threshold = config.ivf_rebuild_threshold;
            return params;
        }
        case JoinAlgorithm::FAISS_IVF: {
            FaissIVFParameters params;
            params.nlist = config.ivf_nlist;
            params.nprobe = config.ivf_nprobes;
            params.disable_omp = config.faiss_disable_omp;
            return params;
        }
        case JoinAlgorithm::FAISS_HNSW: {
            FaissHNSWParameters params;
            params.M = config.hnsw_m;
            params.efConstruction = config.hnsw_ef_construction;
            params.efSearch = config.hnsw_ef_search;
            params.disable_omp = config.faiss_disable_omp;
            return params;
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

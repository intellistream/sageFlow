#include "operator/utils/join_strategy_config.h"
#include "utils/logger.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>
#include <toml++/toml.h>

namespace sageFlow {

// ==================== 辅助函数 ====================

static std::string toLower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return result;
}

// ==================== 枚举与字符串转换 ====================

std::string toString(JoinAlgorithm algo) {
    switch (algo) {
        case JoinAlgorithm::BRUTEFORCE: return "bruteforce";
        case JoinAlgorithm::IVF: return "ivf";
        case JoinAlgorithm::HNSW: return "hnsw";
        case JoinAlgorithm::HDR_TREE: return "hdr_tree";
        case JoinAlgorithm::CLUSTERED_JOIN: return "clustered_join";
        case JoinAlgorithm::S3J: return "s3j";
        case JoinAlgorithm::VSJOIN: return "vsjoin";
        case JoinAlgorithm::FAISS_IVF: return "faiss_ivf";
        case JoinAlgorithm::FAISS_HNSW: return "faiss_hnsw";
        default: return "unknown";
    }
}

std::string toString(PartitionStrategy ps) {
    switch (ps) {
        case PartitionStrategy::ROUND_ROBIN: return "round_robin";
        case PartitionStrategy::KEY_HASH: return "key_hash";
        case PartitionStrategy::VECTOR_HASH: return "vector_hash";
        case PartitionStrategy::LSH: return "lsh";
        case PartitionStrategy::CENTROID: return "centroid";
        default: return "unknown";
    }
}

std::string toString(WindowStateType ws) {
    switch (ws) {
        case WindowStateType::SHARED: return "shared";
        case WindowStateType::PARTITIONED: return "partitioned";
        case WindowStateType::TWO_TIER: return "two_tier";
        case WindowStateType::PARTITIONED_VECTOR: return "partitioned_vector";
        default: return "unknown";
    }
}

std::string toString(IndexStrategy is) {
    switch (is) {
        case IndexStrategy::SHARED: return "shared";
        case IndexStrategy::PARTITIONED: return "partitioned";
        default: return "unknown";
    }
}

JoinAlgorithm parseJoinAlgorithm(const std::string& s) {
    std::string lower = toLower(s);
    if (lower == "bruteforce") return JoinAlgorithm::BRUTEFORCE;
    if (lower == "ivf") return JoinAlgorithm::IVF;
    if (lower == "hnsw") return JoinAlgorithm::HNSW;
    if (lower == "hdr_tree" || lower == "hdrtree") return JoinAlgorithm::HDR_TREE;
    if (lower == "clustered_join" || lower == "clusteredjoin") return JoinAlgorithm::CLUSTERED_JOIN;
    if (lower == "s3j") return JoinAlgorithm::S3J;
    if (lower == "vsjoin") return JoinAlgorithm::VSJOIN;
    if (lower == "faiss_ivf") return JoinAlgorithm::FAISS_IVF;
    if (lower == "faiss_hnsw") return JoinAlgorithm::FAISS_HNSW;
    throw std::runtime_error("Unknown JoinAlgorithm: " + s);
}

PartitionStrategy parsePartitionStrategy(const std::string& s) {
    std::string lower = toLower(s);
    if (lower == "round_robin" || lower == "roundrobin") return PartitionStrategy::ROUND_ROBIN;
    if (lower == "key_hash" || lower == "keyhash" || lower == "key") return PartitionStrategy::KEY_HASH;
    if (lower == "vector_hash" || lower == "vectorhash") return PartitionStrategy::VECTOR_HASH;
    if (lower == "lsh") return PartitionStrategy::LSH;
    if (lower == "centroid" || lower == "kmeans") return PartitionStrategy::CENTROID;
    throw std::runtime_error("Unknown PartitionStrategy: " + s);
}

WindowStateType parseWindowStateType(const std::string& s) {
    std::string lower = toLower(s);
    if (lower == "shared") return WindowStateType::SHARED;
    if (lower == "partitioned") return WindowStateType::PARTITIONED;
    if (lower == "two_tier" || lower == "twotier") return WindowStateType::TWO_TIER;
    if (lower == "partitioned_vector" || lower == "partitionedvector") return WindowStateType::PARTITIONED_VECTOR;
    throw std::runtime_error("Unknown WindowStateType: " + s);
}

IndexStrategy parseIndexStrategy(const std::string& s) {
    std::string lower = toLower(s);
    if (lower == "shared") return IndexStrategy::SHARED;
    if (lower == "partitioned") return IndexStrategy::PARTITIONED;
    throw std::runtime_error("Unknown IndexStrategy: " + s);
}

// ==================== JoinStrategyConfig 方法实现 ====================

std::vector<std::string> JoinStrategyConfig::validate() const {
    std::vector<std::string> errors;
    
    // 规则1: RoundRobin 必须配 SHARED 状态
    if (partition_strategy == PartitionStrategy::ROUND_ROBIN &&
        window_state_type != WindowStateType::SHARED) {
        errors.emplace_back(
            "RoundRobin partition strategy requires SharedWindowState. "
            "Current: " + toString(window_state_type));
    }
    
    // 规则2: VSJoin 必须配 LSH + PARTITIONED_VECTOR
    if (algorithm == JoinAlgorithm::VSJOIN) {
        if (partition_strategy != PartitionStrategy::LSH) {
            errors.emplace_back(
                "VSJoin requires LSH partition strategy. "
                "Current: " + toString(partition_strategy));
        }
        if (window_state_type != WindowStateType::PARTITIONED_VECTOR) {
            errors.emplace_back(
                "VSJoin requires PartitionedVectorState. "
                "Current: " + toString(window_state_type));
        }
        if (index_strategy != IndexStrategy::PARTITIONED) {
            errors.emplace_back(
                "VSJoin requires partitioned index strategy. "
                "Current: " + toString(index_strategy));
        }
    }
    
    // 规则3: S3J 必须配 CENTROID
    if (algorithm == JoinAlgorithm::S3J &&
        partition_strategy != PartitionStrategy::CENTROID) {
        errors.emplace_back(
            "S3J requires Centroid partition strategy. "
            "Current: " + toString(partition_strategy));
    }
    
    // 规则4: ClusteredJoin 必须配 CENTROID + PARTITIONED
    if (algorithm == JoinAlgorithm::CLUSTERED_JOIN) {
        if (partition_strategy != PartitionStrategy::CENTROID) {
            errors.emplace_back(
                "ClusteredJoin requires Centroid partition strategy. "
                "Current: " + toString(partition_strategy));
        }
        if (window_state_type != WindowStateType::PARTITIONED) {
            errors.emplace_back(
                "ClusteredJoin requires PartitionedWindowState. "
                "Current: " + toString(window_state_type));
        }
    }
    
    // 规则5: 参数范围检查
    if (similarity_threshold < 0.0 || similarity_threshold > 1.0) {
        errors.emplace_back("similarity_threshold must be in [0.0, 1.0]");
    }
    
    if (ivf_nprobes > ivf_nlist) {
        errors.emplace_back("ivf_nprobes cannot exceed ivf_nlist");
    }
    
    if (num_partitions <= 0) {
        errors.emplace_back("num_partitions must be positive");
    }
    
    if (dimension <= 0) {
        errors.emplace_back("dimension must be positive");
    }
    
    if (window_size_ms <= 0) {
        errors.emplace_back("window_size_ms must be positive");
    }
    
    if (step_size_ms <= 0 || step_size_ms > window_size_ms) {
        errors.emplace_back("step_size_ms must be positive and <= window_size_ms");
    }
    
    if (hnsw_m <= 0) {
        errors.emplace_back("hnsw_m must be positive");
    }
    
    if (vsjoin_num_hash_functions <= 0 || vsjoin_num_hash_functions > 64) {
        errors.emplace_back("vsjoin_num_hash_functions must be in [1, 64]");
    }
    
    if (vsjoin_boundary_threshold < 0.0 || vsjoin_boundary_threshold > 1.0) {
        errors.emplace_back("vsjoin_boundary_threshold must be in [0.0, 1.0]");
    }
    
    return errors;
}

void JoinStrategyConfig::inferDefaults() {
    switch (algorithm) {
        case JoinAlgorithm::VSJOIN:
            partition_strategy = PartitionStrategy::LSH;
            window_state_type = WindowStateType::PARTITIONED_VECTOR;
            index_strategy = IndexStrategy::PARTITIONED;
            break;
            
        case JoinAlgorithm::S3J:
            partition_strategy = PartitionStrategy::CENTROID;
            window_state_type = WindowStateType::PARTITIONED;
            index_strategy = IndexStrategy::PARTITIONED;
            if (num_partitions <= 0) {
                num_partitions = s3j_num_centroids;
            }
            break;
            
        case JoinAlgorithm::CLUSTERED_JOIN:
            partition_strategy = PartitionStrategy::CENTROID;
            window_state_type = WindowStateType::PARTITIONED;
            index_strategy = IndexStrategy::PARTITIONED;
            break;
            
        case JoinAlgorithm::HDR_TREE:
            // HDR-Tree 可使用共享索引或分区索引
            if (partition_strategy == PartitionStrategy::ROUND_ROBIN) {
                window_state_type = WindowStateType::SHARED;
                index_strategy = IndexStrategy::SHARED;
            } else {
                window_state_type = WindowStateType::PARTITIONED;
                index_strategy = IndexStrategy::PARTITIONED;
            }
            break;
            
        case JoinAlgorithm::BRUTEFORCE:
        case JoinAlgorithm::IVF:
        case JoinAlgorithm::HNSW:
        default:
            // 默认使用共享索引策略
            partition_strategy = PartitionStrategy::ROUND_ROBIN;
            window_state_type = WindowStateType::SHARED;
            index_strategy = IndexStrategy::SHARED;
            break;
    }
}

std::string JoinStrategyConfig::summary() const {
    std::ostringstream oss;
    oss << "JoinStrategyConfig {\n"
        << "  algorithm: " << toString(algorithm) 
        << (is_eager ? " (eager)" : " (lazy)") << "\n"
        << "  partition: " << toString(partition_strategy) 
        << " (" << num_partitions << " partitions)\n"
        << "  window_state: " << toString(window_state_type) << "\n"
        << "  index: " << toString(index_strategy) << "\n"
        << "  similarity_threshold: " << similarity_threshold << "\n"
        << "  dimension: " << dimension << "\n"
        << "  window: " << window_size_ms << "ms (step: " << step_size_ms << "ms)\n"
        << "}";
    return oss.str();
}

// ==================== 配置加载 ====================

static void loadFromTomlNode(JoinStrategyConfig& config, const toml::table& node) {
    // 基础配置
    if (auto algo = node["algorithm"].value<std::string>()) {
        config.algorithm = parseJoinAlgorithm(*algo);
    }
    if (auto eager = node["is_eager"].value<bool>()) {
        config.is_eager = *eager;
    }
    if (auto threshold = node["similarity_threshold"].value<double>()) {
        config.similarity_threshold = *threshold;
    }
    if (auto dim = node["dimension"].value<int64_t>()) {
        config.dimension = static_cast<int>(*dim);
    }
    
    // 分区配置
    if (auto ps = node["partition_strategy"].value<std::string>()) {
        config.partition_strategy = parsePartitionStrategy(*ps);
    }
    if (auto np = node["num_partitions"].value<int64_t>()) {
        config.num_partitions = static_cast<int>(*np);
    }
    
    // 窗口状态配置
    if (auto ws = node["window_state_type"].value<std::string>()) {
        config.window_state_type = parseWindowStateType(*ws);
    }
    if (auto wsize = node["window_size_ms"].value<int64_t>()) {
        config.window_size_ms = *wsize;
    }
    if (auto ssize = node["step_size_ms"].value<int64_t>()) {
        config.step_size_ms = *ssize;
    }
    
    // 索引配置
    if (auto is = node["index_strategy"].value<std::string>()) {
        config.index_strategy = parseIndexStrategy(*is);
    }
    
    // IVF 参数
    if (auto nlist = node["ivf_nlist"].value<int64_t>()) {
        config.ivf_nlist = static_cast<int>(*nlist);
    }
    if (auto nprobes = node["ivf_nprobes"].value<int64_t>()) {
        config.ivf_nprobes = static_cast<int>(*nprobes);
    }
    if (auto rebuild = node["ivf_rebuild_threshold"].value<double>()) {
        config.ivf_rebuild_threshold = *rebuild;
    }
    
    // HNSW 参数
    if (auto m = node["hnsw_m"].value<int64_t>()) {
        config.hnsw_m = static_cast<int>(*m);
    }
    if (auto efc = node["hnsw_ef_construction"].value<int64_t>()) {
        config.hnsw_ef_construction = static_cast<int>(*efc);
    }
    if (auto efs = node["hnsw_ef_search"].value<int64_t>()) {
        config.hnsw_ef_search = static_cast<int>(*efs);
    }
    
    // VSJoin 参数
    if (auto nhash = node["vsjoin_num_hash_functions"].value<int64_t>()) {
        config.vsjoin_num_hash_functions = static_cast<int>(*nhash);
    }
    if (auto bt = node["vsjoin_boundary_threshold"].value<double>()) {
        config.vsjoin_boundary_threshold = *bt;
    }
    if (auto at = node["vsjoin_async_threads"].value<int64_t>()) {
        config.vsjoin_async_threads = static_cast<int>(*at);
    }
    if (auto al = node["vsjoin_allowed_lateness"].value<int64_t>()) {
        config.vsjoin_allowed_lateness = *al;
    }
    
    // S3J 参数
    if (auto nc = node["s3j_num_centroids"].value<int64_t>()) {
        config.s3j_num_centroids = static_cast<int>(*nc);
    }
    if (auto ai = node["s3j_adapt_interval_ms"].value<int64_t>()) {
        config.s3j_adapt_interval_ms = *ai;
    }
    if (auto lt = node["s3j_load_threshold"].value<double>()) {
        config.s3j_load_threshold = *lt;
    }
    if (auto ea = node["s3j_enable_adaptive"].value<bool>()) {
        config.s3j_enable_adaptive = *ea;
    }
    
    // ClusteredJoin 参数
    if (auto or_ = node["clustered_overlap_ratio"].value<double>()) {
        config.clustered_overlap_ratio = *or_;
    }
    if (auto rt = node["clustered_rebalance_threshold"].value<double>()) {
        config.clustered_rebalance_threshold = *rt;
    }
    if (auto br = node["clustered_border_replication"].value<bool>()) {
        config.clustered_border_replication = *br;
    }
    if (auto ts = node["clustered_training_samples"].value<int64_t>()) {
        config.clustered_training_samples = static_cast<int>(*ts);
    }
    
    // HDR-Tree 参数
    if (auto pd = node["hdr_projected_dim"].value<int64_t>()) {
        config.hdr_projected_dim = static_cast<int>(*pd);
    }
    if (auto mns = node["hdr_max_node_size"].value<int64_t>()) {
        config.hdr_max_node_size = static_cast<int>(*mns);
    }
    if (auto dbs = node["hdr_delta_buffer_size"].value<int64_t>()) {
        config.hdr_delta_buffer_size = static_cast<size_t>(*dbs);
    }
    if (auto pss = node["hdr_pca_sample_size"].value<int64_t>()) {
        config.hdr_pca_sample_size = static_cast<int>(*pss);
    }
    
    // 双层窗口参数
    if (auto ct = node["two_tier_compact_threshold"].value<int64_t>()) {
        config.two_tier_compact_threshold = static_cast<size_t>(*ct);
    }
    if (auto ebt = node["two_tier_enable_boundary_tracking"].value<bool>()) {
        config.two_tier_enable_boundary_tracking = *ebt;
    }

    // FAISS 参数
    if (auto fdo = node["faiss_disable_omp"].value<bool>()) {
        config.faiss_disable_omp = *fdo;
    }
}

JoinStrategyConfig loadJoinStrategyConfig(const std::string& config_path) {
    JoinStrategyConfig config;
    
    try {
        auto tbl = toml::parse_file(config_path);
        
        // 先加载默认配置（如果存在）
        if (auto defaults = tbl["default"].as_table()) {
            loadFromTomlNode(config, *defaults);
        }
        
        // 加载根级别配置（覆盖默认）
        loadFromTomlNode(config, tbl);
        
    } catch (const toml::parse_error& e) {
        throw std::runtime_error("Failed to parse TOML config: " + std::string(e.what()));
    }
    
    return config;
}

JoinStrategyConfig loadJoinStrategyConfig(const std::string& config_path,
                                           const std::string& strategy_name) {
    JoinStrategyConfig config;
    
    try {
        auto tbl = toml::parse_file(config_path);
        
        // 先加载默认配置
        if (auto defaults = tbl["default"].as_table()) {
            loadFromTomlNode(config, *defaults);
        }
        
        // 查找并加载指定策略
        if (auto strategies = tbl["strategies"].as_table()) {
            if (auto strategy = strategies->get(strategy_name)) {
                if (auto strategy_tbl = strategy->as_table()) {
                    loadFromTomlNode(config, *strategy_tbl);
                } else {
                    throw std::runtime_error("Strategy '" + strategy_name + 
                                           "' is not a valid table");
                }
            } else {
                throw std::runtime_error("Strategy '" + strategy_name + 
                                       "' not found in config file");
            }
        } else {
            throw std::runtime_error("No 'strategies' section in config file");
        }
        
    } catch (const toml::parse_error& e) {
        throw std::runtime_error("Failed to parse TOML config: " + std::string(e.what()));
    }
    
    return config;
}

}  // namespace sageFlow

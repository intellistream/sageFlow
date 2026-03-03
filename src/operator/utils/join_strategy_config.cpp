#include "operator/utils/join_strategy_config.h"
#include "utils/logger.h"

#include <algorithm>
#include <cctype>
#include <cmath>
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
        case JoinAlgorithm::LSH: return "lsh";
        case JoinAlgorithm::CLUSTERED_JOIN: return "clustered_join";
        case JoinAlgorithm::S3J: return "s3j";
        case JoinAlgorithm::VSJOIN: return "vsjoin";
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
    if (lower == "lsh") return JoinAlgorithm::LSH;
    if (lower == "clustered_join" || lower == "clusteredjoin") return JoinAlgorithm::CLUSTERED_JOIN;
    if (lower == "s3j") return JoinAlgorithm::S3J;
    if (lower == "vsjoin") return JoinAlgorithm::VSJOIN;
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

// ==================== ClusteredIndexType 转换 ====================

std::string toString(ClusteredIndexType cit) {
    switch (cit) {
        case ClusteredIndexType::BRUTEFORCE: return "bruteforce";
        case ClusteredIndexType::IVF: return "ivf";
        case ClusteredIndexType::HNSW: return "hnsw";
        default: return "unknown";
    }
}

ClusteredIndexType parseClusteredIndexType(const std::string& s) {
    std::string lower = toLower(s);
    
    if (lower == "bruteforce" || lower == "brute_force") {
        return ClusteredIndexType::BRUTEFORCE;
    }
    if (lower == "ivf") {
        return ClusteredIndexType::IVF;
    }
    if (lower == "hnsw") {
        return ClusteredIndexType::HNSW;
    }
    
    // 默认返回 IVF，并记录警告
    SAGEFLOW_LOG_WARN("Config", "Unknown clustered_index_type '{}', defaulting to IVF", s);
    return ClusteredIndexType::IVF;
}

// ==================== SimilarityMode 转换 ====================

std::string toString(SimilarityMode sm) {
    switch (sm) {
        case SimilarityMode::FIXED_ALPHA: return "fixed_alpha";
        case SimilarityMode::ADAPTIVE_ALPHA: return "adaptive_alpha";
        case SimilarityMode::NORMALIZED: return "normalized";
        default: return "unknown";
    }
}

SimilarityMode parseSimilarityMode(const std::string& s) {
    std::string lower = toLower(s);
    
    if (lower == "fixed_alpha" || lower == "fixed" || lower == "fixedalpha") {
        return SimilarityMode::FIXED_ALPHA;
    }
    if (lower == "adaptive_alpha" || lower == "adaptive" || lower == "adaptivealpha" || lower == "auto") {
        return SimilarityMode::ADAPTIVE_ALPHA;
    }
    if (lower == "normalized" || lower == "normalize" || lower == "norm") {
        return SimilarityMode::NORMALIZED;
    }
    
    // 默认返回 FIXED_ALPHA
    SAGEFLOW_LOG_WARN("Config", "Unknown similarity_mode '{}', defaulting to fixed_alpha", s);
    return SimilarityMode::FIXED_ALPHA;
}

// ==================== VSJoinIndexType 转换 ====================

std::string toString(VSJoinIndexType vit) {
    switch (vit) {
        case VSJoinIndexType::BRUTEFORCE: return "bruteforce";
        case VSJoinIndexType::IVF: return "ivf";
        case VSJoinIndexType::HNSW: return "hnsw";
        default: return "unknown";
    }
}

VSJoinIndexType parseVSJoinIndexType(const std::string& s) {
    std::string lower = toLower(s);
    
    if (lower == "bruteforce" || lower == "brute_force") {
        return VSJoinIndexType::BRUTEFORCE;
    }
    if (lower == "ivf") {
        return VSJoinIndexType::IVF;
    }
    if (lower == "hnsw") {
        return VSJoinIndexType::HNSW;
    }
    
    // 默认返回 BRUTEFORCE（推荐用于 Local Index）
    SAGEFLOW_LOG_WARN("Config", "Unknown VSJoinIndexType '{}', defaulting to bruteforce", s);
    return VSJoinIndexType::BRUTEFORCE;
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
    
    // 规则2: VSJoin 需要 LSH 分区 + 分区窗口状态（PARTITIONED/TWO_TIER/PARTITIONED_VECTOR）
    if (algorithm == JoinAlgorithm::VSJOIN) {
        if (partition_strategy != PartitionStrategy::LSH) {
            errors.emplace_back(
                "VSJoin requires LSH partition strategy. "
                "Current: " + toString(partition_strategy));
        }
        // 新版设计：支持 PARTITIONED（推荐）、TWO_TIER、PARTITIONED_VECTOR（旧版兼容）
        if (window_state_type != WindowStateType::PARTITIONED &&
            window_state_type != WindowStateType::TWO_TIER &&
            window_state_type != WindowStateType::PARTITIONED_VECTOR) {
            errors.emplace_back(
                "VSJoin requires PARTITIONED, TWO_TIER, or PARTITIONED_VECTOR window state. "
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

    if (lsh_num_tables <= 0 || lsh_num_tables > 64) {
        errors.emplace_back("lsh_num_tables must be in (0, 64]");
    }

    if (lsh_num_hashes <= 0 || lsh_num_hashes > 256) {
        errors.emplace_back("lsh_num_hashes must be in (0, 256]");
    }
    
    return errors;
}

void JoinStrategyConfig::inferDefaults() {
    switch (algorithm) {
        case JoinAlgorithm::IVF: {
            // IVF 默认走共享索引（RoundRobin + SharedWindowState）
            partition_strategy = PartitionStrategy::ROUND_ROBIN;
            window_state_type = WindowStateType::SHARED;
            index_strategy = IndexStrategy::SHARED;

            // 关键：如果用户没有显式配置 IVF 参数（仍为默认 100/10），则根据窗口大小动态推断。
            // 这与 JoinOperator 旧构造路径中的“基于 window_size/step_size 估计数据量”策略保持一致，
            // 可避免在不同窗口配置下手动调 nprobes/nlist。
            //
            // 注意：只有在参数保持默认值时才覆盖，避免破坏用户在 TOML 中显式指定的配置。
            if (ivf_nlist == 100 && ivf_nprobes == 10) {
                const int64_t window_size = window_size_ms;
                const int64_t step_size = step_size_ms;
                const int64_t vector_count =
                    (step_size > 0) ? (window_size / step_size) : window_size;

                // nlist ~ 4 * sqrt(N)（N 为窗口内估计向量数）
                int nlist = std::max(1, static_cast<int>(4.0 * std::sqrt(static_cast<double>(std::max<int64_t>(1, vector_count)))));
                // nprobes ~ 30% nlist，偏向召回（流式 Join 更看重召回）
                int nprobes = std::max(3, nlist * 30 / 100);
                nprobes = std::min(nprobes, nlist);

                ivf_nlist = nlist;
                ivf_nprobes = nprobes;

                SAGEFLOW_LOG_INFO("Config",
                    "IVF defaults inferred from window: window={}ms step={}ms N≈{} -> nlist={} nprobes={}",
                    window_size, step_size, vector_count, ivf_nlist, ivf_nprobes);
            }
            break;
        }

        case JoinAlgorithm::VSJOIN:
            // VSJoin 使用 LSH 分区 + 分区窗口状态（推荐 PARTITIONED）
            partition_strategy = PartitionStrategy::LSH;
            window_state_type = WindowStateType::PARTITIONED;
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

        case JoinAlgorithm::LSH:
            // LSH 使用基于哈希的分区与分区窗口，保证相似向量落同一分区
            partition_strategy = PartitionStrategy::LSH;
            window_state_type = WindowStateType::PARTITIONED;
            index_strategy = IndexStrategy::PARTITIONED;  // LSH 不依赖外部索引，用分区模式
            break;
            
        case JoinAlgorithm::BRUTEFORCE:
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
        << "  window: " << window_size_ms << "ms (step: " << step_size_ms << "ms)\n";
    
    // ClusteredJoin 特定参数
    if (algorithm == JoinAlgorithm::CLUSTERED_JOIN) {
        oss << "  -- ClusteredJoin --\n"
            << "  clustered_index_type: " << toString(clustered_index_type) << "\n"
            << "  clustered_multicast_k: " << clustered_multicast_k << "\n"
            << "  clustered_overlap_ratio: " << clustered_overlap_ratio << "\n"
            << "  clustered_multicast_enabled: " << clustered_multicast_enabled << "\n"
            << "  clustered_training_samples: " << clustered_training_samples << "\n";
    }
    
    oss << "}";
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
    
    // 相似度计算配置
    if (auto mode = node["similarity_mode"].value<std::string>()) {
        config.similarity_mode = parseSimilarityMode(*mode);
    }
    if (auto alpha = node["similarity_alpha"].value<double>()) {
        config.similarity_alpha = *alpha;
    }
    // 兼容旧配置：支持 "alpha" 作为 "similarity_alpha" 的别名
    if (auto alpha = node["alpha"].value<double>()) {
        config.similarity_alpha = *alpha;
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

    // LSH 参数
    if (auto ltables = node["lsh_num_tables"].value<int64_t>()) {
        config.lsh_num_tables = static_cast<int>(*ltables);
    }
    if (auto lhashes = node["lsh_num_hashes"].value<int64_t>()) {
        config.lsh_num_hashes = static_cast<int>(*lhashes);
    }
    if (auto lseed = node["lsh_seed"].value<int64_t>()) {
        config.lsh_seed = static_cast<uint32_t>(*lseed);
    }
    
    // VSJoin 参数
    if (auto nhash = node["vsjoin_num_hash_functions"].value<int64_t>()) {
        config.vsjoin_num_hash_functions = static_cast<int>(*nhash);
    }
    if (auto bt = node["vsjoin_boundary_threshold"].value<double>()) {
        config.vsjoin_boundary_threshold = *bt;
    }
    if (auto mk = node["vsjoin_multicast_k"].value<int64_t>()) {
        config.vsjoin_multicast_k = static_cast<int>(*mk);
    }
    if (auto ri = node["vsjoin_rebuild_interval_ms"].value<int64_t>()) {
        config.vsjoin_rebuild_interval_ms = *ri;
    }
    if (auto rt = node["vsjoin_rebuild_threshold"].value<int64_t>()) {
        config.vsjoin_rebuild_threshold = static_cast<size_t>(*rt);
    }
    // VSJoin Local/Global Index 类型
    if (auto lit = node["vsjoin_local_index_type"].value<std::string>()) {
        config.vsjoin_local_index_type = parseVSJoinIndexType(*lit);
    }
    if (auto git = node["vsjoin_global_index_type"].value<std::string>()) {
        config.vsjoin_global_index_type = parseVSJoinIndexType(*git);
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
    if (auto mk = node["clustered_multicast_k"].value<int64_t>()) {
        config.clustered_multicast_k = static_cast<int>(*mk);
    }
    if (auto or_ = node["clustered_overlap_ratio"].value<double>()) {
        config.clustered_overlap_ratio = *or_;
    }
    if (auto rt = node["clustered_rebalance_threshold"].value<double>()) {
        config.clustered_rebalance_threshold = *rt;
    }
    if (auto ts = node["clustered_training_samples"].value<int64_t>()) {
        config.clustered_training_samples = static_cast<int>(*ts);
        // 同步到通用 training_samples 字段
        config.training_samples = static_cast<size_t>(*ts);
    }
    // 新增：分区内索引类型
    if (auto cit = node["clustered_index_type"].value<std::string>()) {
        config.clustered_index_type = parseClusteredIndexType(*cit);
    }
    // 新增：是否启用多播
    if (auto cme = node["clustered_multicast_enabled"].value<bool>()) {
        config.clustered_multicast_enabled = *cme;
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

// ==================== 从字符串方法名创建配置 ====================

JoinStrategyConfig createJoinStrategyConfigFromMethodName(
    const std::string& method_name,
    double similarity_threshold,
    int dimension,
    int64_t window_size_ms,
    int64_t step_size_ms) {
    JoinStrategyConfig config;
    
    // 提取算法名称（移除 "_eager"/"_lazy" 后缀）
    std::string algo = toLower(method_name);
    if (algo.rfind("_eager") != std::string::npos) {
        algo = algo.substr(0, algo.rfind("_eager"));
    } else if (algo.rfind("_lazy") != std::string::npos) {
        algo = algo.substr(0, algo.rfind("_lazy"));
    }
    
    // 解析算法类型
    config.algorithm = parseJoinAlgorithm(algo);
    config.similarity_threshold = similarity_threshold;
    config.dimension = dimension;
    config.window_size_ms = window_size_ms;
    config.step_size_ms = step_size_ms;
    config.is_eager = true;  // 所有方法使用 Eager 模式
    
    // 根据算法类型设置默认参数（与旧构造函数逻辑保持一致）
    switch (config.algorithm) {
        case JoinAlgorithm::IVF:
            // IVF 默认使用共享状态
            config.window_state_type = WindowStateType::SHARED;
            config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
            config.index_strategy = IndexStrategy::SHARED;
            config.ivf_rebuild_threshold = 2.0;  // 与原来构造函数中的默认值保持一致
            // IVF 的 nlist 和 nprobes 会在 initializeWithStrategyConfig 中根据窗口大小动态计算
            break;
            
        case JoinAlgorithm::BRUTEFORCE:
            // BruteForce 使用共享状态
            config.window_state_type = WindowStateType::SHARED;
            config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
            config.index_strategy = IndexStrategy::SHARED;
            break;
            
        case JoinAlgorithm::HNSW:
            // HNSW 使用共享状态
            config.window_state_type = WindowStateType::SHARED;
            config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
            config.index_strategy = IndexStrategy::SHARED;
            config.hnsw_m = 16;
            config.hnsw_ef_construction = 200;
            config.hnsw_ef_search = 100;
            break;
            
        case JoinAlgorithm::HDR_TREE:
            // HDRTree 推荐使用共享状态
            config.window_state_type = WindowStateType::SHARED;
            config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
            config.index_strategy = IndexStrategy::SHARED;
            config.hdr_projected_dim = 16;
            config.hdr_pca_sample_size = 100;
            config.hdr_max_node_size = 100;  // 与原来构造函数中的默认值保持一致
            config.hdr_delta_buffer_size = 1000;  // 默认值
            break;
            
        case JoinAlgorithm::LSH:
            // LSH 使用分区状态
            config.window_state_type = WindowStateType::PARTITIONED;
            config.partition_strategy = PartitionStrategy::LSH;
            config.index_strategy = IndexStrategy::PARTITIONED;
            config.lsh_num_tables = 4;
            config.lsh_num_hashes = 8;
            config.lsh_seed = 42;
            break;
            
        case JoinAlgorithm::CLUSTERED_JOIN:
            // ClusteredJoin 必须使用分区状态和质心分区
            config.window_state_type = WindowStateType::PARTITIONED;
            config.partition_strategy = PartitionStrategy::CENTROID;
            config.index_strategy = IndexStrategy::PARTITIONED;
            config.num_partitions = 8;  // 默认值，运行时会被 parallelism 覆盖
            config.clustered_overlap_ratio = 0.1;
            config.clustered_rebalance_threshold = 0.3;
            config.clustered_multicast_enabled = true;
            config.clustered_index_type = ClusteredIndexType::BRUTEFORCE;
            break;
            
        default:
            // 其他算法使用默认值
            config.inferDefaults();
            break;
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

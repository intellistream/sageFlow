#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>


#include "common/data_types.h"

namespace sageFlow {

/**
 * @brief Join 算法类型枚举
 */
enum class JoinAlgorithm {
    BRUTEFORCE,      ///< Ground Truth baseline
    IVF,             ///< IVF-based approximate join
    HNSW,            ///< HNSW-based approximate join
    HDR_TREE,        ///< HDR-Tree baseline
    LSH,             ///< Hyperplane LSH 近似 Join（移植 Danny baseline）
    CLUSTERED_JOIN,  ///< VectraFlow ClusteredJoin
    S3J,             ///< DEBS'23 S3J baseline
    VSJOIN           ///< Our method
};

/**
 * @brief 分区策略类型枚举
 */
enum class PartitionStrategy {
    ROUND_ROBIN,     ///< 轮询分发（需要 SharedWindowState）
    KEY_HASH,        ///< 基于 key 的哈希分区
    VECTOR_HASH,     ///< 基于向量内容的哈希分区
    LSH,             ///< 局部敏感哈希分区（VSJoin）
    CENTROID         ///< 基于质心的分区（S3J/ClusteredJoin）
};

/**
 * @brief 窗口状态类型枚举
 */
enum class WindowStateType {
    SHARED,              ///< SharedWindowState（所有实例共享）
    PARTITIONED,         ///< PartitionedWindowState（每个 subtask 独立）
    TWO_TIER,            ///< TwoTierWindowState（写友好层+紧凑层,暂未正确实现）
    PARTITIONED_VECTOR   ///< PartitionedVectorState（向量空间分区,暂未正确实现）
};

/**
 * @brief 索引策略类型枚举
 */
enum class IndexStrategy {
    SHARED,           ///< 共享索引（所有实例使用同一索引）
    PARTITIONED       ///< 分区索引（每个分区独立索引）
};

/**
 * @brief Clustered Join 分区内索引类型
 * 
 * 控制 ClusteredJoin 在每个分区内使用的索引策略。
 */
enum class ClusteredIndexType {
    BRUTEFORCE,  ///< 暴力扫描（Ground Truth，用于验证）
    IVF,         ///< IVF 索引（默认，推荐用于生产）
    HNSW         ///< HNSW 索引（可选）
};

/**
 * @brief VSJoin 索引类型枚举
 * 
 * 控制 VSJoin 的 Local/Global 索引类型。
 * 注意：与 index/index.h 中的 IndexType 类似，但为了避免循环依赖独立定义。
 */
enum class VSJoinIndexType {
    BRUTEFORCE,  ///< 暴力扫描（Local Index 推荐，轻量级）
    IVF,         ///< IVF 索引（Global Index 推荐，快速查询）
    HNSW         ///< HNSW 索引（备选）
};

/**
 * @brief VSJoin routing mode for Mechanism II (Budgeted Boundary Coverage)
 */
enum class VSJoinRouteMode {
    UNICAST,    ///< Route to single best partition only
    BUDGETED,   ///< Route to up to fanout_budget partitions (deterministic top-k by distance)
    BROADCAST   ///< Route to all partitions
};

/**
 * @brief VSJoin snapshot validity filtering policy for Mechanism I
 */
enum class VSJoinSnapshotFilterPolicy {
    WINDOW_ONLY,     ///< Only filter by window lower bound (default, current behavior)
    MAX_STALENESS,   ///< Also enforce max staleness guardrail
    AGGRESSIVE       ///< Window + staleness + discard if rebuild too old
};

/**
 * @brief 相似度计算模式
 * 
 * 控制 exp(-alpha * L2_distance) 中 alpha 的计算方式
 */
enum class SimilarityMode {
    FIXED_ALPHA,      ///< 使用固定的 alpha 值（默认 0.1）
    ADAPTIVE_ALPHA,   ///< 根据数据分布自动计算 alpha
    NORMALIZED        ///< 先归一化向量，再使用固定 alpha
};

/**
 * @brief Join 策略完整配置
 * 
 * 包含所有可配置的 Join 参数，支持从 TOML 配置文件加载。
 */
struct JoinStrategyConfig {
    // ==================== 基础配置 ====================
    JoinAlgorithm algorithm = JoinAlgorithm::BRUTEFORCE;
    bool is_eager = false;  ///< true=Eager模式, false=Lazy模式
    double similarity_threshold = 0.8;
    int dimension = 128;  ///< 向量维度
    
    // ==================== 相似度计算配置 ====================
    /**
     * @brief 相似度计算模式
     * 
     * - FIXED_ALPHA: 使用固定的 similarity_alpha 值
     * - ADAPTIVE_ALPHA: 根据数据分布自动计算 alpha（使 median 距离 → sim=0.5）
     * - NORMALIZED: 先归一化向量（L2范数=1），再使用固定 alpha
     */
    SimilarityMode similarity_mode = SimilarityMode::FIXED_ALPHA;
    
    /**
     * @brief 相似度计算的 alpha 参数
     * 
     * 相似度公式: sim = exp(-alpha * L2_distance)
     * 
     * 推荐值:
     * - 归一化向量 (范数≈1): alpha = 0.1 ~ 1.0
     * - 原始 SIFT 向量 (范数≈500): alpha = 0.001 ~ 0.002
     * - 自适应模式: 此值作为初始值，会根据数据自动调整
     * 
     * 选择原则: 使 "典型相似对" 的相似度落在 [0.5, 0.9] 范围内
     */
    double similarity_alpha = 0.1;
    
    // ==================== 分区配置 ====================
    PartitionStrategy partition_strategy = PartitionStrategy::ROUND_ROBIN;
    int num_partitions = 4;  ///< 向量空间分区数（用于 LSH/CENTROID）
    
    // ==================== 窗口状态配置 ====================
    WindowStateType window_state_type = WindowStateType::SHARED;
    int64_t window_size_ms = 10000;  ///< 窗口大小（毫秒）
    int64_t step_size_ms = 1000;     ///< 滑动步长（毫秒）
    int64_t time_interval_ms = 10;   ///< 向量到达间隔（毫秒），用于估算窗口内向量数
    
    // ==================== 索引配置 ====================
    IndexStrategy index_strategy = IndexStrategy::SHARED;
    
    // ==================== IVF 参数 ====================
    int ivf_nlist = 100;           ///< IVF 聚类数量
    int ivf_nprobes = 10;          ///< IVF 搜索时探测的聚类数
    double ivf_rebuild_threshold = 2.0;  ///< 触发重建的阈值（与字符串路径一致）
    
    // ==================== HNSW 参数 ====================
    int hnsw_m = 16;                   ///< 每层最大邻居数
    int hnsw_ef_construction = 200;    ///< 构建时候选集大小
    int hnsw_ef_search = 50;           ///< 搜索时候选集大小

    // ==================== LSH 参数 ====================
    int lsh_num_tables = 4;            ///< LSH 表数量（重复次数）
    int lsh_num_hashes = 8;            ///< 每张表的超平面数量
    uint32_t lsh_seed = 42;            ///< 随机种子，确保可复现
    
    // ==================== VSJoin 参数 ====================
    int vsjoin_multicast_k = 2;               ///< 边界向量多播到 k 个分区（推荐 2-3）
    int64_t vsjoin_rebuild_interval_ms = 2000;  ///< Global Index 重建间隔
    size_t vsjoin_rebuild_threshold = 1000;     ///< 触发重建的阈值
    double vsjoin_rebalance_imbalance_ratio = 1.35;  ///< 触发重平衡的负载失衡比（max/avg）
    size_t vsjoin_rebalance_max_moves = 8;           ///< 每轮最多迁移的 logical partition 数

    /**
     * @brief VSJoin Local Index 类型
     * 
     * Local Index 用于分区内的近邻查询，推荐使用 BruteForce（轻量级）。
     */
    VSJoinIndexType vsjoin_local_index_type = VSJoinIndexType::BRUTEFORCE;
    
    /**
     * @brief VSJoin Global Index 类型
     * 
     * Global Index 用于跨分区的候选召回，推荐使用 IVF（快速查询）。
     */
    VSJoinIndexType vsjoin_global_index_type = VSJoinIndexType::IVF;

    // ---- Mechanism I: Bounded-Staleness Read/Write Decoupling ----
    VSJoinSnapshotFilterPolicy vsjoin_snapshot_filter_policy = VSJoinSnapshotFilterPolicy::WINDOW_ONLY;
    int64_t vsjoin_max_staleness_ms = 0;  ///< Max staleness guardrail (0 = disabled)

    // ---- Mechanism II: Budgeted Boundary Coverage Routing ----
    VSJoinRouteMode vsjoin_route_mode = VSJoinRouteMode::BUDGETED;  ///< Routing mode
    int vsjoin_fanout_budget = 2;  ///< Max partitions per probe in BUDGETED mode

    // ---- Mechanism III: Predictable Skew Control Plane ----
    int64_t vsjoin_rebalance_cooldown_ms = 10000;  ///< Cooldown between rebalance rounds
    bool vsjoin_use_smoothed_load = true;           ///< Use EWMA-smoothed load for rebalance decisions

    // LSH 分区器参数
    int vsjoin_num_hash_functions = 8;         ///< LSH 哈希函数数量
    double vsjoin_boundary_threshold = 0.1;    ///< 边界向量阈值
    
    // ==================== S3J 参数 ====================
    int s3j_num_centroids = 16;          ///< S3J 质心数量
    int64_t s3j_adapt_interval_ms = 1000; ///< 自适应调整间隔
    double s3j_load_threshold = 0.3;      ///< 负载不均衡阈值
    bool s3j_enable_adaptive = true;      ///< 是否启用自适应调整
    
    // ==================== ClusteredJoin 参数 ====================
    /**
     * @brief 多播到最近的 k 个分区
     * 
     * - k = 0: 使用 overlap_ratio 阈值判定（当前行为）
     * - k = 1: 仅主分区（等同于单播）
     * - k >= 2: 固定多播到最近的 k 个分区
     */
    int clustered_multicast_k = 0;               ///< 多播到最近的 k 个分区 (0=使用overlap_ratio)
    double clustered_overlap_ratio = 0.1;        ///< 边界重叠比例（当 multicast_k=0 时使用）
    double clustered_rebalance_threshold = 0.3;  ///< 触发重平衡的阈值（未使用，保留供未来扩展）
    int clustered_training_samples = 1000;       ///< 训练样本数
    
    /**
     * @brief 分区内索引类型
     * 
     * - BRUTEFORCE: 分区内全量扫描，100% 召回但较慢
     * - IVF: 分区内 IVF 索引，平衡速度和召回
     * - HNSW: 分区内 HNSW 索引，适合稀疏查询
     */
    ClusteredIndexType clustered_index_type = ClusteredIndexType::IVF;
    
    /**
     * @brief 是否启用多播分区（边界向量复制）
     * 
     * 当为 true 时，边界向量会被复制到多个分区以保证召回率。
     * 需要配合 CentroidPartitioner::setMulticastEnabled() 使用。
     */
    bool clustered_multicast_enabled = true;
    
    // ==================== 冷启动训练参数 ====================
    /**
     * @brief 是否启用冷启动模式
     * 
     * 当为 true 时，在 CentroidPartitioner 训练完成前，使用广播模式。
     * 训练完成后自动切换到多播模式。
     */
    bool enable_cold_start = true;
    
    /**
     * @brief 训练样本数阈值
     * 
     * 当收集的样本数达到此阈值时，触发 CentroidPartitioner 训练。
     * 注意：此参数主要用于配置默认值，实际使用时会同步到 CentroidPartitioner::Config。
     */
    size_t training_samples = 1000;
    
    /**
     * @brief 广播阶段是否去重
     * 
     * 当为 true 时，在广播阶段使用 Owner-Computes 策略：
     * - 所有 subtask 都收到相同数据并更新状态
     * - 只有 (uid % parallelism == subtask_index) 的 subtask 产生输出
     * 这避免了重复的 Join 结果输出。
     */
    bool deduplicate_during_broadcast = true;
    
    // ==================== HDR-Tree 参数 ====================
    int hdr_projected_dim = 8;           ///< PCA 降维目标维度
    int hdr_max_node_size = 100;         ///< 节点最大大小
    size_t hdr_delta_buffer_size = 1000; ///< Delta 缓冲区大小
    int hdr_pca_sample_size = 10000;     ///< PCA 训练样本数
    
    // ==================== 双层窗口参数 ====================
    size_t two_tier_compact_threshold = 100;  ///< 双层窗口压缩阈值
    bool two_tier_enable_boundary_tracking = true;  ///< 是否启用边界追踪
    
    /**
     * @brief 验证配置的一致性
     * @return 错误信息列表，空表示验证通过
     */
    [[nodiscard]] std::vector<std::string> validate() const;
    
    /**
     * @brief 推断默认的分区和窗口策略
     * 根据算法类型自动设置合适的策略
     */
    void inferDefaults();
    
    /**
     * @brief 获取配置摘要字符串
     * @return 配置摘要
     */
    [[nodiscard]] std::string summary() const;
};

// ==================== 配置加载函数 ====================

/**
 * @brief 从 TOML 配置文件加载 JoinStrategyConfig
 * @param config_path 配置文件路径
 * @return JoinStrategyConfig 配置对象
 * @throws std::runtime_error 如果加载失败
 */
JoinStrategyConfig loadJoinStrategyConfig(const std::string& config_path);

/**
 * @brief 从 TOML 配置文件加载指定策略的配置
 * @param config_path 配置文件路径
 * @param strategy_name 策略名称（在 [strategies.xxx] 下定义）
 * @return JoinStrategyConfig 配置对象
 * @throws std::runtime_error 如果加载失败或策略不存在
 */
JoinStrategyConfig loadJoinStrategyConfig(const std::string& config_path,
                                           const std::string& strategy_name);

/**
 * @brief 从字符串方法名和阈值创建 JoinStrategyConfig（用于统一初始化路径）
 * 
 * 将旧的字符串方法名（如 "bruteforce", "ivf", "clustered_join"）转换为 JoinStrategyConfig，
 * 并设置合理的默认值。支持移除 "_eager"/"_lazy" 后缀。
 * 
 * @param method_name 方法名字符串（如 "bruteforce", "ivf", "clustered_join"）
 * @param similarity_threshold 相似度阈值
 * @param dimension 向量维度（如果为 0，则需要在后续设置）
 * @param window_size_ms 窗口大小（毫秒，如果为 0，则需要在后续设置）
 * @param step_size_ms 滑动步长（毫秒，如果为 0，则需要在后续设置）
 * @return JoinStrategyConfig 配置对象
 * @throws std::runtime_error 如果方法名无效
 */
JoinStrategyConfig createJoinStrategyConfigFromMethodName(
    const std::string& method_name,
    double similarity_threshold = 0.8,
    int dimension = 0,
    int64_t window_size_ms = 0,
    int64_t step_size_ms = 0);

// ==================== 枚举类型与字符串转换 ====================

std::string toString(JoinAlgorithm algo);
std::string toString(PartitionStrategy ps);
std::string toString(WindowStateType ws);
std::string toString(IndexStrategy is);
std::string toString(ClusteredIndexType cit);
std::string toString(SimilarityMode sm);
std::string toString(VSJoinIndexType vit);
std::string toString(VSJoinRouteMode rm);
std::string toString(VSJoinSnapshotFilterPolicy sp);

JoinAlgorithm parseJoinAlgorithm(const std::string& s);
PartitionStrategy parsePartitionStrategy(const std::string& s);
WindowStateType parseWindowStateType(const std::string& s);
IndexStrategy parseIndexStrategy(const std::string& s);
ClusteredIndexType parseClusteredIndexType(const std::string& s);
SimilarityMode parseSimilarityMode(const std::string& s);
VSJoinIndexType parseVSJoinIndexType(const std::string& s);
VSJoinRouteMode parseVSJoinRouteMode(const std::string& s);
VSJoinSnapshotFilterPolicy parseVSJoinSnapshotFilterPolicy(const std::string& s);

}  // namespace sageFlow

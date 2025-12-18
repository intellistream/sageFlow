#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace sageFlow {

/**
 * @brief Join 算法类型枚举
 */
enum class JoinAlgorithm {
    BRUTEFORCE,      ///< Ground Truth baseline
    IVF,             ///< IVF-based approximate join
    HNSW,            ///< HNSW-based approximate join
    HDR_TREE,        ///< HDR-Tree baseline
    CLUSTERED_JOIN,  ///< VectraFlow ClusteredJoin
    S3J,             ///< DEBS'23 S3J baseline
    VSJOIN,          ///< Our method
    FAISS_IVF,
    FAISS_HNSW
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
    TWO_TIER,            ///< TwoTierWindowState（写友好层+紧凑层）
    PARTITIONED_VECTOR   ///< PartitionedVectorState（向量空间分区）
};

/**
 * @brief 索引策略类型枚举
 */
enum class IndexStrategy {
    SHARED,           ///< 共享索引（所有实例使用同一索引）
    PARTITIONED       ///< 分区索引（每个分区独立索引）
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
    
    // ==================== 分区配置 ====================
    PartitionStrategy partition_strategy = PartitionStrategy::ROUND_ROBIN;
    int num_partitions = 4;  ///< 向量空间分区数（用于 LSH/CENTROID）
    
    // ==================== 窗口状态配置 ====================
    WindowStateType window_state_type = WindowStateType::SHARED;
    int64_t window_size_ms = 10000;  ///< 窗口大小（毫秒）
    int64_t step_size_ms = 1000;     ///< 滑动步长（毫秒）
    
    // ==================== 索引配置 ====================
    IndexStrategy index_strategy = IndexStrategy::SHARED;
    
    // ==================== IVF 参数 ====================
    int ivf_nlist = 100;           ///< IVF 聚类数量
    int ivf_nprobes = 10;          ///< IVF 搜索时探测的聚类数
    double ivf_rebuild_threshold = 0.3;  ///< 触发重建的阈值
    
    // ==================== HNSW 参数 ====================
    int hnsw_m = 16;                   ///< 每层最大邻居数
    int hnsw_ef_construction = 200;    ///< 构建时候选集大小
    int hnsw_ef_search = 50;           ///< 搜索时候选集大小
    
    // ==================== VSJoin 参数 ====================
    int vsjoin_num_hash_functions = 8;    ///< LSH 哈希函数数量
    double vsjoin_boundary_threshold = 0.1;  ///< 边界判定阈值
    int vsjoin_async_threads = 2;            ///< 异步处理线程数
    int64_t vsjoin_allowed_lateness = 1000;  ///< 允许的延迟（毫秒）
    
    // ==================== S3J 参数 ====================
    int s3j_num_centroids = 16;          ///< S3J 质心数量
    int64_t s3j_adapt_interval_ms = 1000; ///< 自适应调整间隔
    double s3j_load_threshold = 0.3;      ///< 负载不均衡阈值
    bool s3j_enable_adaptive = true;      ///< 是否启用自适应调整
    
    // ==================== ClusteredJoin 参数 ====================
    double clustered_overlap_ratio = 0.1;     ///< 边界重叠比例
    double clustered_rebalance_threshold = 0.3;  ///< 触发重平衡的阈值
    bool clustered_border_replication = true;    ///< 是否复制边界向量
    int clustered_training_samples = 1000;       ///< 训练样本数
    
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

// ==================== 枚举类型与字符串转换 ====================

std::string toString(JoinAlgorithm algo);
std::string toString(PartitionStrategy ps);
std::string toString(WindowStateType ws);
std::string toString(IndexStrategy is);

JoinAlgorithm parseJoinAlgorithm(const std::string& s);
PartitionStrategy parsePartitionStrategy(const std::string& s);
WindowStateType parseWindowStateType(const std::string& s);
IndexStrategy parseIndexStrategy(const std::string& s);

}  // namespace sageFlow

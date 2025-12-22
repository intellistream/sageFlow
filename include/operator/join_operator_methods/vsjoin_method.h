#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "operator/join_operator_methods/vsjoin_components/async_candidate_generator.h"
#include "operator/join_operator_methods/vsjoin_components/distance_verifier.h"
#include "concurrency/concurrency_manager.h"
#include "state/partitioned_vector_state.h"
#include "index/partitioned_index.h"
#include "coordination/partition_coordinator.h"
#include "execution/vector_space_partitioner.h"

#include <memory>
#include <string>

namespace sageFlow {

/**
 * @brief VSJoin 配置结构
 *
 * 用于配置 VSJoin 流式向量连接模式的各项参数。
 * VSJoin 使用向量空间分区策略，实现高效的跨分区相似性连接。
 */
struct VSJoinConfig {
    bool enabled = false;                    ///< 是否启用 VSJoin 模式
    int num_partitions = 8;                  ///< 向量空间分区数
    size_t compact_threshold = 100;          ///< 双层窗口压缩阈值
    bool enable_boundary_tracking = true;    ///< 启用边界向量追踪
    int64_t allowed_lateness = 0;            ///< 允许的延迟（毫秒，0=不处理延迟）
    int64_t watermark_delay = 1000;          ///< watermark 延迟（毫秒）
    size_t async_generator_threads = 4;      ///< 异步候选生成线程数
    size_t num_probes = 2;                   ///< 跨分区探测数
    int ivf_nlist = 100;                     ///< 每个分区 IVF 的聚类数
    int ivf_nprobes = 10;                    ///< IVF 查询时探测的聚类数
    double distance_alpha = 0.1;             ///< 距离到相似度的转换系数
    int dimension = 128;                     ///< 向量维度
};

/**
 * @brief VSJoin 方法实现
 *
 * VSJoin 使用以下技术：
 * - LSH (Locality Sensitive Hashing) 用于向量空间分区
 * - PartitionedVectorState 用于分区状态管理（双层窗口）
 * - PartitionedIndex 用于每个分区的索引管理
 * - PartitionCoordinator 用于延迟处理和 watermark 管理
 * - AsyncCandidateGenerator 用于异步候选生成
 * - DistanceVerifier 用于候选验证
 */
class VSJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param config VSJoin 配置
     * @param concurrency_manager 并发管理器
     */
    VSJoinMethod(const VSJoinConfig& config,
                 std::shared_ptr<ConcurrencyManager> concurrency_manager);
    
    ~VSJoinMethod() override;
    
    // ==================== BaseMethod 接口实现 ====================
    
    /**
     * @brief 执行 Eager 模式的 Join 查询
     * 
     * @param query_record 查询记录
     * @param query_slot 查询来源 slot（0=左流，1=右流）
     * @return 满足阈值的候选向量列表
     */
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot) override;
    
    // ==================== 生命周期管理 ====================
    
    /**
     * @brief 初始化 VSJoin 组件
     * @param subtask_index 子任务索引
     * @param parallelism 并行度
     */
    void initialize(size_t subtask_index, size_t parallelism);
    
    /**
     * @brief 关闭 VSJoin 组件
     */
    void close();
    
    // ==================== 状态管理 ====================
    
    /**
     * @brief 处理新记录（更新状态和索引）
     * @param record 新记录
     * @param slot 来源 slot
     * @param subtask_index 子任务索引
     * @return 处理状态
     */
    bool processRecord(std::unique_ptr<VectorRecord> record, int slot, size_t subtask_index);
    
    /**
     * @brief 清理过期记录
     * @param current_timestamp 当前时间戳
     * @param window_size 窗口大小
     * @param subtask_index 子任务索引
     */
    void evictExpired(int64_t current_timestamp, int64_t window_size, size_t subtask_index);
    
    // ==================== 配置访问 ====================
    
    /**
     * @brief 获取 VSJoin 配置
     * @return 配置引用
     */
    const VSJoinConfig& getConfig() const { return config_; }
    
    /**
     * @brief 设置 slot ID
     * @param left_slot_id 左侧 slot ID
     * @param right_slot_id 右侧 slot ID
     */
    void setSlotIds(int left_slot_id, int right_slot_id) {
        left_slot_id_ = left_slot_id;
        right_slot_id_ = right_slot_id;
    }
    
    /**
     * @brief 获取分区器
     * @return 向量空间分区器
     */
    std::shared_ptr<VectorSpacePartitioner> getPartitioner() const { return partitioner_; }
    
    /**
     * @brief 获取分区协调器
     * @return 分区协调器引用
     */
    PartitionCoordinator* getCoordinator() const { return coordinator_.get(); }
    
    /**
     * @brief 获取距离验证器
     * @return 距离验证器
     */
    std::shared_ptr<DistanceVerifier> getVerifier() const { return verifier_; }

private:
    // ==================== 配置 ====================
    VSJoinConfig config_;
    int left_slot_id_ = 0;
    int right_slot_id_ = 1;
    bool initialized_ = false;
    
    // ==================== 并发管理 ====================
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    
    // ==================== 向量空间分区 ====================
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    
    // ==================== 分区状态（双层窗口） ====================
    std::unique_ptr<PartitionedVectorState> left_state_;
    std::unique_ptr<PartitionedVectorState> right_state_;
    
    // ==================== 分区索引 ====================
    std::shared_ptr<PartitionedIndex> left_index_;
    std::shared_ptr<PartitionedIndex> right_index_;
    
    // ==================== 分区协调 ====================
    std::unique_ptr<PartitionCoordinator> coordinator_;
    
    // ==================== 异步候选生成 ====================
    std::unique_ptr<AsyncCandidateGenerator> left_async_generator_;
    std::unique_ptr<AsyncCandidateGenerator> right_async_generator_;
    
    // ==================== 距离验证 ====================
    std::shared_ptr<DistanceVerifier> verifier_;
    
    // ==================== 辅助方法 ====================
    
    /**
     * @brief 初始化分区器
     */
    void initPartitioner();
    
    /**
     * @brief 初始化分区状态
     */
    void initStates();
    
    /**
     * @brief 初始化分区索引
     * @param subtask_index 子任务索引
     */
    void initIndices(size_t subtask_index);
    
    /**
     * @brief 初始化分区协调器
     */
    void initCoordinator();
    
    /**
     * @brief 初始化异步候选生成器
     */
    void initAsyncGenerators();
    
    /**
     * @brief 初始化距离验证器
     */
    void initVerifier();
};

}  // namespace sageFlow

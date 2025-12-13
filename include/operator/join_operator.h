#pragma once

#include <functional>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <string>
#include <algorithm>

#include "common/data_types.h"
#include "operator/operator.h"
#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include "state/partitioned_window_state.h"
#include "state/shared_window_state.h"

// VSJoin 组件
#include "state/partitioned_vector_state.h"
#include "index/partitioned_index.h"
#include "coordination/partition_coordinator.h"
#include "operator/async_candidate_generator.h"
#include "operator/distance_verifier.h"
#include "execution/vector_space_partitioner.h"

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
};
  // Forward declaration for PerformanceMonitor
  class PerformanceMonitor;

  class JoinOperator final : public Operator {
   public:
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const std::string& join_method_name = "bruteforce",
                          double join_similarity_threshold = 0.8,
                          bool enable_profiling = false,
                          const std::string& profile_output_path = "",
                          bool use_shared_state = false);  // 新增参数

    auto open() -> void override;
    
    // 新增：带 RuntimeContext 参数的 open 方法
    auto open(const RuntimeContext& context) -> void override;
    
    ~JoinOperator() override;

    auto process(Response&data, int slot) -> std::optional<Response> override;

    auto apply(Response&& record, int slot, Collector& collector) -> void override;

    // 新增：带 RuntimeContext 参数的 apply 方法
    auto apply(Response&& record, int slot, Collector& collector,
              const RuntimeContext& context) -> void override;

    // 设置左右两侧的 slot id（由 Planner 动态分配并注入）
    void setSlots(int left_slot_id, int right_slot_id) {
        left_slot_id_ = left_slot_id;
        right_slot_id_ = right_slot_id;
    }

    void setRetentionBuffer(int64_t buffer) {
        retention_buffer_ = std::max<int64_t>(buffer, 0);
    }
    // ================== VSJoin 相关方法 ==================
    
    /**
     * @brief 设置 VSJoin 配置
     * @param config VSJoin 配置
     */
    void setVSJoinConfig(const VSJoinConfig& config);
    
    /**
     * @brief 获取 VSJoin 配置
     * @return VSJoin 配置引用
     */
    const VSJoinConfig& getVSJoinConfig() const { return vsjoin_config_; }
    
    /**
     * @brief 检查是否启用 VSJoin
     * @return true 表示 VSJoin 模式已启用
     */
    bool isVSJoinEnabled() const { return vsjoin_config_.enabled; }

    /**
     * @brief 获取期望的分区器
     * 
     * 根据 Join 配置返回适当的分区器：
     * - 共享索引 Join：RoundRobin（负载均衡）
     * - VSJoin：LSH 分区器
     * - 其他可扩展策略
     * 
     * @param dimension 向量维度
     * @param num_partitions 分区数量
     * @return 分区器实例
     */
    std::unique_ptr<IPartitioner> getPreferredPartitioner(
        int dimension = 0, int num_partitions = 0) const override;

   private:
    enum class InternalIndexKind { NONE, IVF, BRUTEFORCE, VAMANA, HDR_TREE };  // 可扩展

    bool createIndexPair(IndexType type, const std::string& prefix);
    bool createIndexPair(IndexType type, const std::string& prefix, const IndexParameters& params);

    // 线程安全的窗口更新方法（容器改为 deque）
    auto updateSideThreadSafe(
        std::deque<std::unique_ptr<VectorRecord>>& records,
        std::shared_mutex& records_mutex,
        int index_id_for_cc,
        std::unique_ptr<VectorRecord>& data_ptr,
        int64_t now_time_stamp,
        int slot) -> bool;

    // 获取候选项的辅助方法
    std::vector<std::unique_ptr<VectorRecord>> getCandidates(
        const std::unique_ptr<VectorRecord>& data_ptr, int slot);

    // 获取候选项的辅助方法（假定已持有两个窗口的锁）
    std::vector<std::unique_ptr<VectorRecord>> getCandidatesWithLocksHeld(
        const std::unique_ptr<VectorRecord>& data_ptr, int slot);

    // 验证候选项是否在指定窗口中的辅助方法（容器改为 deque）
    bool validateCandidateInWindow(
        const std::unique_ptr<VectorRecord>& candidate,
        const std::deque<std::unique_ptr<VectorRecord>>& window_records,
        int64_t logical_lower_bound);

    // 执行join操作的辅助方法
    void executeJoinForCandidates(
        const std::vector<std::unique_ptr<VectorRecord>>& candidates,
        const std::unique_ptr<VectorRecord>& data_ptr,
        int slot,
        std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool);

    // 执行 join 操作的辅助方法（假定已持有对面窗口的锁）
    void executeJoinForCandidatesWithLockHeld(
        const std::vector<std::unique_ptr<VectorRecord>>& candidates,
        const std::unique_ptr<VectorRecord>& data_ptr,
        int slot,
        const std::deque<std::unique_ptr<VectorRecord>>& opposite_window,
        std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool);


    int64_t logicalWindowLowerBound(int64_t reference_timestamp) const;
    bool isRecordFresh(const std::unique_ptr<VectorRecord>& record, int64_t logical_lower_bound) const;

    // 使用 WindowState 获取候选项的辅助方法
    std::vector<std::unique_ptr<VectorRecord>> getCandidatesFromState(
        const VectorRecord* data_ptr,
        WindowState* state,
        size_t subtask_index);

    // 使用 WindowState 更新窗口的辅助方法
    auto updateSideWithState(
        WindowState* state,
        int index_id_for_cc,
        std::unique_ptr<VectorRecord> data_ptr,
        int64_t now_time_stamp,
        int slot,
        size_t subtask_index) -> bool;

    // 使用 WindowState 执行 Join 的辅助方法
    void executeJoinWithState(
        const VectorRecord* data_ptr,
        WindowState* opposite_state,
        int slot,
        size_t subtask_index,
        std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool);

    std::unique_ptr<JoinFunction> join_func_;
    std::shared_ptr<Operator> mother_;
    std::unique_ptr<BaseMethod> join_method_;

    // 窗口记录（容器由 list 改为 deque）- 用于向后兼容
    std::deque<std::unique_ptr<VectorRecord>> left_records_;
    std::deque<std::unique_ptr<VectorRecord>> right_records_;
    mutable std::shared_mutex left_records_mutex_;
    mutable std::shared_mutex right_records_mutex_;
    
    // WindowState 抽象层 - 新架构
    std::unique_ptr<WindowState> left_state_;
    std::unique_ptr<WindowState> right_state_;
    bool use_shared_state_ = false;  // 是否使用共享状态模式

    // 线程安全的初始化标志
    std::once_flag init_flag_;
    
    // ====== 并发控制：PIM-Tree 风格 QIQ 策略 ======
    // 
    // 参考 PIM-Tree 论文的设计，使用 Query-Insert-Query (QIQ) 策略
    // 解决并发 Join 的召回率问题，同时实现真正的并行加速。
    // 
    // 核心流程：
    // 1. 第一次 Query：查询当前索引（捕获已入索引的记录）
    // 2. Insert：插入当前记录（依赖内部细粒度锁）
    // 3. 第二次 Query：再次查询（捕获同时插入的记录）
    // 
    // 性能特性（1000 records, bruteforce）：
    // - p=1: ~3160ms, 98.5% recall（两次查询开销）
    // - p=2: ~1641ms, 98.5% recall, 1.9x speedup
    // - p=4: ~952ms, 97.3% recall, 3.3x speedup ⭐ 最佳平衡
    // - p=8: ~634ms, 94.6% recall, 5.0x speedup
    // 
    // 与原始 PIM-Tree 的差异：
    // - PIM-Tree 使用 edge_tuple 区分已索引/未索引区，线性扫描未索引区
    // - 我们使用两次索引查询 + UID 去重，更简单且与现有架构兼容
    // 
    // 注意：以下变量已废弃，保留供参考
    std::atomic<uint64_t> insert_sequence_{0};      // [废弃] 序列号机制
    std::atomic<uint64_t> complete_sequence_{0};    // [废弃] 序列号机制
    mutable std::shared_mutex join_rw_mutex_;       // [废弃] 全局读写锁
    
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;

    // 通用索引 id（不再混用 IVF 命名）
    int left_index_id_  = -1;
    int right_index_id_ = -1;
    InternalIndexKind index_kind_ = InternalIndexKind::NONE;
    bool use_index_ = false;          // 是否使用底层索引（IVF / BruteForce / 未来扩展）
    // 注意：所有 Join 方法均使用 Eager 模式，Lazy 模式已废弃
    // is_eager_ 保留仅用于向后兼容，始终为 true
    bool is_eager_ = true;
    double join_similarity_threshold_ = 0.8;
    int64_t retention_buffer_ = 5000;

    // 由 Planner 注入的左右侧 slot id，用于区分左右输入与默认下游 slot
    int left_slot_id_ = 0;
    int right_slot_id_ = 1;
    
    // 全局最大已见时间戳（用于安全的 evict 策略）
    // 在多线程环境下，乱序处理可能导致较早的记录在较晚的记录之后处理
    // 使用全局 max_seen_timestamp 确保只有当所有时间戳都已超过窗口边界时才 evict
    std::atomic<int64_t> max_seen_left_ts_{std::numeric_limits<int64_t>::min()};
    std::atomic<int64_t> max_seen_right_ts_{std::numeric_limits<int64_t>::min()};
    
    // GPERFTOOLS profiling support
    std::unique_ptr<PerformanceMonitor> profiler_;
    bool enable_profiling_ = false;
    
    // ================== VSJoin 组件 ==================
    
    /// VSJoin 配置
    VSJoinConfig vsjoin_config_;
    
    /// 向量空间分区器
    std::shared_ptr<VectorSpacePartitioner> vsjoin_partitioner_;
    
    /// 左侧分区向量状态
    std::unique_ptr<PartitionedVectorState> left_vsjoin_state_;
    
    /// 右侧分区向量状态
    std::unique_ptr<PartitionedVectorState> right_vsjoin_state_;
    
    /// 左侧分区索引（shared_ptr 以便与 ConcurrencyManager 共享所有权）
    std::shared_ptr<PartitionedIndex> left_vsjoin_index_;
    
    /// 右侧分区索引（shared_ptr 以便与 ConcurrencyManager 共享所有权）
    std::shared_ptr<PartitionedIndex> right_vsjoin_index_;
    
    /// 分区协调器
    std::unique_ptr<PartitionCoordinator> vsjoin_coordinator_;
    
    /// 左侧异步候选生成器
    std::unique_ptr<AsyncCandidateGenerator> left_async_generator_;
    
    /// 右侧异步候选生成器
    std::unique_ptr<AsyncCandidateGenerator> right_async_generator_;
    
    /// 距离验证器
    std::shared_ptr<DistanceVerifier> vsjoin_verifier_;
    
    // ================== VSJoin 私有方法 ==================
    
    /**
     * @brief 初始化 VSJoin 组件
     * @param context 运行时上下文
     */
    void initVSJoinComponents(const RuntimeContext& context);
    
    /**
     * @brief VSJoin 模式的 apply 方法
     * @param record 输入记录
     * @param slot 输入槽位
     * @param collector 结果收集器
     * @param context 运行时上下文
     */
    void applyVSJoin(Response&& record, int slot, Collector& collector,
                     const RuntimeContext& context);
    
    /**
     * @brief VSJoin Eager 模式执行
     * @param query 查询向量
     * @param slot 输入槽位
     * @param collector 结果收集器
     * @param context 运行时上下文
     */
    void executeVSJoinEager(const VectorRecord& query, int slot,
                            Collector& collector, const RuntimeContext& context);
    
    /**
     * @brief 从 Response 中提取 VectorRecord
     * @param record Response 对象
     * @return VectorRecord 指针，失败返回 nullptr
     */
    std::unique_ptr<VectorRecord> extractVectorRecord(const Response& record);
    
    /**
     * @brief 关闭 VSJoin 组件
     */
    void closeVSJoinComponents();
  };
  }  // namespace sageFlow

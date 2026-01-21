#pragma once

#include <functional>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <string>
#include <algorithm>
#include <vector>
#include <thread>
#include <chrono>
#include <unordered_set>
#include <mutex>

#include "common/data_types.h"
#include "operator/operator.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include "state/partitioned_window_state.h"
#include "state/shared_window_state.h"

namespace sageFlow {
  // Forward declaration for PerformanceMonitor
  class PerformanceMonitor;

  class JoinOperator final : public Operator {
   public:
    /**
     * @brief 使用方法名字符串构造 JoinOperator（向后兼容）
     */
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const std::string& join_method_name = "bruteforce",
                          double join_similarity_threshold = 0.8,
                          bool enable_profiling = false,
                          const std::string& profile_output_path = "",
                          bool use_shared_state = false);

    /**
     * @brief 使用策略配置构造 JoinOperator（E-01 新增）
     *
     * 通过 JoinStrategyConfig 创建完整的 Join 策略，包括：
     * - JoinMethod（通过 JoinStrategyFactory）
     * - WindowState（左右两侧）
     * - 索引（共享或分区）
     *
     * @param join_func Join 函数
     * @param concurrency_manager 并发管理器
     * @param config 策略配置
     * @param enable_profiling 是否启用性能分析
     * @param profile_output_path 性能分析输出路径
     */
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const JoinStrategyConfig& config,
                          bool enable_profiling = false,
                          const std::string& profile_output_path = "");

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

    /**
     * @brief E-01: 使用策略配置初始化 JoinOperator
     *
     * 通过 JoinStrategyFactory 创建所有必要的组件：
     * - JoinMethod
     * - WindowState (左右两侧)
     * - 索引
     *
     * @param context 运行时上下文
     */
    void initializeWithStrategyConfig(const RuntimeContext& context);

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

    // E-01: 策略配置支持
    JoinStrategyConfig strategy_config_;       // 策略配置
    bool use_strategy_config_ = false;         // 是否使用策略配置模式
    size_t parallelism_ = 1;                   // 并行度（从 RuntimeContext 获取）
    
    /**
     * @brief 检查是否使用分区策略
     * 
     * 分区策略（Centroid/LSH）使用 PartitionedWindowState + 分区索引，
     * 分区内无锁竞争，可以直接使用 IQ 逻辑。
     * 
     * @return true 表示使用分区策略
     */
    bool isPartitionedStrategy() const {
        if (!use_strategy_config_) return false;
        return strategy_config_.partition_strategy == PartitionStrategy::CENTROID ||
               strategy_config_.partition_strategy == PartitionStrategy::LSH;
    }

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
    
    // ====== 批量删除配置 ======
    // 当 WindowState 中过期记录数超过此阈值时，触发批量删除索引中的过期数据
    // 
    // 公式设计原理：
    //   window_vector_count = window_size_ms / step_size_ms  // 窗口内向量数量
    //   batch_delete_threshold_ = max(kMinBatchDeleteThreshold, 
    //                                 window_vector_count * parallelism / kBatchDeleteDivisor)
    //
    // - window_vector_count 越大，过期记录积累越多，可以容忍更大的批次
    // - parallelism 越高，多个 subtask 并发操作，需要更大的阈值减少删除频率
    // - kBatchDeleteDivisor (10) 控制批次大小与窗口向量数的比例
    // - kMinBatchDeleteThreshold (50) 保证最小批次，避免过于频繁的删除
    //
    // 示例计算（window=10000ms, step=10ms → vector_count=1000）：
    //   p=1:  max(50, 1000*1/10)  = max(50, 100)  = 100
    //   p=4:  max(50, 1000*4/10)  = max(50, 400)  = 400
    //   p=16: max(50, 1000*16/10) = max(50, 1600) = 1600
    //
    // 示例计算（window=60000ms, step=10ms → vector_count=6000）：
    //   p=1:  max(50, 6000*1/10)  = max(50, 600)  = 600
    //   p=8:  max(50, 6000*8/10)  = max(50, 4800) = 4800
    //
    static constexpr size_t kMinBatchDeleteThreshold = 50;   ///< 最小批量删除阈值
    static constexpr size_t kBatchDeleteDivisor = 10;         ///< 批量删除除数因子
    size_t batch_delete_threshold_ = kMinBatchDeleteThreshold; ///< 实际使用的批量删除阈值

    // ==================== VSJoin 专用 ====================
    // Local Index ID 数组（每分区独立）
    std::vector<int> vsjoin_local_left_ids_;   // size = parallelism_
    std::vector<int> vsjoin_local_right_ids_;  // size = parallelism_

    // Global Index ID（共享只读）
    int vsjoin_global_left_id_ = -1;
    int vsjoin_global_right_id_ = -1;

    // ==================== VSJoin 后台重建 ====================
    std::once_flag rebuild_thread_started_;
    std::unique_ptr<std::thread> rebuild_thread_;
    std::atomic<bool> rebuild_running_{false};
    std::atomic<int64_t> rebuild_interval_ms_{5000};

    void globalIndexRebuildLoop();
    void startGlobalIndexRebuilder();
    void stopGlobalIndexRebuilder();
    
    // GPERFTOOLS profiling support
    std::unique_ptr<PerformanceMonitor> profiler_;
    bool enable_profiling_ = false;
  };
  }  // namespace sageFlow

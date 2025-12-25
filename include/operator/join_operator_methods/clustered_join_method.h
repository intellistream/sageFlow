#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>
#include <unordered_set>
#include <atomic>

#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "function/join_function.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"

namespace sageFlow {

/**
 * @brief ClusteredJoin 方法实现（方案 A：独立索引）
 *
 * 每个 subtask 拥有独立的 left/right 索引，数据通过
 * CentroidPartitioner 分发到对应的 subtask。
 *
 * 边界向量通过多播复制到相邻分区，使用 Owner-Computes
 * 规则去重避免重复输出。
 *
 * 核心特性：
 * 1. 每个 subtask 在 initialize() 中创建独立的 left/right 索引
 * 2. ExecuteEager() 只查询本地索引
 * 3. 使用 Owner-Computes 去重：min(left_uid, right_uid) % parallelism == subtask_index
 *
 * 推荐配置：
 * - partition_strategy: centroid
 * - window_state_type: partitioned
 * - index_strategy: partitioned
 */
class ClusteredJoinMethod final : public BaseMethod {
 public:
  /**
   * @brief 配置结构体
   */
  struct Config {
    double similarity_threshold = 0.8;  ///< 相似度阈值
    int dimension = 128;                ///< 向量维度
    int64_t window_size_ms = 10000;     ///< 窗口大小（毫秒）
    
    // 索引类型
    ClusteredIndexType index_type = ClusteredIndexType::IVF;
    
    // IVF 参数
    int ivf_nlist = 100;    ///< IVF 聚类数量
    int ivf_nprobes = 10;   ///< IVF 搜索时探测的聚类数
    
    // HNSW 参数
    int hnsw_m = 16;                   ///< 每层最大邻居数
    int hnsw_ef_construction = 200;    ///< 构建时候选集大小
    int hnsw_ef_search = 50;           ///< 搜索时候选集大小
    
    // 旧配置兼容（已废弃，保留字段但不使用）
    int num_partitions = 16;            ///< [已废弃] 分区数量
    double overlap_ratio = 0.1;         ///< [已废弃] 边界重叠比例
    double rebalance_threshold = 0.3;   ///< [已废弃] 重平衡阈值
    bool use_border_replication = true; ///< [已废弃] 是否复制边界向量
    int training_samples = 1000;        ///< [已废弃] 训练样本数
    double learning_rate = 0.01;        ///< [已废弃] 增量更新学习率
  };

  /**
   * @brief 构造函数（使用 Config 结构体）
   * @param config 配置
   */
  explicit ClusteredJoinMethod(const Config& config);

  /**
   * @brief 简化构造函数（仅指定阈值和维度）
   * @param similarity_threshold 相似度阈值
   * @param dimension 向量维度
   */
  ClusteredJoinMethod(double similarity_threshold, int dimension);

  ~ClusteredJoinMethod() override = default;

  // ==================== 生命周期 ====================

  /**
   * @brief 初始化：创建独立索引
   *
   * 在 JoinOperator::open(RuntimeContext) 中调用。
   * 每个 subtask 创建自己的 left_index 和 right_index。
   *
   * @param context 运行时上下文，包含 subtask_index 和 parallelism
   * @param concurrency_manager 用于创建索引
   */
  void initialize(const RuntimeContext& context,
                  std::shared_ptr<ConcurrencyManager> concurrency_manager);

  /**
   * @brief 关闭：清理索引和状态
   */
  void close();

  // ==================== BaseMethod 接口实现 ====================

  /**
   * @brief 执行 Eager 模式 Join
   *
   * 流程：
   * 1. 在本地对侧索引中查询候选项
   * 2. 应用 Owner-Computes 去重
   * 3. 返回属于当前 subtask 的匹配结果
   *
   * @param query_record 查询记录
   * @param query_slot 来源 slot (0=左流, 1=右流)
   * @return 满足阈值的候选向量列表（已去重）
   */
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record,
      int query_slot) override;

  // ==================== 状态管理 ====================

  /**
   * @brief 添加记录到本地状态和索引
   *
   * @param record 记录
   * @param slot 来源 slot (0=左流, 1=右流)
   */
  void addRecord(std::unique_ptr<VectorRecord> record, int slot);

  /**
   * @brief 驱逐过期记录
   *
   * @param current_timestamp 当前时间戳
   */
  void evictExpired(int64_t current_timestamp);

  /**
   * @brief 检查是否已初始化
   */
  bool isInitialized() const { return initialized_; }

  /**
   * @brief 获取配置
   */
  const Config& getConfig() const { return config_; }

  /**
   * @brief 获取方法名称
   * @return 方法名
   */
  std::string getName() const { return "ClusteredJoin"; }

  /**
   * @brief 获取左侧索引 ID
   */
  int getLeftIndexId() const { return left_index_id_; }

  /**
   * @brief 获取右侧索引 ID
   */
  int getRightIndexId() const { return right_index_id_; }

  /**
   * @brief 获取当前 subtask 索引
   */
  size_t getSubtaskIndex() const { return subtask_index_; }

  /**
   * @brief 获取并行度
   */
  size_t getParallelism() const { return parallelism_; }

  /**
   * @brief 获取左侧窗口记录数
   */
  size_t getLeftWindowSize() const { return left_window_.size(); }

  /**
   * @brief 获取右侧窗口记录数
   */
  size_t getRightWindowSize() const { return right_window_.size(); }

  /**
   * @brief 设置有效并行度
   * 
   * 当 CentroidPartitioner 未训练时，所有数据都路由到 subtask 0，
   * 此时 effective_parallelism 应设为 1 以禁用 Owner-Computes 去重。
   * 
   * @param effective_p 有效并行度
   */
  void setEffectiveParallelism(size_t effective_p) {
    effective_parallelism_ = effective_p;
  }

 private:
  Config config_;

  // 运行时信息
  size_t subtask_index_ = 0;
  size_t parallelism_ = 1;
  size_t effective_parallelism_ = 1;  ///< 有效并行度（用于 Owner-Computes）
  bool initialized_ = false;

  // 索引管理
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  int left_index_id_ = -1;
  int right_index_id_ = -1;

  // 本地窗口状态（每个 subtask 独立维护）
  std::deque<std::unique_ptr<VectorRecord>> left_window_;
  std::deque<std::unique_ptr<VectorRecord>> right_window_;
  std::unordered_set<uint64_t> left_uids_;   ///< 左侧窗口中的 UID 集合（用于快速验证）
  std::unordered_set<uint64_t> right_uids_;  ///< 右侧窗口中的 UID 集合（用于快速验证）

  // ==================== 内部方法 ====================

  /**
   * @brief 创建指定类型的索引
   * @param name 索引名称
   * @return 索引 ID
   */
  int createIndex(const std::string& name);

  /**
   * @brief BruteForce 模式的 Eager 查询
   * 
   * 直接遍历本地窗口计算相似度，不使用索引。
   * 因为 BruteForce 索引会查询整个 StorageManager，无法区分左右流。
   * 
   * @param query_record 查询记录
   * @param query_slot 查询来源 slot
   * @return 满足阈值且属于当前 subtask 的候选列表
   */
  std::vector<std::unique_ptr<VectorRecord>> executeEagerBruteForce(
      const VectorRecord& query_record,
      int query_slot);

  /**
   * @brief 使用索引的 Eager 查询（IVF/HNSW）
   * 
   * 通过 ConcurrencyManager 查询索引。IVF 和 HNSW 维护自己的 UID 列表，
   * 可以正确区分属于该索引的记录。
   * 
   * @param query_record 查询记录
   * @param query_slot 查询来源 slot
   * @return 满足阈值且属于当前 subtask 的候选列表
   */
  std::vector<std::unique_ptr<VectorRecord>> executeEagerWithIndex(
      const VectorRecord& query_record,
      int query_slot);

  /**
   * @brief Owner-Computes 去重判断
   *
   * 对于匹配对 (left_uid, right_uid)，只有 owner_subtask 输出该匹配对。
   * owner_subtask = min(left_uid, right_uid) % effective_parallelism
   *
   * 注意：使用 effective_parallelism_ 而不是 parallelism_。
   * 当 CentroidPartitioner 未训练时，effective_parallelism_ = 1，
   * 这使得所有匹配都由 subtask 0 输出（因为所有数据都路由到 subtask 0）。
   *
   * @param left_uid 左侧记录 UID
   * @param right_uid 右侧记录 UID
   * @return true 如果当前 subtask 是 owner
   */
  bool isOwner(uint64_t left_uid, uint64_t right_uid) const {
    return (std::min(left_uid, right_uid) % effective_parallelism_) == subtask_index_;
  }

  /**
   * @brief 获取对侧索引 ID
   * @param slot 当前 slot (0=左流查右索引, 1=右流查左索引)
   * @return 对侧索引 ID
   */
  int getOppositeIndexId(int slot) const {
    return (slot == 0) ? right_index_id_ : left_index_id_;
  }

  /**
   * @brief 获取对侧窗口引用
   * @param slot 当前 slot (0=左流查右窗口, 1=右流查左窗口)
   * @return 对侧窗口的引用
   */
  std::deque<std::unique_ptr<VectorRecord>>& getOppositeWindow(int slot) {
    return (slot == 0) ? right_window_ : left_window_;
  }

  const std::deque<std::unique_ptr<VectorRecord>>& getOppositeWindow(int slot) const {
    return (slot == 0) ? right_window_ : left_window_;
  }

  /**
   * @brief 获取对侧 UID 集合引用
   * @param slot 当前 slot (0=左流查右 UIDs, 1=右流查左 UIDs)
   * @return 对侧 UID 集合的引用
   */
  std::unordered_set<uint64_t>& getOppositeUids(int slot) {
    return (slot == 0) ? right_uids_ : left_uids_;
  }

  const std::unordered_set<uint64_t>& getOppositeUids(int slot) const {
    return (slot == 0) ? right_uids_ : left_uids_;
  }

  /**
   * @brief 获取当前侧窗口引用
   * @param slot 当前 slot
   * @return 当前侧窗口的引用
   */
  std::deque<std::unique_ptr<VectorRecord>>& getCurrentWindow(int slot) {
    return (slot == 0) ? left_window_ : right_window_;
  }

  /**
   * @brief 获取当前侧 UID 集合引用
   * @param slot 当前 slot
   * @return 当前侧 UID 集合的引用
   */
  std::unordered_set<uint64_t>& getCurrentUids(int slot) {
    return (slot == 0) ? left_uids_ : right_uids_;
  }

  /**
   * @brief 获取当前侧索引 ID
   * @param slot 当前 slot
   * @return 当前侧索引 ID
   */
  int getCurrentIndexId(int slot) const {
    return (slot == 0) ? left_index_id_ : right_index_id_;
  }
};

}  // namespace sageFlow

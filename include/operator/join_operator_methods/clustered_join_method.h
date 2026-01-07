#pragma once

#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "function/join_function.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "state/window_state.h"

namespace sageFlow {

/**
 * @brief ClusteredJoin 方法实现（重构版：统一架构）
 *
 * 采用与共享索引方法统一的架构：
 * - 共享索引模式: RoundRobin + SharedWindowState + 全局索引
 * - 分区索引模式: CentroidPartitioner + PartitionedWindowState + 分区索引
 *
 * 核心特性：
 * 1. 不再维护内部窗口状态，使用外部传入的 WindowState
 * 2. 不再创建索引，使用外部传入的索引 ID
 * 3. ExecuteEager 仅负责索引查询
 * 4. 与其他 Join 方法共享统一的 apply() 流程：
 *    - updateSideWithState(): 更新 WindowState + 插入索引
 *    - getCandidatesFromState() → ExecuteEager(): 获取候选
 *    - executeJoinWithState(): 执行 Join 函数
 *
 * 去重机制：
 * - 在 Sink 层使用统一去重（基于 combined_id = left_uid * 1000000 + right_uid）
 * - 不再使用 Owner-Computes 规则
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
   * @brief 初始化
   *
   * 记录 subtask_index 和 parallelism 用于 Owner-Computes 去重。
   * 不再创建索引，索引由 JoinOperator 管理。
   *
   * @param context 运行时上下文
   * @param concurrency_manager 并发管理器（用于索引查询，BruteForce 模式可为 nullptr）
   */
  void initialize(const RuntimeContext& context,
                  std::shared_ptr<ConcurrencyManager> concurrency_manager);

  /**
   * @brief 设置 WindowState（BruteForce 模式必需）
   * 
   * 在 BruteForce 模式下，直接从 WindowState 获取数据，
   * 而不是通过 ConcurrencyManager 的 StorageManager。
   * 
   * @param left_state 左侧窗口状态
   * @param right_state 右侧窗口状态
   */
  void setWindowStates(WindowState* left_state, WindowState* right_state);

  /**
   * @brief 设置索引 ID
   *
   * 由 JoinOperator 在创建索引后调用。
   * BruteForce 模式下可以跳过（索引 ID 保持 -1）。
   *
   * @param left_index_id 左侧索引 ID
   * @param right_index_id 右侧索引 ID
   */
  void setIndexIds(int left_index_id, int right_index_id);

  /**
   * @brief 关闭：清理索引和状态
   */
  void close();

  // ==================== BaseMethod 接口实现 ====================

  /**
   * @brief 执行 Eager 模式 Join
   *
   * 流程：
   * 1. 在对侧索引中查询候选项
   * 2. 应用 Owner-Computes 去重（分区模式）
   * 3. 返回属于当前 subtask 的候选向量
   *
   * 注意：此方法不再处理窗口状态，调用方需要先通过
   * updateSideWithState() 更新窗口和索引。
   *
   * @param query_record 查询记录
   * @param query_slot 来源 slot (0=左流, 1=右流)
   * @param subtask_index 当前执行的 subtask 索引（用于 PartitionedWindowState 分区访问）
   * @return 满足阈值的候选向量列表
   */
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record,
      int query_slot,
      size_t subtask_index = 0) override;

  // ==================== 配置与状态查询 ====================

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
   * @brief 设置有效并行度（已弃用）
   *
   * 注意：Owner-Computes 机制已移除，此方法仅保留向后兼容性。
   * 去重现在在 Sink 层统一处理。
   *
   * @param effective_p 有效并行度（已弃用）
   * @deprecated Owner-Computes 机制已移除
   */
  void setEffectiveParallelism(size_t effective_p) {
    effective_parallelism_ = effective_p;
  }

  /**
   * @brief 获取有效并行度
   */
  size_t getEffectiveParallelism() const { return effective_parallelism_; }

  /**
   * @brief 检查是否使用分区模式
   * 
   * 分区模式下使用 Owner-Computes 去重，共享模式下不需要。
   * 
   * @return true 表示分区模式
   */
  bool isPartitionedMode() const { return effective_parallelism_ > 1; }

  /**
   * @brief 获取推荐的索引类型
   * @return IndexType 枚举值
   */
  IndexType getPreferredIndexType() const;

  /**
   * @brief 获取推荐的索引参数
   * @return IndexParameters variant
   */
  IndexParameters getPreferredIndexParams() const;

 private:
  Config config_;

  // 运行时信息
  size_t subtask_index_ = 0;
  size_t parallelism_ = 1;
  size_t effective_parallelism_ = 1;  ///< 有效并行度（用于 Owner-Computes）
  bool initialized_ = false;

  // ConcurrencyManager（用于索引查询，仅 IVF/HNSW 模式使用）
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  
  // WindowState（用于 BruteForce 模式直接访问窗口数据）
  // BruteForce 模式下绕过 ConcurrencyManager，直接从 WindowState 获取快照
  // 这样更 cache 友好，且与 BruteForceBaseline 架构一致
  WindowState* left_state_ = nullptr;
  WindowState* right_state_ = nullptr;
  
  // 索引 ID（由 JoinOperator 创建并传入）
  int left_index_id_ = -1;
  int right_index_id_ = -1;

  // ==================== 内部方法 ====================

  /**
   * @brief Owner-Computes 去重判断（已弃用）
   *
   * 注意：此方法已弃用，不再使用 Owner-Computes 规则。
   * 去重现在在 Sink 层统一处理。
   *
   * @param left_uid 左侧记录 UID
   * @param right_uid 右侧记录 UID
   * @return true（始终返回 true，因为不再进行去重）
   * @deprecated 不再使用 Owner-Computes 去重机制
   */
  bool isOwner(uint64_t left_uid, uint64_t right_uid) const {
    return true;  // 不再进行 Owner-Computes 去重
  }

  /**
   * @brief 获取对侧索引 ID
   * @param slot 当前 slot (0=左流查右索引, 1=右流查左索引)
   * @return 对侧索引 ID
   */
  int getOppositeIndexId(int slot) const {
    return (slot == 0) ? right_index_id_ : left_index_id_;
  }

  // ==================== 双模式执行 ====================

  /**
   * @brief BruteForce 模式：直接从 WindowState 获取快照进行暴力搜索
   * 
   * 与 BruteForceBaseline 架构一致，绕过 ConcurrencyManager，
   * 直接从本地 WindowState 获取数据，更 cache 友好。
   * 
   * @param query_record 查询记录
   * @param query_slot 查询来源 slot
   * @param subtask_index 当前执行的 subtask 索引
   * @return 满足阈值的候选记录列表
   */
  std::vector<std::unique_ptr<VectorRecord>> executeEagerBruteForce(
      const VectorRecord& query_record,
      int query_slot,
      size_t subtask_index);

  /**
   * @brief IVF/HNSW 模式：通过 ConcurrencyManager 查询索引
   * 
   * 使用近似索引加速查询，适用于大规模数据。
   * 
   * @param query_record 查询记录
   * @param query_slot 查询来源 slot
   * @param subtask_index 当前执行的 subtask 索引
   * @return 满足阈值的候选记录列表
   */
  std::vector<std::unique_ptr<VectorRecord>> executeEagerIndexed(
      const VectorRecord& query_record,
      int query_slot,
      size_t subtask_index);

  /**
   * @brief 计算两个向量的相似度
   * 
   * 使用 L2 距离 + 指数衰减转换，与 ComputeEngine::Similarity 一致。
   * 
   * @param a 第一个向量
   * @param b 第二个向量
   * @return 相似度值，范围 [0.0, 1.0]
   */
  double computeSimilarity(const std::vector<float>& a, 
                          const std::vector<float>& b) const;
};

}  // namespace sageFlow

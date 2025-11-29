#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "execution/centroid_partitioner.h"
#include "function/join_function.h"
#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"

namespace sageFlow {

/**
 * @brief ClusteredJoin 方法
 *
 * 基于质心分区的分布式 Join 实现。
 * 参考 VectraFlow 项目设计，使用 k-means 聚类将向量空间划分为多个分区，
 * 相似向量有高概率被分配到同一分区，从而减少跨分区查询。
 *
 * 特性：
 * - 使用 CentroidPartitioner 进行向量空间分区
 * - 支持边界向量的多分区查询
 * - 分区内使用 IVF 索引加速搜索
 * - 支持动态重平衡
 *
 * 推荐配置：
 * - partition_strategy: centroid
 * - window_state_type: partitioned
 * - index_strategy: ivf
 */
class ClusteredJoinMethod final : public BaseMethod {
 public:
  /**
   * @brief 配置结构
   */
  struct Config {
    double similarity_threshold = 0.8;  ///< 相似度阈值
    int num_partitions = 16;            ///< 分区数量
    double overlap_ratio = 0.1;         ///< 边界重叠比例
    double rebalance_threshold = 0.3;   ///< 重平衡阈值
    bool use_border_replication = true; ///< 是否复制边界向量
    int dimension = 128;                ///< 向量维度
    int training_samples = 1000;        ///< 训练样本数
    double learning_rate = 0.01;        ///< 增量更新学习率
  };

  /**
   * @brief 构造函数
   * @param left_index_id 左侧索引 ID
   * @param right_index_id 右侧索引 ID
   * @param config 配置
   * @param concurrency_manager 并发管理器
   */
  ClusteredJoinMethod(int left_index_id,
                      int right_index_id,
                      const Config& config,
                      const std::shared_ptr<ConcurrencyManager>& concurrency_manager);

  /**
   * @brief 简化构造函数（使用默认配置）
   * @param left_index_id 左侧索引 ID
   * @param right_index_id 右侧索引 ID
   * @param join_similarity_threshold 相似度阈值
   * @param concurrency_manager 并发管理器
   */
  ClusteredJoinMethod(int left_index_id,
                      int right_index_id,
                      double join_similarity_threshold,
                      const std::shared_ptr<ConcurrencyManager>& concurrency_manager);

  ~ClusteredJoinMethod() override = default;

  // ==================== BaseMethod 接口实现 ====================

  /**
   * @brief Eager 执行模式（单查询）
   * @param query_record 查询向量
   * @param query_slot 查询 slot (0=left, 1=right)
   * @return 匹配的候选向量
   */
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record,
      int query_slot) override;

  /**
   * @brief Lazy 执行模式（批量查询）
   * @param query_records 查询向量批次
   * @param query_slot 查询 slot
   * @return 所有匹配的候选向量
   */
  std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
      const std::deque<std::unique_ptr<VectorRecord>>& query_records,
      int query_slot) override;

  // ==================== ClusteredJoin 特有方法 ====================

  /**
   * @brief 获取方法名称
   * @return 方法名
   */
  std::string getName() const { return "ClusteredJoin"; }

  /**
   * @brief 获取配置
   * @return 配置引用
   */
  const Config& getConfig() const { return config_; }

  /**
   * @brief 获取分区器
   * @return 质心分区器
   */
  std::shared_ptr<CentroidPartitioner> getPartitioner() const {
    return partitioner_;
  }

  /**
   * @brief 训练分区器
   * @param samples 训练样本
   */
  void trainPartitioner(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 使用 VectorRecord 训练分区器
   * @param samples 样本记录
   */
  void trainPartitioner(const std::vector<const VectorRecord*>& samples);

  /**
   * @brief 检查分区器是否已训练
   * @return true 表示已训练
   */
  bool isPartitionerTrained() const;

  /**
   * @brief 触发重平衡
   */
  void rebalance();

  /**
   * @brief 获取分区统计
   * @return 分区统计信息
   */
  CentroidPartitioner::PartitionStats getPartitionStats() const;

  /**
   * @brief 增量更新分区器质心
   * @param record 新到达的向量
   */
  void updatePartitioner(const VectorRecord& record);

 private:
  Config config_;
  int left_index_id_ = -1;
  int right_index_id_ = -1;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  std::shared_ptr<CentroidPartitioner> partitioner_;

  // 训练样本缓存（用于自动训练）
  std::vector<std::vector<float>> training_buffer_;
  mutable std::mutex training_mutex_;
  bool auto_trained_ = false;

  // ==================== 内部方法 ====================

  /**
   * @brief 获取对面索引 ID
   * @param slot 当前 slot
   * @return 对面索引 ID
   */
  int otherIndexId(int slot) const {
    return (slot == 0) ? right_index_id_ : left_index_id_;
  }

  /**
   * @brief 在主分区内搜索
   * @param query 查询向量
   * @param threshold 相似度阈值
   * @param target_index_id 目标索引 ID
   * @return 候选结果
   */
  std::vector<std::shared_ptr<const VectorRecord>> searchPrimaryPartition(
      const VectorRecord& query,
      double threshold,
      int target_index_id);

  /**
   * @brief 在边界分区内搜索
   * @param query 查询向量
   * @param partitions 需要搜索的分区
   * @param threshold 相似度阈值
   * @param target_index_id 目标索引 ID
   * @return 候选结果
   */
  std::vector<std::shared_ptr<const VectorRecord>> searchBorderPartitions(
      const VectorRecord& query,
      const std::vector<int>& partitions,
      double threshold,
      int target_index_id);

  /**
   * @brief 合并并去重结果
   * @param results 结果集
   */
  void deduplicateResults(
      std::vector<std::shared_ptr<const VectorRecord>>& results);

  /**
   * @brief 尝试自动训练分区器
   * @param record 新到达的向量
   */
  void tryAutoTrain(const VectorRecord& record);

  /**
   * @brief 从 VectorRecord 提取浮点向量
   * @param record 向量记录
   * @return 浮点向量
   */
  std::vector<float> extractFloatVector(const VectorRecord& record) const;
};

}  // namespace sageFlow

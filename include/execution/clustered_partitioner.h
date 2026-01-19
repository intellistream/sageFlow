#pragma once

#include "execution/partitioner.h"
#include "execution/centroid_partitioner.h"

#include <atomic>
#include <memory>
#include <vector>

namespace sageFlow {

/**
 * @brief Clustered Join 专用两级分区器
 *
 * 支持两级分区策略：
 * 1. 第一级：向量空间分区（复用 CentroidPartitioner）- 基于质心距离
 * 2. 第二级：分区内负载均衡（支持大分区多线程并行）- 可选轮询
 *
 * 分区模式：
 * | 模式 | 配置                        | 行为                              |
 * |------|----------------------------|-----------------------------------|
 * | 1:1  | threads_per_partition = 1  | partition_id 直接映射到 subtask_id |
 * | 1:N  | threads_per_partition > 1  | 同一分区的向量轮询分发到多个 subtask |
 * | N:1  | num_partitions > parallelism | 多个分区映射到同一 subtask        |
 *
 * 边界向量多播：
 * - 当 multicast_enabled = true 时，边界向量会被复制到相邻分区
 * - 边界判定基于 overlap_ratio 参数
 */
class ClusteredPartitioner : public IPartitioner {
 public:
  /**
   * @brief 分区器配置
   */
  struct Config {
    int num_vector_partitions = 8;       ///< 向量空间分区数
    int threads_per_partition = 1;       ///< 每个分区的线程数（1:1 或 1:N）
    bool multicast_enabled = true;       ///< 是否启用边界向量多播
    double overlap_ratio = 0.1;          ///< 边界重叠比例
    int dimension = 128;                 ///< 向量维度
    int training_samples = 1000;         ///< 训练样本数
    int max_iterations = 100;            ///< k-means 最大迭代次数
    int seed = 42;                       ///< 随机种子
  };

  /**
   * @brief 构造函数
   * @param config 分区器配置
   */
  explicit ClusteredPartitioner(const Config& config);

  ~ClusteredPartitioner() override = default;

  // ==================== IPartitioner 接口实现 ====================

  /**
   * @brief 单播分区：返回向量应发送到的 subtask
   *
   * 计算逻辑：
   *   vec_partition = centroid_partitioner_.partition(data)
   *   if threads_per_partition == 1:
   *       return vec_partition
   *   else:
   *       thread_offset = round_robin_counter++ % threads_per_partition
   *       return vec_partition * threads_per_partition + thread_offset
   *
   * @param data 待分区的数据
   * @param num_channels 分区通道数
   * @return 目标分区 ID
   */
  size_t partition(const Response& data, size_t num_channels) override;

  /**
   * @brief 多播分区：返回向量应发送到的所有 subtask（包括边界复制）
   *
   * 对于边界向量，可能返回多个分区以保证 Join 正确性
   *
   * @param data 待分区的数据
   * @param num_channels 分区通道数
   * @return 目标分区 ID 列表（至少包含一个元素）
   */
  std::vector<size_t> partitionMulti(const Response& data,
                                     size_t num_channels) override;

  /**
   * @brief 是否支持多播
   * @return true 如果多播已启用
   */
  bool supportsMulticast() const override { return config_.multicast_enabled; }

  // ==================== 训练接口 ====================

  /**
   * @brief 使用样本数据训练质心
   * @param samples 训练样本（VectorRecord 指针数组）
   */
  void train(const std::vector<const VectorRecord*>& samples);

  /**
   * @brief 使用样本数据训练质心
   * @param samples 训练样本（浮点向量数组）
   */
  void train(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 检查是否已训练
   * @return true 如果质心已初始化
   */
  bool isTrained() const { return trained_; }

  // ==================== 查询接口 ====================

  /**
   * @brief 获取向量的空间分区 ID（不考虑线程映射）
   * @param record 向量记录
   * @return 向量空间分区索引 [0, num_vector_partitions)
   */
  size_t getVectorPartition(const VectorRecord& record) const;

  /**
   * @brief 获取指定向量分区对应的所有 subtask ID
   * @param vec_partition 向量空间分区索引
   * @return subtask ID 列表
   */
  std::vector<size_t> getSubtasksForPartition(size_t vec_partition) const;

  /**
   * @brief 获取总 subtask 数（所有分区 × 每分区线程数）
   * @return 总 subtask 数
   */
  size_t getTotalSubtasks() const {
    return static_cast<size_t>(config_.num_vector_partitions *
                               config_.threads_per_partition);
  }

  /**
   * @brief 获取配置
   * @return 配置引用
   */
  const Config& getConfig() const { return config_; }

  /**
   * @brief 获取内部的 CentroidPartitioner
   * @return CentroidPartitioner 引用
   */
  const CentroidPartitioner& getCentroidPartitioner() const {
    return *centroid_partitioner_;
  }

  /**
   * @brief 检查向量是否为边界向量
   * @param record 向量记录
   * @return true 如果该向量靠近分区边界
   */
  bool isBoundaryVector(const VectorRecord& record) const;

 private:
  Config config_;
  std::shared_ptr<CentroidPartitioner> centroid_partitioner_;
  std::atomic<size_t> round_robin_counter_{0};
  bool trained_ = false;

  /**
   * @brief 将向量分区 ID 映射到 subtask ID
   *
   * 支持三种映射模式：
   * - 1:1 模式：partition_id == subtask_id
   * - 1:N 模式：一个分区对应多个 subtask（轮询分发）
   * - N:1 模式：多个分区映射到同一 subtask（取模）
   *
   * @param vec_partition 向量空间分区索引
   * @param num_channels 实际的 channel 数量（可能与 num_vector_partitions 不同）
   * @return subtask ID
   */
  size_t mapPartitionToSubtask(size_t vec_partition, size_t num_channels);
};

}  // namespace sageFlow

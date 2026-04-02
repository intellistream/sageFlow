#pragma once

#include "execution/partitioner.h"
#include "operator/utils/join_strategy_config.h"

#include <memory>

namespace sageFlow {

// 前向声明
class VectorSpacePartitioner;
class LSHPartitioner;

/**
 * @brief LSH 分区器适配器
 *
 * 将 VectorSpacePartitioner 的 LSHPartitioner 包装为 IPartitioner 接口。
 * 这允许在使用 Response-based 分区系统时使用 LSH 分区策略。
 */
class LSHIPartitioner : public IPartitioner {
 public:
  /**
   * @brief 构造函数
   * @param dimension 向量维度
   * @param num_hash_functions 哈希函数数量
   * @param num_partitions 分区数量
   * @param seed 随机种子
   * @param boundary_threshold 边界判定阈值
   */
  LSHIPartitioner(int dimension, int num_hash_functions, int num_partitions,
                  int seed = 42, double boundary_threshold = 0.1);

  ~LSHIPartitioner() override = default;

  /**
   * @brief 分区函数
   * @param data Response 数据
   * @param num_channels 分区通道数
   * @return 分区索引
   */
  size_t partition(const Response& data, size_t num_channels) override;

  bool supportsMulticast() const override { return multicast_k_ > 1; }

  std::vector<size_t> partitionMulti(const Response& data,
                                     size_t num_channels) override;

  void setMulticastK(size_t multicast_k);

  void setLogicalPartitionCount(size_t num_logical_partitions);

  void setVirtualNodesPerPartition(size_t virtual_nodes_per_partition);

  int getLogicalPartitionId(const Response& data, size_t num_channels);

  std::vector<int> getMulticastLogicalPartitionIds(const Response& data,
                                                   size_t num_channels);

  [[nodiscard]] size_t getLogicalPartitionCount() const {
    return num_logical_partitions_;
  }

  [[nodiscard]] size_t getVirtualNodesPerPartition() const {
    return virtual_nodes_per_partition_;
  }

  /**
   * @brief 获取向量的所有相关分区（包含邻近分区）
   * @param data Response 数据
   * @param num_channels 分区通道数
   * @param num_probes 探测数量
   * @return 分区索引列表
   */
  std::vector<size_t> getCandidatePartitions(const Response& data,
                                              size_t num_channels,
                                              size_t num_probes = 1) const;

  /**
   * @brief 检查是否为边界向量
   * @param data Response 数据
   * @param num_channels 分区通道数
   * @return 是否为边界向量
   */
  bool isBoundaryVector(const Response& data, size_t num_channels) const;

  /**
   * @brief 获取内部 LSHPartitioner
   * @return LSHPartitioner 指针
   */
  const LSHPartitioner* getLSHPartitioner() const;

 private:
  int computeVirtualNodeIndex(uint64_t uid) const;

  std::unique_ptr<LSHPartitioner> lsh_partitioner_;
  int num_partitions_;
  size_t num_logical_partitions_ = 0;
  size_t virtual_nodes_per_partition_ = 1;
  size_t multicast_k_ = 1;
};

/**
 * @brief 分区器工厂
 *
 * 根据 JoinStrategyConfig 动态创建适当的分区器实例。
 * 支持以下分区策略：
 * - ROUND_ROBIN: 轮询分发
 * - KEY_HASH: 基于时间戳的哈希分区
 * - VECTOR_HASH: 基于向量内容的哈希分区
 * - LSH: 局部敏感哈希分区（用于 VSJoin）
 * - CENTROID: 基于质心的分区（用于 S3J/ClusteredJoin）
 */
class PartitionerFactory {
 public:
  /**
   * @brief 根据策略创建分区器
   * @param strategy 分区策略类型
   * @param dimension 向量维度
   * @param num_partitions 分区数量
   * @param config 完整配置（用于获取算法特定参数）
   * @return 分区器实例
   * @throws std::runtime_error 如果策略不支持
   */
  static std::unique_ptr<IPartitioner> create(
      PartitionStrategy strategy,
      int dimension,
      int num_partitions,
      const JoinStrategyConfig& config);

  /**
   * @brief 根据配置创建分区器（简化接口）
   * @param config JoinStrategyConfig 配置
   * @return 分区器实例
   */
  static std::unique_ptr<IPartitioner> create(const JoinStrategyConfig& config);

  /**
   * @brief 获取策略推荐的默认分区数
   * @param strategy 分区策略类型
   * @param parallelism 并行度
   * @return 推荐分区数
   */
  static int getRecommendedPartitionCount(PartitionStrategy strategy,
                                          int parallelism);

  /**
   * @brief 检查策略是否需要训练
   * @param strategy 分区策略类型
   * @return 是否需要训练
   */
  static bool requiresTraining(PartitionStrategy strategy);

  /**
   * @brief 获取策略描述
   * @param strategy 分区策略类型
   * @return 策略描述字符串
   */
  static std::string getDescription(PartitionStrategy strategy);
};

}  // namespace sageFlow

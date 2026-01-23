#pragma once

#include "common/data_types.h"

#include <cstdint>
#include <random>
#include <vector>
#include <mutex>
#include <atomic>
#include <memory>

namespace sageFlow {

/**
 * @brief 向量空间分区器基类
 *
 * 提供基于向量内容的分区策略，确保相似向量大概率被分配到同一分区。
 * 主要用于 VSJoin 等需要向量局部性的操作。
 */
class VectorSpacePartitioner {
 public:
  virtual ~VectorSpacePartitioner() = default;

  /**
   * @brief 计算向量所属分区
   * @param record 向量记录
   * @param num_partitions 分区总数
   * @return 分区ID
   */
  virtual size_t partition(const VectorRecord& record, size_t num_partitions) = 0;

  /**
   * @brief 获取查询时需要检查的候选分区（包含邻近分区）
   * @param query 查询向量
   * @param num_partitions 分区总数
   * @param num_probes 探测数量（1=仅主分区）
   * @return 候选分区列表
   */
  virtual std::vector<size_t> getCandidatePartitions(const VectorRecord& query, size_t num_partitions,
                                                     size_t num_probes = 1) = 0;

  /**
   * @brief 判断向量是否靠近分区边界
   * @param record 向量记录
   * @param num_partitions 分区总数
   * @return 是否为边界向量
   */
  virtual bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) = 0;
};

/**
 * @brief 基于局部敏感哈希的分区器
 *
 * 使用随机超平面将向量空间划分，相似向量有高概率获得相同哈希码。
 * 适用于欧氏距离和角距离场景。
 *
 * 算法原理：
 * 1. 预生成 num_hash_functions 个随机超平面（单位向量）
 * 2. 对于输入向量，计算其与每个超平面的点积
 * 3. 点积 > 0 时对应位为 1，否则为 0，组合成哈希码
 * 4. 相似向量有高概率获得相同的哈希码
 */
class LSHPartitioner : public VectorSpacePartitioner {
 public:
  /**
   * @brief 构造函数
   * @param dimension 向量维度
   * @param num_hash_functions 哈希函数数量（影响分区粒度，最大64）
   * @param seed 随机种子
   * @param boundary_threshold 边界判定阈值（与超平面距离的比例）
   */
  LSHPartitioner(int dimension, int num_hash_functions = 8, int seed = 42, double boundary_threshold = 0.1);

  size_t partition(const VectorRecord& record, size_t num_partitions) override;

  std::vector<size_t> getCandidatePartitions(const VectorRecord& query, size_t num_partitions,
                                             size_t num_probes = 1) override;

  bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;

  /**
   * @brief 获取向量的原始 LSH 哈希码（用于调试）
   * @param record 向量记录
   * @return 64位哈希码
   */
  uint64_t getHashCode(const VectorRecord& record) const;

  /**
   * @brief 获取向量维度
   */
  int getDimension() const { return dimension_; }

  /**
   * @brief 获取哈希函数数量
   */
  int getNumHashFunctions() const { return num_hash_functions_; }

 private:
  int dimension_;
  int num_hash_functions_;
  double boundary_threshold_;

  // 随机投影向量 (num_hash_functions x dimension)
  std::vector<std::vector<float>> random_projections_;

  /**
   * @brief 计算 LSH 哈希码
   * @param record 向量记录
   * @return 二进制哈希码
   */
  uint64_t computeHashCode(const VectorRecord& record) const;

  /**
   * @brief 计算向量到各超平面的有符号距离
   * @param record 向量记录
   * @return 各超平面的距离（正=超平面一侧，负=另一侧）
   */
  std::vector<float> computeDistancesToHyperplanes(const VectorRecord& record) const;

  /**
   * @brief 初始化随机投影向量
   * @param seed 随机种子
   */
  void initRandomProjections(int seed);

  /**
   * @brief 计算向量的模长
   * @param record 向量记录
   * @return 向量模长
   */
  float computeVectorNorm(const VectorRecord& record) const;
};

/**
 * @brief 基于 K-Means 的分区器（备选方案）
 *
 * 使用 K-Means 聚类将向量空间划分为 k 个分区。
 * 适用于数据分布相对稳定的场景。
 */
class KMeansPartitioner : public VectorSpacePartitioner {
 public:
  /**
   * @brief 构造函数
   * @param dimension 向量维度
   * @param num_clusters 聚类数量
   * @param seed 随机种子
   * @param enable_cold_start 是否启用冷启动（默认 false，保持向后兼容）
   * @param cold_start_samples 冷启动所需的样本数量
   */
  KMeansPartitioner(int dimension, int num_clusters, int seed = 42,
                    bool enable_cold_start = false, size_t cold_start_samples = 300);

  /**
   * @brief 使用样本数据初始化质心
   * @param samples 样本向量
   * @param max_iterations 最大迭代次数
   */
  void initCentroids(const std::vector<const VectorRecord*>& samples, int max_iterations = 100);

  /**
   * @brief 在线更新质心（增量 K-Means）
   * @param record 新向量
   * @param learning_rate 学习率
   */
  void updateCentroids(const VectorRecord& record, double learning_rate = 0.01);

  size_t partition(const VectorRecord& record, size_t num_partitions) override;

  std::vector<size_t> getCandidatePartitions(const VectorRecord& query, size_t num_partitions,
                                             size_t num_probes = 1) override;

  bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;

  /**
   * @brief 检查质心是否已初始化
   */
  bool isInitialized() const { return centroids_initialized_; }

  /**
   * @brief 获取聚类数量
   */
  int getNumClusters() const { return num_clusters_; }

  /**
   * @brief 检查是否处于冷启动阶段
   * @return true 如果正在收集样本或尚未训练完成
   */
  bool isInColdStart() const { return enable_cold_start_ && !centroids_initialized_; }

  /**
   * @brief 收集冷启动样本
   * @param record 向量记录
   * @return true 如果训练被触发
   */
  bool collectSample(const VectorRecord& record);

  /**
   * @brief 获取冷启动进度
   * @return {当前样本数, 目标样本数}
   */
  std::pair<size_t, size_t> getColdStartProgress() const;

 private:
  int dimension_;
  int num_clusters_;
  int seed_;
  bool centroids_initialized_;
  std::vector<std::vector<float>> centroids_;
  std::vector<size_t> cluster_counts_;  // 用于在线更新时的加权

  // 冷启动相关成员
  bool enable_cold_start_;
  size_t cold_start_samples_;
  std::vector<std::unique_ptr<VectorRecord>> training_buffer_;
  mutable std::mutex cold_start_mutex_;
  std::atomic<bool> training_triggered_{false};

  /**
   * @brief 找到最近的质心
   * @param record 向量记录
   * @return 最近质心的索引
   */
  size_t findNearestCentroid(const VectorRecord& record) const;

  /**
   * @brief 计算向量到质心的距离
   * @param record 向量记录
   * @param centroid_idx 质心索引
   * @return 欧氏距离
   */
  float computeDistanceToCentroid(const VectorRecord& record, size_t centroid_idx) const;

  /**
   * @brief 提取向量的浮点数据
   * @param record 向量记录
   * @return 浮点向量
   */
  std::vector<float> extractFloatVector(const VectorRecord& record) const;

  /**
   * @brief 触发冷启动训练
   */
  void triggerColdStartTraining();
};

}  // namespace sageFlow

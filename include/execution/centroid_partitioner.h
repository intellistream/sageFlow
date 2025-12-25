#pragma once

#include "common/data_types.h"
#include "execution/partitioner.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <vector>

namespace sageFlow {

/**
 * @brief 质心分区器
 *
 * 基于 k-means 聚类的向量空间分区策略。
 * 用于 ClusteredJoin 实现，确保相似向量被分配到同一或邻近分区。
 *
 * 特性：
 * - K-Means++ 初始化算法
 * - 支持边界向量检测和多分区查询
 * - 增量质心更新（在线学习）
 * - 分区负载均衡检测
 * - 线程安全
 */
class CentroidPartitioner : public IPartitioner {
 public:
  /**
   * @brief 配置结构
   */
  struct Config {
    int num_partitions = 16;          ///< 分区（聚类）数量
    double overlap_ratio = 0.1;       ///< 边界重叠比例，用于判定边界向量
    int max_iterations = 100;         ///< k-means 最大迭代次数
    std::string init_method = "kmeans++";  ///< 初始化方法：kmeans++ 或 random
    double rebalance_threshold = 0.3; ///< 触发重平衡的不均衡阈值
    int seed = 42;                    ///< 随机种子
    int dimension = 128;              ///< 向量维度
  };

  /**
   * @brief 分区统计信息
   */
  struct PartitionStats {
    std::vector<size_t> sizes;  ///< 各分区的大小
    double balance_score;       ///< 均衡得分 [0, 1]，1 表示完美均衡
  };

  /**
   * @brief 构造函数
   * @param config 分区器配置
   */
  explicit CentroidPartitioner(const Config& config);

  ~CentroidPartitioner() override = default;

  // ==================== 训练与分区 ====================

  /**
   * @brief 使用样本数据训练质心
   * @param samples 训练样本向量
   */
  void train(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 使用 VectorRecord 样本训练质心
   * @param samples VectorRecord 指针数组
   */
  void train(const std::vector<const VectorRecord*>& samples);

  /**
   * @brief 检查是否已训练
   * @return true 表示质心已初始化
   */
  bool isTrained() const { return trained_.load(); }

  /**
   * @brief 获取向量的主分区
   * @param record 向量记录
   * @return 分区索引
   */
  int getPrimaryPartition(const VectorRecord& record) const;

  /**
   * @brief 获取向量的所有相关分区（包含边界分区）
   * @param record 向量记录
   * @return 分区索引列表
   */
  std::vector<int> getPartitions(const VectorRecord& record) const;

  // ==================== IPartitioner 接口实现 ====================

  /**
   * @brief IPartitioner 接口实现
   * @param data Response 数据
   * @param num_channels 分区通道数
   * @return 分区索引
   */
  size_t partition(const Response& data, size_t num_channels) override;

  // ==================== 多播支持（Clustered Join） ====================

  /**
   * @brief 启用/禁用多播模式
   * 
   * 启用后，边界向量将被复制到多个分区以保证 Join 召回率。
   * 
   * @param enable true 启用边界向量多播
   */
  void setMulticastEnabled(bool enable) { multicast_enabled_ = enable; }

  /**
   * @brief 检查多播是否启用
   * @return true 表示多播已启用
   */
  bool isMulticastEnabled() const { return multicast_enabled_; }

  /**
   * @brief 检查是否支持多播
   * @return true 表示支持 partitionMulti()
   */
  bool supportsMulticast() const override { return multicast_enabled_; }

  /**
   * @brief 多播分区实现
   * 
   * 对于边界向量，返回主分区 + 所有边界分区。
   * 对于非边界向量，仅返回主分区。
   * 
   * @param data Response 数据
   * @param num_channels 分区通道数
   * @return 目标分区 ID 列表
   */
  std::vector<size_t> partitionMulti(const Response& data, size_t num_channels) override;

  // ==================== 质心管理 ====================

  /**
   * @brief 获取所有质心
   * @return 质心向量数组
   */
  const std::vector<std::vector<float>>& getCentroids() const;

  /**
   * @brief 增量更新质心（在线学习）
   * @param new_samples 新的样本向量
   * @param learning_rate 学习率
   */
  void updateCentroids(const std::vector<std::vector<float>>& new_samples,
                       double learning_rate = 0.01);

  /**
   * @brief 使用单个向量增量更新最近质心
   * @param vec 新向量
   * @param learning_rate 学习率
   */
  void updateCentroidsIncremental(const std::vector<float>& vec,
                                  double learning_rate = 0.01);

  // ==================== 负载均衡 ====================

  /**
   * @brief 检查是否需要重平衡
   * @param partition_sizes 各分区当前大小
   * @return true 表示需要重平衡
   */
  bool needsRebalance(const std::vector<size_t>& partition_sizes) const;

  /**
   * @brief 获取分区统计信息
   * @return 分区统计
   */
  PartitionStats getStats() const;

  /**
   * @brief 更新分区大小统计（线程安全）
   * @param partition_idx 分区索引
   * @param delta 变化量（正数增加，负数减少）
   */
  void updatePartitionSize(int partition_idx, int delta);

  /**
   * @brief 重置分区大小统计
   */
  void resetPartitionSizes();

  // ==================== 边界处理 ====================

  /**
   * @brief 检查向量是否为边界向量
   * @param record 向量记录
   * @return true 表示该向量靠近分区边界
   */
  bool isBoundaryVector(const VectorRecord& record) const;

  /**
   * @brief 获取边界向量需要检查的额外分区
   * @param record 边界向量
   * @return 需要额外检查的分区索引列表
   */
  std::vector<int> getBorderPartitions(const VectorRecord& record) const;

  // ==================== 配置访问 ====================

  /**
   * @brief 获取配置
   * @return 配置引用
   */
  const Config& getConfig() const { return config_; }

  /**
   * @brief 获取分区数量
   * @return 分区数
   */
  int getNumPartitions() const { return config_.num_partitions; }

  /**
   * @brief 获取向量维度
   * @return 维度
   */
  int getDimension() const { return config_.dimension; }

 private:
  Config config_;
  std::vector<std::vector<float>> centroids_;
  std::atomic<bool> trained_{false};
  mutable std::shared_mutex mutex_;
  
  // 分区大小统计（用于负载均衡检测）
  std::vector<std::atomic<size_t>> partition_sizes_;

  // 多播模式开关（默认禁用）
  bool multicast_enabled_ = false;

  // ==================== 内部方法 ====================

  /**
   * @brief K-Means++ 初始化质心
   * @param samples 样本数据
   */
  void initKMeansPlusPlus(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 随机初始化质心
   * @param samples 样本数据
   */
  void initRandom(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 执行 K-Means 迭代
   * @param samples 样本数据
   * @return 是否收敛
   */
  bool runKMeansIteration(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 计算向量到所有质心的距离
   * @param vec 向量数据
   * @return 距离数组
   */
  std::vector<float> computeDistances(const std::vector<float>& vec) const;

  /**
   * @brief 计算向量到指定质心的距离
   * @param vec 向量数据
   * @param centroid_idx 质心索引
   * @return 欧氏距离
   */
  float computeDistanceToCentroid(const std::vector<float>& vec, int centroid_idx) const;

  /**
   * @brief 找到最近的质心
   * @param vec 向量数据
   * @return 最近质心索引
   */
  int findNearestCentroid(const std::vector<float>& vec) const;

  /**
   * @brief 找到最近的 k 个质心
   * @param vec 向量数据
   * @param k 数量
   * @return 最近质心索引列表（按距离排序）
   */
  std::vector<int> findNearestKCentroids(const std::vector<float>& vec, int k) const;

  /**
   * @brief 从 VectorRecord 提取浮点向量
   * @param record 向量记录
   * @return 浮点向量
   */
  std::vector<float> extractFloatVector(const VectorRecord& record) const;

  /**
   * @brief 计算均衡得分
   * @param sizes 各分区大小
   * @return 均衡得分 [0, 1]
   */
  double computeBalanceScore(const std::vector<size_t>& sizes) const;
};

}  // namespace sageFlow

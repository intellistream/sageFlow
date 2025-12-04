#pragma once

#include "execution/vector_space_partitioner.h"
#include "index/index.h"
#include "index/ivf.h"

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace sageFlow {

/**
 * @brief 分区索引
 *
 * 将向量空间分区，每个分区维护独立的 IVF 索引。
 * 支持分区级别的并发操作，减少全局锁竞争。
 *
 * 设计特点：
 * 1. 继承 Index 基类，与 HNSW、IVF、BruteForce 同级
 * 2. 内部管理多个 IVF 子索引，每个分区一个
 * 3. 使用 VectorSpacePartitioner 确定向量所属分区
 * 4. 支持跨分区查询以提高召回率
 */
class PartitionedIndex : public Index {
 public:
  /**
   * @brief 构造函数
   * @param num_partitions 分区数量
   * @param dimension 向量维度
   * @param partitioner 向量空间分区器
   * @param nlist 每个分区 IVF 的聚类数
   * @param nprobes 查询时探测的聚类数
   */
  PartitionedIndex(size_t num_partitions, int dimension, std::shared_ptr<VectorSpacePartitioner> partitioner,
                   int nlist = 100, int nprobes = 10);

  /**
   * @brief 析构函数
   */
  ~PartitionedIndex() override = default;

  // ==========================================================================
  // Index 接口实现
  // ==========================================================================

  /**
   * @brief 插入向量（通过 UID 从 StorageManager 获取数据）
   * @param id 向量 UID
   * @return 成功返回 true，失败返回 false
   */
  bool insert(uint64_t id) override;

  /**
   * @brief 删除向量
   * @param id 向量 UID
   * @return 成功返回 true，失败返回 false
   */
  bool erase(uint64_t id) override;

  /**
   * @brief 查询 Top-K 近邻
   * @param record 查询向量
   * @param k 返回数量
   * @return 匹配的向量 UID 列表
   */
  std::vector<uint64_t> query(const VectorRecord& record, int k) override;

  /**
   * @brief 用于 Join 的阈值查询
   * @param record 查询向量
   * @param join_similarity_threshold 相似度阈值
   * @return 满足阈值的向量 UID 列表
   */
  std::vector<uint64_t> query_for_join(const VectorRecord& record, double join_similarity_threshold) override;

  // ==========================================================================
  // 分区特定操作
  // ==========================================================================

  /**
   * @brief 直接插入到指定分区
   * @param partition_id 分区 ID
   * @param uid 向量 UID
   * @return 成功返回 true，失败返回 false
   */
  bool insertToPartition(size_t partition_id, uint64_t uid);

  /**
   * @brief 查询指定分区
   * @param partition_id 分区 ID
   * @param query 查询向量
   * @param k 返回数量
   * @return 查询结果 UID 列表
   */
  std::vector<uint64_t> queryPartition(size_t partition_id, const VectorRecord& query, int k);

  /**
   * @brief 查询指定分区（用于 Join 的阈值查询）
   * @param partition_id 分区 ID
   * @param query 查询向量
   * @param threshold 相似度阈值
   * @return 满足阈值的向量 UID 列表
   */
  std::vector<uint64_t> queryPartitionForJoin(size_t partition_id, const VectorRecord& query, double threshold);

  /**
   * @brief 跨分区查询
   * @param query 查询向量
   * @param k 返回数量
   * @param num_probes 探测分区数
   * @return 合并去重的结果 UID 列表
   */
  std::vector<uint64_t> queryMultiPartition(const VectorRecord& query, int k, size_t num_probes = 2);

  /**
   * @brief 跨分区查询（用于 Join 的阈值查询）
   * @param query 查询向量
   * @param threshold 相似度阈值
   * @param num_probes 探测分区数
   * @return 满足阈值的向量 UID 列表
   */
  std::vector<uint64_t> queryMultiPartitionForJoin(const VectorRecord& query, double threshold, size_t num_probes = 2);

  // ==========================================================================
  // 状态查询
  // ==========================================================================

  /**
   * @brief 获取分区数量
   */
  size_t getNumPartitions() const { return num_partitions_; }

  /**
   * @brief 获取向量维度
   */
  int getDimension() const { return dimension_; }

  /**
   * @brief 获取指定分区的大小
   * @param partition_id 分区 ID
   * @return 分区中的向量数量
   */
  size_t getPartitionSize(size_t partition_id) const;

  /**
   * @brief 获取所有分区的大小统计
   * @return 每个分区的向量数量
   */
  std::vector<size_t> getPartitionSizes() const;

  /**
   * @brief 获取总向量数量
   * @return 所有分区的向量总数
   */
  size_t getTotalSize() const;

  /**
   * @brief 计算负载均衡度（最大分区/平均分区）
   * @return 负载均衡比率，1.0 表示完美均衡
   */
  double getLoadImbalance() const;

  /**
   * @brief 获取向量所属的分区 ID
   * @param uid 向量 UID
   * @return 分区 ID，如果不存在返回 std::nullopt
   */
  std::optional<size_t> getPartitionForUid(uint64_t uid) const;

 private:
  size_t num_partitions_;
  int dimension_;
  std::shared_ptr<VectorSpacePartitioner> partitioner_;

  // 每个分区的 IVF 索引（直接持有，不通过 ConcurrencyManager）
  std::vector<std::unique_ptr<Ivf>> partition_indexes_;

  // 每个分区独立的互斥锁（用于分区级别的并发控制）
  std::unique_ptr<std::shared_mutex[]> partition_mutexes_;

  // uid -> partition_id 映射，用于删除时定位分区
  std::unordered_map<uint64_t, size_t> uid_partition_map_;
  mutable std::shared_mutex map_mutex_;

  // 每个分区的大小计数
  std::unique_ptr<std::atomic<size_t>[]> partition_sizes_;

  /**
   * @brief 获取向量所属分区（通过分区器计算）
   * @param record 向量记录
   * @return 分区 ID
   */
  size_t computePartition(const VectorRecord& record) const;

  /**
   * @brief 合并多个分区的查询结果
   * @param results_per_partition 每个分区的查询结果
   * @param query 查询向量（用于距离重计算排序）
   * @param k 返回数量
   * @return 合并去重后的 top-k 结果
   */
  std::vector<uint64_t> mergeResults(const std::vector<std::vector<uint64_t>>& results_per_partition,
                                      const VectorRecord& query, int k);

  /**
   * @brief 合并多个分区的 Join 查询结果（去重）
   * @param results_per_partition 每个分区的查询结果
   * @return 去重后的结果
   */
  std::vector<uint64_t> mergeJoinResults(const std::vector<std::vector<uint64_t>>& results_per_partition);
};

}  // namespace sageFlow

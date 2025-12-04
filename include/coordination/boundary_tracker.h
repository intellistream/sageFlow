#pragma once

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace sageFlow {

/**
 * @brief 边界向量追踪器
 *
 * 追踪靠近分区边界的向量，用于跨分区查询时的额外检查。
 * 线程安全，支持高并发读取。
 */
class BoundaryTracker {
 public:
  BoundaryTracker() = default;

  /**
   * @brief 标记向量为边界向量
   * @param vector_uid 向量唯一ID
   * @param partition_id 所属分区ID
   */
  void markAsBoundary(uint64_t vector_uid, size_t partition_id);

  /**
   * @brief 取消边界标记
   * @param vector_uid 向量唯一ID
   */
  void unmark(uint64_t vector_uid);

  /**
   * @brief 批量取消边界标记
   * @param vector_uids 向量ID列表
   */
  void unmarkBatch(const std::vector<uint64_t>& vector_uids);

  /**
   * @brief 检查是否为边界向量
   * @param vector_uid 向量唯一ID
   * @return 是否为边界向量
   */
  [[nodiscard]] auto isBoundaryVector(uint64_t vector_uid) const -> bool;

  /**
   * @brief 获取特定分区的所有边界向量 UID
   * @param partition_id 分区ID
   * @return 边界向量UID列表
   */
  [[nodiscard]] auto getBoundaryVectorsForPartition(size_t partition_id) const -> std::vector<uint64_t>;

  /**
   * @brief 获取向量所属分区（仅对边界向量有效）
   * @param vector_uid 向量唯一ID
   * @return 分区ID，如果不是边界向量返回 -1
   */
  [[nodiscard]] auto getPartition(uint64_t vector_uid) const -> int64_t;

  /**
   * @brief 获取边界向量总数
   * @return 边界向量总数
   */
  [[nodiscard]] auto size() const -> size_t;

  /**
   * @brief 获取各分区边界向量数量
   * @return 分区ID到边界向量数量的映射
   */
  [[nodiscard]] auto getPartitionStats() const -> std::unordered_map<size_t, size_t>;

  /**
   * @brief 清空所有记录
   */
  void clear();

 private:
  /// uid -> partition_id
  std::unordered_map<uint64_t, size_t> boundary_vectors_;

  /// partition_id -> set of uids (用于快速获取分区边界向量)
  std::unordered_map<size_t, std::unordered_set<uint64_t>> partition_boundaries_;

  mutable std::shared_mutex mutex_;
};

}  // namespace sageFlow

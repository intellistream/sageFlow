#include "coordination/boundary_tracker.h"

#include <mutex>

namespace sageFlow {

void BoundaryTracker::markAsBoundary(uint64_t vector_uid, size_t partition_id) {
  std::unique_lock lock(mutex_);

  // [TODO-S3J] 验证 2*t 规则
  // 确保调用此函数的地方（通常在 Partitioner 或 State 中），
  // 使用的判定公式是 dist(r, c_j) <= dist(r, c_i) + 2*t。
  // 这里的逻辑本身是通用的，只需确认调用源的判定条件正确。

  // 检查是否已存在，如果在不同分区则先移除旧记录
  auto it = boundary_vectors_.find(vector_uid);
  if (it != boundary_vectors_.end()) {
    size_t old_partition = it->second;
    if (old_partition != partition_id) {
      partition_boundaries_[old_partition].erase(vector_uid);
    }
  }

  // 更新映射
  boundary_vectors_[vector_uid] = partition_id;
  partition_boundaries_[partition_id].insert(vector_uid);
}

void BoundaryTracker::unmark(uint64_t vector_uid) {
  std::unique_lock lock(mutex_);

  auto it = boundary_vectors_.find(vector_uid);
  if (it != boundary_vectors_.end()) {
    size_t partition_id = it->second;
    partition_boundaries_[partition_id].erase(vector_uid);
    boundary_vectors_.erase(it);

    // 如果分区为空，可选择清理
    if (partition_boundaries_[partition_id].empty()) {
      partition_boundaries_.erase(partition_id);
    }
  }
}

void BoundaryTracker::unmarkBatch(const std::vector<uint64_t>& vector_uids) {
  std::unique_lock lock(mutex_);

  for (uint64_t uid : vector_uids) {
    auto it = boundary_vectors_.find(uid);
    if (it != boundary_vectors_.end()) {
      size_t partition_id = it->second;
      partition_boundaries_[partition_id].erase(uid);
      boundary_vectors_.erase(it);
    }
  }

  // 清理空分区
  for (auto it = partition_boundaries_.begin(); it != partition_boundaries_.end();) {
    if (it->second.empty()) {
      it = partition_boundaries_.erase(it);
    } else {
      ++it;
    }
  }
}

auto BoundaryTracker::isBoundaryVector(uint64_t vector_uid) const -> bool {
  std::shared_lock lock(mutex_);
  return boundary_vectors_.find(vector_uid) != boundary_vectors_.end();
}

auto BoundaryTracker::getBoundaryVectorsForPartition(size_t partition_id) const -> std::vector<uint64_t> {
  std::shared_lock lock(mutex_);

  auto it = partition_boundaries_.find(partition_id);
  if (it == partition_boundaries_.end()) {
    return {};
  }

  // 返回副本以避免锁持有时间过长
  return {it->second.begin(), it->second.end()};
}

auto BoundaryTracker::getPartition(uint64_t vector_uid) const -> int64_t {
  std::shared_lock lock(mutex_);

  auto it = boundary_vectors_.find(vector_uid);
  if (it == boundary_vectors_.end()) {
    return -1;
  }
  return static_cast<int64_t>(it->second);
}

auto BoundaryTracker::size() const -> size_t {
  std::shared_lock lock(mutex_);
  return boundary_vectors_.size();
}

auto BoundaryTracker::getPartitionStats() const -> std::unordered_map<size_t, size_t> {
  std::shared_lock lock(mutex_);

  std::unordered_map<size_t, size_t> stats;
  for (const auto& [partition_id, uids] : partition_boundaries_) {
    stats[partition_id] = uids.size();
  }
  return stats;
}

void BoundaryTracker::clear() {
  std::unique_lock lock(mutex_);
  boundary_vectors_.clear();
  partition_boundaries_.clear();
}

}  // namespace sageFlow

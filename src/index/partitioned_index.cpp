#include "index/partitioned_index.h"

#include "utils/logger.h"

#include <algorithm>
#include <unordered_set>

namespace sageFlow {

PartitionedIndex::PartitionedIndex(size_t num_partitions, int dimension,
                                   std::shared_ptr<VectorSpacePartitioner> partitioner, int nlist, int nprobes)
    : num_partitions_(num_partitions), dimension_(dimension), partitioner_(std::move(partitioner)) {
  // 参数校验
  if (num_partitions_ == 0) {
    throw std::invalid_argument("PartitionedIndex: num_partitions must be > 0");
  }
  if (dimension_ <= 0) {
    throw std::invalid_argument("PartitionedIndex: dimension must be > 0");
  }
  if (!partitioner_) {
    throw std::invalid_argument("PartitionedIndex: partitioner cannot be null");
  }

  // 设置 Index 基类属性
  Index::dimension_ = dimension;
  index_type_ = IndexType::IVF;  // 标记为 IVF 类型（内部使用 IVF）

  // 初始化分区互斥锁（使用 unique_ptr 数组避免 shared_mutex 不可复制问题）
  partition_mutexes_ = std::make_unique<std::shared_mutex[]>(num_partitions_);

  // 初始化分区大小计数器
  partition_sizes_ = std::make_unique<std::atomic<size_t>[]>(num_partitions_);
  for (size_t i = 0; i < num_partitions_; ++i) {
    partition_sizes_[i].store(0);
  }

  // 创建每个分区的 IVF 索引
  partition_indexes_.reserve(num_partitions_);
  for (size_t i = 0; i < num_partitions_; ++i) {
    auto ivf = std::make_unique<Ivf>(nlist, 1.5, nprobes);
    // 共享 StorageManager（如果已设置）
    if (storage_manager_) {
      ivf->storage_manager_ = storage_manager_;
    }
    partition_indexes_.push_back(std::move(ivf));
  }

  SAGEFLOW_LOG_INFO("INDEX", "PartitionedIndex created: partitions={}, dim={}, nlist={}, nprobes={}", num_partitions_,
                    dimension, nlist, nprobes);
}

size_t PartitionedIndex::computePartition(const VectorRecord& record) const {
  return partitioner_->partition(record, num_partitions_);
}

bool PartitionedIndex::insert(uint64_t id) {
  // 从 StorageManager 获取记录
  if (!storage_manager_) {
    SAGEFLOW_LOG_ERROR("INDEX", "PartitionedIndex::insert - storage_manager is null");
    return false;
  }

  auto record = storage_manager_->getVectorByUid(id);
  if (!record) {
    SAGEFLOW_LOG_ERROR("INDEX", "PartitionedIndex::insert - record not found for uid: {}", id);
    return false;
  }

  // 计算分区
  size_t partition_id = computePartition(*record);

  return insertToPartition(partition_id, id);
}

bool PartitionedIndex::insertToPartition(size_t partition_id, uint64_t uid) {
  if (partition_id >= num_partitions_) {
    SAGEFLOW_LOG_ERROR("INDEX", "PartitionedIndex::insertToPartition - invalid partition_id: {} >= {}", partition_id,
                       num_partitions_);
    return false;
  }

  // 获取分区锁
  {
    std::unique_lock<std::shared_mutex> lock(partition_mutexes_[partition_id]);

    // 确保子索引共享 StorageManager
    if (storage_manager_ && !partition_indexes_[partition_id]->storage_manager_) {
      partition_indexes_[partition_id]->storage_manager_ = storage_manager_;
    }

    // 插入到分区索引
    if (!partition_indexes_[partition_id]->insert(uid)) {
      return false;
    }
  }

  // 更新 uid -> partition 映射
  {
    std::unique_lock<std::shared_mutex> map_lock(map_mutex_);
    uid_partition_map_[uid] = partition_id;
  }

  // 更新分区大小
  partition_sizes_[partition_id].fetch_add(1, std::memory_order_relaxed);

  return true;
}

bool PartitionedIndex::erase(uint64_t id) {
  // 查找 uid 所在的分区
  size_t partition_id;
  {
    std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
    auto it = uid_partition_map_.find(id);
    if (it == uid_partition_map_.end()) {
      SAGEFLOW_LOG_WARN("INDEX", "PartitionedIndex::erase - uid {} not found in partition map", id);
      return false;
    }
    partition_id = it->second;
  }

  // 从分区索引删除
  {
    std::unique_lock<std::shared_mutex> lock(partition_mutexes_[partition_id]);
    if (!partition_indexes_[partition_id]->erase(id)) {
      return false;
    }
  }

  // 从映射中删除
  {
    std::unique_lock<std::shared_mutex> map_lock(map_mutex_);
    uid_partition_map_.erase(id);
  }

  // 更新分区大小
  partition_sizes_[partition_id].fetch_sub(1, std::memory_order_relaxed);

  return true;
}

std::vector<uint64_t> PartitionedIndex::query(const VectorRecord& record, int k) {
  // 默认查询所有分区并合并结果
  return queryMultiPartition(record, k, num_partitions_);
}

std::vector<uint64_t> PartitionedIndex::query_for_join(const VectorRecord& record, double join_similarity_threshold) {
  // 默认查询所有分区并合并结果
  return queryMultiPartitionForJoin(record, join_similarity_threshold, num_partitions_);
}

std::vector<uint64_t> PartitionedIndex::queryPartition(size_t partition_id, const VectorRecord& query, int k) {
  if (partition_id >= num_partitions_) {
    SAGEFLOW_LOG_WARN("INDEX", "PartitionedIndex::queryPartition - invalid partition_id: {}", partition_id);
    return {};
  }

  std::shared_lock<std::shared_mutex> lock(partition_mutexes_[partition_id]);
  return partition_indexes_[partition_id]->query(query, k);
}

std::vector<uint64_t> PartitionedIndex::queryPartitionForJoin(size_t partition_id, const VectorRecord& query,
                                                               double threshold) {
  if (partition_id >= num_partitions_) {
    SAGEFLOW_LOG_WARN("INDEX", "PartitionedIndex::queryPartitionForJoin - invalid partition_id: {}", partition_id);
    return {};
  }

  std::shared_lock<std::shared_mutex> lock(partition_mutexes_[partition_id]);
  return partition_indexes_[partition_id]->query_for_join(query, threshold);
}

std::vector<uint64_t> PartitionedIndex::queryMultiPartition(const VectorRecord& query, int k, size_t num_probes) {
  // 限制探测分区数
  num_probes = std::min(num_probes, num_partitions_);

  // 获取分区器推荐的分区顺序
  std::vector<size_t> probe_order = partitioner_->getCandidatePartitions(query, num_partitions_, num_probes);

  // 查询每个分区
  std::vector<std::vector<uint64_t>> results_per_partition;
  results_per_partition.reserve(probe_order.size());

  for (size_t partition_id : probe_order) {
    auto results = queryPartition(partition_id, query, k);
    if (!results.empty()) {
      results_per_partition.push_back(std::move(results));
    }
  }

  // 合并结果
  return mergeResults(results_per_partition, query, k);
}

std::vector<uint64_t> PartitionedIndex::queryMultiPartitionForJoin(const VectorRecord& query, double threshold,
                                                                    size_t num_probes) {
  // 限制探测分区数
  num_probes = std::min(num_probes, num_partitions_);

  // 获取分区器推荐的分区顺序
  std::vector<size_t> probe_order = partitioner_->getCandidatePartitions(query, num_partitions_, num_probes);

  // 查询每个分区
  std::vector<std::vector<uint64_t>> results_per_partition;
  results_per_partition.reserve(probe_order.size());

  for (size_t partition_id : probe_order) {
    auto results = queryPartitionForJoin(partition_id, query, threshold);
    if (!results.empty()) {
      results_per_partition.push_back(std::move(results));
    }
  }

  // 合并结果（去重）
  return mergeJoinResults(results_per_partition);
}

std::vector<uint64_t> PartitionedIndex::mergeResults(const std::vector<std::vector<uint64_t>>& results_per_partition,
                                                      const VectorRecord& query, int k) {
  if (results_per_partition.empty()) {
    return {};
  }

  // 合并所有结果并去重
  std::unordered_set<uint64_t> seen;
  std::vector<std::pair<uint64_t, double>> candidates;

  ComputeEngine compute_engine;

  for (const auto& partition_results : results_per_partition) {
    for (uint64_t uid : partition_results) {
      if (seen.insert(uid).second) {
        // 从 StorageManager 获取记录计算距离
        if (storage_manager_) {
          auto record = storage_manager_->getVectorByUid(uid);
          if (record) {
            double distance = compute_engine.EuclideanDistance(query.data_, record->data_);
            candidates.emplace_back(uid, distance);
          }
        } else {
          // 如果没有 StorageManager，假设结果已经排序，使用顺序作为优先级
          candidates.emplace_back(uid, static_cast<double>(candidates.size()));
        }
      }
    }
  }

  // 按距离排序
  std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) { return a.second < b.second; });

  // 取 top-k
  std::vector<uint64_t> results;
  results.reserve(std::min(static_cast<size_t>(k), candidates.size()));
  for (size_t i = 0; i < candidates.size() && static_cast<int>(i) < k; ++i) {
    results.push_back(candidates[i].first);
  }

  return results;
}

std::vector<uint64_t> PartitionedIndex::mergeJoinResults(
    const std::vector<std::vector<uint64_t>>& results_per_partition) {
  if (results_per_partition.empty()) {
    return {};
  }

  // 使用 unordered_set 去重
  std::unordered_set<uint64_t> unique_results;
  for (const auto& partition_results : results_per_partition) {
    for (uint64_t uid : partition_results) {
      unique_results.insert(uid);
    }
  }

  return std::vector<uint64_t>(unique_results.begin(), unique_results.end());
}

size_t PartitionedIndex::getPartitionSize(size_t partition_id) const {
  if (partition_id >= num_partitions_) {
    return 0;
  }
  return partition_sizes_[partition_id].load(std::memory_order_relaxed);
}

std::vector<size_t> PartitionedIndex::getPartitionSizes() const {
  std::vector<size_t> sizes;
  sizes.reserve(num_partitions_);
  for (size_t i = 0; i < num_partitions_; ++i) {
    sizes.push_back(partition_sizes_[i].load(std::memory_order_relaxed));
  }
  return sizes;
}

size_t PartitionedIndex::getTotalSize() const {
  size_t total = 0;
  for (size_t i = 0; i < num_partitions_; ++i) {
    total += partition_sizes_[i].load(std::memory_order_relaxed);
  }
  return total;
}

double PartitionedIndex::getLoadImbalance() const {
  size_t total = getTotalSize();
  if (total == 0) {
    return 1.0;
  }

  double avg = static_cast<double>(total) / static_cast<double>(num_partitions_);
  size_t max_size = 0;
  for (size_t i = 0; i < num_partitions_; ++i) {
    max_size = std::max(max_size, partition_sizes_[i].load(std::memory_order_relaxed));
  }

  return static_cast<double>(max_size) / avg;
}

std::optional<size_t> PartitionedIndex::getPartitionForUid(uint64_t uid) const {
  std::shared_lock<std::shared_mutex> lock(map_mutex_);
  auto it = uid_partition_map_.find(uid);
  if (it != uid_partition_map_.end()) {
    return it->second;
  }
  return std::nullopt;
}

}  // namespace sageFlow

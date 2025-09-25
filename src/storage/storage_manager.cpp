#include "storage/storage_manager.h"

#include <queue>

#include "utils/logger.h"

auto candy::StorageManager::insert(std::unique_ptr<VectorRecord> record) -> void {
  if (record == nullptr) {
    throw std::runtime_error("StorageManager::insert: Attempt to insert a null record.");
  }
  std::unique_lock<std::shared_mutex> lock(map_mutex_);
  const auto uid = record->uid_;
  CANDY_LOG_DEBUG("STORAGE", "Inserting record uid={} current_size={} ", uid, records_.size());
  if (map_.find(uid) != map_.end()) {
    return; // UID 已存在
  }
  std::shared_ptr<VectorRecord> shared_record = std::move(record);
  auto idx = static_cast<int32_t>(records_.size());
  records_.push_back(shared_record);
  map_.emplace(uid, idx);
}

// auto candy::StorageManager::insert(std::shared_ptr<VectorRecord> record) -> void {
//   if (record == nullptr) {
//     throw std::runtime_error("StorageManager::insert: Attempt to insert a null record.");
//   }
//   std::unique_lock<std::shared_mutex> lock(map_mutex_);
//   const auto uid = record->uid_;
//   if (map_.contains(uid)) {
//     return; // UID 已存在
//   }
//   const auto idx = static_cast<int32_t>(records_.size());
//   records_.push_back(std::move(record));
//   map_.emplace(uid, idx);
// }

auto candy::StorageManager::erase(const uint64_t vector_id) -> bool {
  std::unique_lock<std::shared_mutex> lock(map_mutex_);
  const auto it = map_.find(vector_id);
  if (it == map_.end()) {
    return false;
  }
  const int32_t idx = it->second;

  // 若待删除元素不是最后一项，则将最后一项交换到 idx 处，并更新其在 map_ 中的索引
  if (idx < records_.size() - 1) {
    const uint64_t last_element_uid = records_.back()->uid_;
    std::swap(records_[idx], records_.back());
    map_[last_element_uid] = idx; // 更新最后一项的索引
  }

  records_.pop_back();
  map_.erase(it);
  return true;
}

auto candy::StorageManager::getVectorByUid(const uint64_t vector_id) -> std::shared_ptr<const VectorRecord> {
  std::shared_lock<std::shared_mutex> lock(map_mutex_);
  const auto it = map_.find(vector_id);
  if (it == map_.end()) {
    return nullptr;
  }

  const int32_t index = it->second;

  // 边界检查，增加代码健壮性
  if (index < 0 || index >= records_.size()) {
    // 这是一个数据不一致的错误状态，理论上不应发生
    // 可以增加日志记录
    return nullptr;
  }

  return records_[index];
}

auto candy::StorageManager::getVectorsByUids(const std::vector<uint64_t>& vector_ids)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  std::vector<std::shared_ptr<const VectorRecord>> records;
  records.reserve(vector_ids.size());
  std::shared_lock<std::shared_mutex> lock(map_mutex_);
  for (const auto uid : vector_ids) {
    const auto it = map_.find(uid);

    if (it != map_.end()) {
      const int32_t index = it->second;

      // 边界检查，增加代码健壮性
      if (index >= 0 && index < records_.size()) {
        // 直接从内部 vector 中获取 shared_ptr，这是一个非常轻量级的操作（仅增加引用计数）
        records.push_back(records_[index]);
      }
      // 如果索引越界，可以考虑增加日志来捕获这种不一致的状态
    }
  }
  return records;
}

auto candy::StorageManager::topk(const VectorRecord& record, int k) const -> std::vector<uint64_t> {
  if (engine_ == nullptr) {
    throw std::runtime_error("StorageManager::topk: Compute engine is not set.");
  }
  if (k <= 0) {
    return {};
  }

  // 使用优先队列来高效维护 Top-K 结果
  std::priority_queue<UidAndDist> top_k_results;

  // 步骤 1: 加共享锁，允许多个线程并发执行 topk
  {
    std::shared_lock<std::shared_mutex> lock(map_mutex_);

    // 步骤 2: 遍历存储中的所有向量
    for (const auto& stored_record_sptr : records_) {
      if (!stored_record_sptr) {
        continue;
      }

      // 计算距离
      double distance = engine_->EuclideanDistance(record.data_, stored_record_sptr->data_); // 替换为真实的距离计算

      // 步骤 3: 使用标准 Top-K 逻辑更新优先队列
      if (top_k_results.size() < k) {
        top_k_results.emplace(stored_record_sptr->uid_, distance);
      } else if (distance < top_k_results.top().distance_) {
        // 如果新向量比当前 Top-K 中最远的那个还要近，就替换它
        top_k_results.pop();
        top_k_results.emplace(stored_record_sptr->uid_, distance);
      }
    }
  }

  // 步骤 4: 从优先队列中提取并排序结果
  std::vector<uint64_t> final_ids;
  final_ids.reserve(top_k_results.size());
  while (!top_k_results.empty()) {
    final_ids.push_back(top_k_results.top().uid_);
    top_k_results.pop();
  }

  // 优先队列得到的是从远到近的顺序，我们需要将其反转
  std::reverse(final_ids.begin(), final_ids.end());

  return final_ids;
}

auto candy::StorageManager::similarityJoinQuery(const VectorRecord &record, double join_similarity_threshold) const -> std::vector<uint64_t> {
  if (engine_ == nullptr) {
    throw std::runtime_error("StorageManager::similarityJoinQuery: Compute engine is not set.");
  }
  std::vector<uint64_t> final_ids;
  {
    // 步骤 1: 加共享锁，允许多个线程并发执行 similarityJoinQuery
    std::shared_lock<std::shared_mutex> lock(map_mutex_);

    // 步骤 2: 遍历存储中的所有向量
    for (const auto& stored_record_sptr : records_) {
      if (!stored_record_sptr) {
        continue;
      }

      // 计算相似度
      double similarity = engine_->Similarity(record.data_, stored_record_sptr->data_);

      // 步骤 3: 如果相似度满足条件，则添加到结果中
      if (similarity >= join_similarity_threshold) {
        final_ids.push_back(stored_record_sptr->uid_);
      }
    }
  }
  return final_ids;
}
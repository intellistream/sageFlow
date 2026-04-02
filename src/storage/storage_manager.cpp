#include "storage/storage_manager.h"

#include <queue>

#include "utils/logger.h"

namespace sageFlow {

// ============================================================
// Shard resolution
// ============================================================

auto StorageManager::resolveShard(int shard_id) -> Shard* {
  if (shard_id == GLOBAL_SHARD) return &global_shard_;
  {
    std::shared_lock<std::shared_mutex> lk(shards_map_mutex_);
    auto it = shards_.find(shard_id);
    if (it != shards_.end()) return it->second.get();
  }
  return &global_shard_;  // fallback
}

auto StorageManager::resolveShard(int shard_id) const -> const Shard* {
  if (shard_id == GLOBAL_SHARD) return &global_shard_;
  {
    std::shared_lock<std::shared_mutex> lk(shards_map_mutex_);
    auto it = shards_.find(shard_id);
    if (it != shards_.end()) return it->second.get();
  }
  return &global_shard_;  // fallback
}

// ============================================================
// Shard lifecycle
// ============================================================

void StorageManager::createShard(int shard_id) {
  std::unique_lock<std::shared_mutex> lk(shards_map_mutex_);
  if (shards_.find(shard_id) != shards_.end()) {
    SAGEFLOW_LOG_WARN("STORAGE", "Shard {} already exists, skipping creation", shard_id);
    return;
  }
  shards_[shard_id] = std::make_unique<Shard>();
  SAGEFLOW_LOG_INFO("STORAGE", "Created shard {}", shard_id);
}

void StorageManager::removeShard(int shard_id) {
  std::unique_lock<std::shared_mutex> lk(shards_map_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    SAGEFLOW_LOG_WARN("STORAGE", "Shard {} not found for removal", shard_id);
    return;
  }
  shards_.erase(it);
  SAGEFLOW_LOG_INFO("STORAGE", "Removed shard {}", shard_id);
}

bool StorageManager::hasShard(int shard_id) const {
  std::shared_lock<std::shared_mutex> lk(shards_map_mutex_);
  return shards_.find(shard_id) != shards_.end();
}

// ============================================================
// insert
// ============================================================

auto StorageManager::insert(std::unique_ptr<VectorRecord> record, int shard_id) -> void {
  if (record == nullptr) {
    throw std::runtime_error("StorageManager::insert: Attempt to insert a null record.");
  }
  Shard* shard = resolveShard(shard_id);
  std::unique_lock<std::shared_mutex> lock(shard->mutex);
  const auto uid = record->uid_;
  SAGEFLOW_LOG_DEBUG("STORAGE", "Inserting record uid={} shard={} current_size={}", uid, shard_id, shard->records.size());
  if (shard->map.find(uid) != shard->map.end()) {
    return;  // UID already exists in this shard
  }
  std::shared_ptr<VectorRecord> shared_record = std::move(record);
  auto idx = static_cast<int32_t>(shard->records.size());
  shard->records.push_back(shared_record);
  shard->map.emplace(uid, idx);
}

// ============================================================
// erase
// ============================================================

auto StorageManager::erase(const uint64_t vector_id, int shard_id) -> bool {
  Shard* shard = resolveShard(shard_id);
  std::unique_lock<std::shared_mutex> lock(shard->mutex);
  const auto it = shard->map.find(vector_id);
  if (it == shard->map.end()) {
    return false;
  }
  const int32_t idx = it->second;

  // Swap-with-last for O(1) removal
  if (idx < static_cast<int32_t>(shard->records.size()) - 1) {
    const uint64_t last_element_uid = shard->records.back()->uid_;
    std::swap(shard->records[idx], shard->records.back());
    shard->map[last_element_uid] = idx;
  }

  shard->records.pop_back();
  shard->map.erase(it);
  return true;
}

// ============================================================
// getVectorByUid
// ============================================================

auto StorageManager::getVectorByUid(const uint64_t vector_id, int shard_id) -> std::shared_ptr<const VectorRecord> {
  Shard* shard = resolveShard(shard_id);
  std::shared_lock<std::shared_mutex> lock(shard->mutex);
  const auto it = shard->map.find(vector_id);
  if (it == shard->map.end()) {
    return nullptr;
  }

  const int32_t index = it->second;
  if (index < 0 || index >= static_cast<int32_t>(shard->records.size())) {
    return nullptr;
  }

  return shard->records[index];
}

// ============================================================
// getVectorsByUids
// ============================================================

auto StorageManager::getVectorsByUids(const std::vector<uint64_t>& vector_ids, int shard_id)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  Shard* shard = resolveShard(shard_id);
  std::vector<std::shared_ptr<const VectorRecord>> records;
  records.reserve(vector_ids.size());
  std::shared_lock<std::shared_mutex> lock(shard->mutex);
  for (const auto uid : vector_ids) {
    const auto it = shard->map.find(uid);
    if (it != shard->map.end()) {
      const int32_t index = it->second;
      if (index >= 0 && index < static_cast<int32_t>(shard->records.size())) {
        records.push_back(shard->records[index]);
      }
    }
  }
  return records;
}

// ============================================================
// topk
// ============================================================

auto StorageManager::topk(const VectorRecord& record, int k, int shard_id) const -> std::vector<uint64_t> {
  if (engine_ == nullptr) {
    throw std::runtime_error("StorageManager::topk: Compute engine is not set.");
  }
  if (k <= 0) {
    return {};
  }

  const Shard* shard = resolveShard(shard_id);
  std::priority_queue<UidAndDist> top_k_results;

  {
    std::shared_lock<std::shared_mutex> lock(shard->mutex);
    for (const auto& stored_record_sptr : shard->records) {
      if (!stored_record_sptr) {
        continue;
      }

      double distance = engine_->EuclideanDistance(record.data_, stored_record_sptr->data_);

      if (top_k_results.size() < static_cast<size_t>(k)) {
        top_k_results.emplace(stored_record_sptr->uid_, distance);
      } else if (distance < top_k_results.top().distance_) {
        top_k_results.pop();
        top_k_results.emplace(stored_record_sptr->uid_, distance);
      }
    }
  }

  std::vector<uint64_t> final_ids;
  final_ids.reserve(top_k_results.size());
  while (!top_k_results.empty()) {
    final_ids.push_back(top_k_results.top().uid_);
    top_k_results.pop();
  }
  std::reverse(final_ids.begin(), final_ids.end());

  return final_ids;
}

// ============================================================
// similarityJoinQuery
// ============================================================

auto StorageManager::similarityJoinQuery(const VectorRecord& record,
                                         double join_similarity_threshold,
                                         double similarity_alpha,
                                         int shard_id) const -> std::vector<uint64_t> {
  if (engine_ == nullptr) {
    throw std::runtime_error("StorageManager::similarityJoinQuery: Compute engine is not set.");
  }

  const Shard* shard = resolveShard(shard_id);
  std::vector<uint64_t> final_ids;
  {
    std::shared_lock<std::shared_mutex> lock(shard->mutex);
    for (const auto& stored_record_sptr : shard->records) {
      if (!stored_record_sptr) {
        continue;
      }
      double similarity = engine_->Similarity(record.data_, stored_record_sptr->data_, similarity_alpha);
      if (similarity >= join_similarity_threshold) {
        final_ids.push_back(stored_record_sptr->uid_);
      }
    }
  }
  return final_ids;
}

}  // namespace sageFlow

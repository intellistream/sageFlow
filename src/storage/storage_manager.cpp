#include "storage/storage_manager.h"

#include <queue>

auto candy::StorageManager::insert(std::unique_ptr<VectorRecord>& record) -> idx_t {
  auto uid = record->uid_;
  auto idx = static_cast<int32_t>(records_.size());
  records_.push_back(std::move(record));
  map_.emplace(uid, idx);
  return idx;
}

auto candy::StorageManager::erase(const uint64_t vector_id) -> bool {
  if (const auto it = map_.find(vector_id); it != map_.end()) {
    records_[it->second].reset();
    map_.erase(it);
    return true;
  }
  return false;
}

auto candy::StorageManager::getVectorByUid(const uint64_t vector_id) -> std::unique_ptr<VectorRecord> {
  if (const auto it = map_.find(vector_id); it != map_.end()) {
    return std::make_unique<VectorRecord>(*records_[it->second]);
  }
  return nullptr;
}

auto candy::StorageManager::getVectorsByUids(const std::vector<uint64_t>& vector_ids)
    -> std::vector<std::unique_ptr<VectorRecord>> {
  std::vector<std::unique_ptr<VectorRecord>> result;
  for (const auto& id : vector_ids) {
    if (auto record = getVectorByUid(id)) {
      result.push_back(std::move(record));
    }
  }
  return result;
}

auto candy::StorageManager::getVectorById(int32_t id) const -> std::unique_ptr<VectorRecord> {
  if (id < 0 || id >= static_cast<int32_t>(records_.size())) {
    return nullptr;
  }
  return std::make_unique<VectorRecord>(*records_[id]);
}

auto candy::StorageManager::getVectorsByIds(const std::vector<int32_t>& ids) const
    -> std::vector<std::unique_ptr<VectorRecord>> {
  std::vector<std::unique_ptr<VectorRecord>> result;
  for (const auto& id : ids) {
    if (auto record = getVectorById(id)) {
      result.push_back(std::move(record));
    }
  }
  return result;
}

auto candy::StorageManager::topk(const std::unique_ptr<VectorRecord>& record, int k) const -> std::vector<int32_t> {
  const auto rec = record.get();
  std::priority_queue<std::pair<double, int32_t>> pq;
  for (int i = 0; i < records_.size(); ++i) {
    const auto local_rec = records_[i].get();
    auto dist = engine_->EuclideanDistance(rec->data_, local_rec->data_);
    if (pq.size() < k) {
      pq.emplace(dist, i);
    } else if (dist < pq.top().first) {
      pq.pop();
      pq.emplace(dist, i);
    }
  }
  std::vector<int32_t> result;
  while (!pq.empty()) {
    result.push_back(pq.top().second);
    pq.pop();
  }
  std::ranges::reverse(result);
  return result;
}
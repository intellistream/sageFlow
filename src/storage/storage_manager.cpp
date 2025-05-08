#include "storage/storage_manager.h"

#include <queue>

auto candy::StorageManager::insert(std::unique_ptr<VectorRecord>& record) -> void {
  auto uid = record->uid_;
  auto idx = static_cast<int32_t>(records_.size());
  records_.push_back(std::move(record));
  map_.emplace(uid, idx);
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

auto candy::StorageManager::topk(const std::unique_ptr<VectorRecord>& record, int k) const -> std::vector<uint64_t> {
  if (engine_ == nullptr) throw std::runtime_error("StorageManager::topk: Compute engine is not set.");
  const auto rec = record.get();
  std::priority_queue<std::pair<double, int32_t>> pq;
  for (size_t i = 0; i < records_.size(); ++i) {
    if (records_[i] == nullptr) {
      continue;
    }
    const auto local_rec = records_[i].get();
    auto dist = engine_->EuclideanDistance(rec->data_, local_rec->data_);
    if (pq.size() < static_cast<size_t>(k)) {
      pq.emplace(dist, static_cast<int32_t>(i));
    } else if (dist < pq.top().first) {
      
      pq.emplace(dist, static_cast<int32_t>(i));
      pq.pop();
    }
  }
  std::vector<uint64_t> result;
  while (!pq.empty()) {
    result.push_back(records_[pq.top().second]->uid_);
    pq.pop();
  }
  std::ranges::reverse(result);
  return result;
}
//
// Created by Pygon on 25-4-17.
//
#include "index/knn.h"
#include "storage/storage_manager.h"
#include <algorithm>

sageFlow::Knn::~Knn() = default;

auto sageFlow::Knn::insert(uint64_t id) -> bool {
  // Fetch the actual record from global StorageManager and cache locally
  if (!storage_manager_) return false;
  auto records = storage_manager_->getVectorsByUids({id});
  if (records.empty()) return false;

  std::unique_lock lk(local_mutex_);
  local_records_[id] = records[0];
  return true;
}

auto sageFlow::Knn::erase(uint64_t id) -> bool {
  std::unique_lock lk(local_mutex_);
  return local_records_.erase(id) > 0;
}

size_t sageFlow::Knn::size() const {
  std::shared_lock lk(local_mutex_);
  return local_records_.size();
}

auto sageFlow::Knn::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
  if (!storage_manager_ || !storage_manager_->engine_) {
    return storage_manager_ ? storage_manager_->topk(record, k) : std::vector<uint64_t>{};
  }
  auto& engine = storage_manager_->engine_;

  std::shared_lock lk(local_mutex_);
  std::vector<std::pair<double, uint64_t>> scored;
  scored.reserve(local_records_.size());
  for (const auto& [uid, rec] : local_records_) {
    if (!rec) continue;
    double sim = engine->Similarity(record.data_, rec->data_, 1.0);
    scored.emplace_back(sim, uid);
  }
  std::partial_sort(scored.begin(),
                    scored.begin() + std::min(static_cast<size_t>(k), scored.size()),
                    scored.end(),
                    [](const auto& a, const auto& b) { return a.first > b.first; });
  std::vector<uint64_t> result;
  result.reserve(std::min(static_cast<size_t>(k), scored.size()));
  for (size_t i = 0; i < scored.size() && static_cast<int>(i) < k; ++i) {
    result.push_back(scored[i].second);
  }
  return result;
}

auto sageFlow::Knn::query_for_join(const VectorRecord &record,
                    double join_similarity_threshold,
                    double similarity_alpha) -> std::vector<uint64_t> {
  if (!storage_manager_ || !storage_manager_->engine_) {
    return storage_manager_ ? storage_manager_->similarityJoinQuery(record, join_similarity_threshold, similarity_alpha)
                            : std::vector<uint64_t>{};
  }
  auto& engine = storage_manager_->engine_;

  std::shared_lock lk(local_mutex_);
  std::vector<uint64_t> result;
  for (const auto& [uid, rec] : local_records_) {
    if (!rec) continue;
    double sim = engine->Similarity(record.data_, rec->data_, similarity_alpha);
    if (sim >= join_similarity_threshold) {
      result.push_back(uid);
    }
  }
  return result;
}

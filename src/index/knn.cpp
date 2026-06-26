//
// Created by Pygon on 25-4-17.
//
#include "index/knn.h"

#include <algorithm>
#include <mutex>
#include <queue>

sageFlow::Knn::~Knn() = default;

auto sageFlow::Knn::insert(uint64_t id) -> bool {
  std::unique_lock lock(ids_mutex_);
  return live_ids_.insert(id).second;
}

auto sageFlow::Knn::erase(uint64_t id) -> bool {
  std::unique_lock lock(ids_mutex_);
  return live_ids_.erase(id) != 0;
}

auto sageFlow::Knn::snapshotIds() const -> std::vector<uint64_t> {
  std::shared_lock lock(ids_mutex_);
  return {live_ids_.begin(), live_ids_.end()};
}

auto sageFlow::Knn::query(const VectorRecord& record, int k) -> std::vector<uint64_t> {
  if (!storage_manager_ || !storage_manager_->engine_ || k <= 0) {
    return {};
  }

  const auto ids = snapshotIds();
  const auto records = storage_manager_->getVectorsByUids(ids);
  std::priority_queue<UidAndDist> top_k_results;

  for (const auto& stored_record : records) {
    if (!stored_record) {
      continue;
    }
    const double distance =
        storage_manager_->engine_->EuclideanDistance(record.data_, stored_record->data_);
    if (static_cast<int>(top_k_results.size()) < k) {
      top_k_results.emplace(stored_record->uid_, distance);
    } else if (distance < top_k_results.top().distance_) {
      top_k_results.pop();
      top_k_results.emplace(stored_record->uid_, distance);
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

auto sageFlow::Knn::query_for_join(const VectorRecord &record,
                    double join_similarity_threshold,
                    double similarity_alpha) -> std::vector<uint64_t> {
  if (!storage_manager_ || !storage_manager_->engine_) {
    return {};
  }

  const auto ids = snapshotIds();
  const auto records = storage_manager_->getVectorsByUids(ids);
  std::vector<uint64_t> final_ids;
  final_ids.reserve(records.size());

  for (const auto& stored_record : records) {
    if (!stored_record) {
      continue;
    }
    const double similarity =
        storage_manager_->engine_->Similarity(record.data_, stored_record->data_, similarity_alpha);
    if (similarity >= join_similarity_threshold) {
      final_ids.push_back(stored_record->uid_);
    }
  }

  return final_ids;
}

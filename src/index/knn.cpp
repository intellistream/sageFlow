//
// Created by Pygon on 25-4-17.
//
#include "index/knn.h"
#include "storage/storage_manager.h"

sageFlow::Knn::~Knn() = default;

auto sageFlow::Knn::insert(uint64_t id) -> bool {
  std::unique_lock lk(local_mutex_);
  local_records_[id] = nullptr;  // placeholder, actual data in StorageManager
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
  auto all = storage_manager_->topk(record, k * 2);
  std::shared_lock lk(local_mutex_);
  std::vector<uint64_t> out;
  out.reserve(std::min(static_cast<size_t>(k), all.size()));
  for (uint64_t uid : all) {
    if (local_records_.count(uid)) {
      out.push_back(uid);
      if (static_cast<int>(out.size()) >= k) break;
    }
  }
  return out;
}

auto sageFlow::Knn::query_for_join(const VectorRecord &record,
                    double join_similarity_threshold,
                    double similarity_alpha) -> std::vector<uint64_t> {
  // Scan global storage for similar records, but only return those in this index
  auto all = storage_manager_->similarityJoinQuery(record, join_similarity_threshold, similarity_alpha);
  std::shared_lock lk(local_mutex_);
  std::vector<uint64_t> out;
  out.reserve(all.size());
  for (uint64_t uid : all) {
    if (local_records_.count(uid)) {
      out.push_back(uid);
    }
  }
  return out;
}

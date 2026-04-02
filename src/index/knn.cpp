#include "index/knn.h"
#include "storage/storage_manager.h"

namespace sageFlow {

Knn::~Knn() = default;

auto Knn::insert(uint64_t /*id*/) -> bool {
  // Data is managed by StorageManager shard; index only needs uid tracking
  // which is handled at the shard level. No-op here.
  return true;
}

auto Knn::erase(uint64_t id) -> bool {
  // Deletion is handled by StorageManager shard via the controller.
  return true;
}

size_t Knn::size() const {
  // Delegate to the shard in StorageManager if available.
  // For now, this is an approximation — the controller manages the shard.
  return 0;
}

auto Knn::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
  if (!storage_manager_) return {};
  // Route to our shard (index_id_ maps to shard_id if shard exists, else global)
  return storage_manager_->topk(record, k, index_id_);
}

auto Knn::query_for_join(const VectorRecord &record,
                         double join_similarity_threshold,
                         double similarity_alpha) -> std::vector<uint64_t> {
  if (!storage_manager_) return {};
  return storage_manager_->similarityJoinQuery(
      record, join_similarity_threshold, similarity_alpha, index_id_);
}

}  // namespace sageFlow

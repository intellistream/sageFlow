#include "operator/join_operator_methods/diskann.h"

namespace sageFlow {

std::vector<std::unique_ptr<VectorRecord>> DiskANNJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> results;
  if (!concurrency_manager_) {
    return results;
  }
  int idx = otherIndexId(query_slot);
  if (idx == -1) {
    return results;
  }
  // FreshDiskANN applies the similarity threshold internally; copy out candidates.
  auto shared_candidates = concurrency_manager_->query_for_join(idx, query_record, join_similarity_threshold_);
  results.reserve(shared_candidates.size());
  for (const auto& c : shared_candidates) {
    if (c) {
      results.emplace_back(std::make_unique<VectorRecord>(*c));
    }
  }
  return results;
}

}  // namespace sageFlow

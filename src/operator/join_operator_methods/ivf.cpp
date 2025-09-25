#include "operator/join_operator_methods/ivf.h"
#include "utils/logger.h"
#include <deque>

namespace candy {

std::vector<std::unique_ptr<VectorRecord>> IvfJoinMethod::ExecuteEager(const VectorRecord &query_record, int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> results;
  if (!concurrency_manager_) return results;
  int idx = otherIndexId(query_slot);
  if (idx == -1) [[unlikely]] {
    return results;
  }
  auto candidates = concurrency_manager_->query_for_join(idx, query_record, join_similarity_threshold_);
  CANDY_LOG_DEBUG("JOIN_IVF", "eager_query slot={} candidates={} ", query_slot, candidates.size());
  // LOG输出匹配上的向量和到达向量具体是什么
  CANDY_LOG_DEBUG("JOIN_IVF", "eager_query input uid={} ", query_record.uid_);
  for (auto &c : candidates) {
    if (c) {
  CANDY_LOG_DEBUG("JOIN_IVF", "eager_query matched candidate uid={} ", c->uid_);
    }
  }
  results.reserve(candidates.size());
  for (auto &c : candidates) {
    if (c) results.emplace_back(std::make_unique<VectorRecord>(*c));
  }
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> IvfJoinMethod::ExecuteLazy(const std::deque<std::unique_ptr<VectorRecord>> &query_records, int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> all_results;
  if (!concurrency_manager_) return all_results;
  int idx = (query_slot == 0) ? right_index_id_ : left_index_id_;
  if (idx == -1) [[unlikely]] {
    return all_results;
  }
  for (auto &qr : query_records) {
    if (!qr) continue;
    auto candidates = concurrency_manager_->query_for_join(idx, *qr, join_similarity_threshold_);
    for (auto &c : candidates) {
      if (c) all_results.emplace_back(std::make_unique<VectorRecord>(*c));
    }
  }
  return all_results;
}

} // namespace candy

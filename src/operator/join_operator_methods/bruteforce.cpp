#include "operator/join_operator_methods/bruteforce.h"
#include <unordered_set>
#include <deque>
#include "spdlog/spdlog.h"

namespace candy {

std::vector<std::unique_ptr<VectorRecord>> BruteForceJoinMethod::ExecuteEager(const VectorRecord &query_record, int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> results;
  if (!concurrency_manager_) return results;
  int idx = otherIndexId(query_slot);
  SPDLOG_DEBUG("BruteForceJoinMethod::ExecuteEager - Using index ID: {}", idx);
  if (idx == -1) {
    return results;
  }
  auto candidates = concurrency_manager_->query_for_join(idx, query_record, join_similarity_threshold_);
  results.reserve(candidates.size());
  for (auto &c : candidates) {
    if (c) results.emplace_back(std::make_unique<VectorRecord>(*c));
  }
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceJoinMethod::ExecuteLazy(const std::deque<std::unique_ptr<VectorRecord>> &query_records, int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> all_results;
  if (!concurrency_manager_) return all_results;
  int idx = otherIndexId(query_slot);
  SPDLOG_DEBUG("BruteForceJoinMethod::ExecuteLazy - Using index ID: {}", idx);
  if (idx == -1) {
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

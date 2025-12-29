#include "operator/join_operator_methods/bruteforce.h"
#include "operator/utils/join_method_registry.h"
#include <unordered_set>
#include <deque>
#include "spdlog/spdlog.h"

namespace sageFlow {

std::vector<std::unique_ptr<VectorRecord>> BruteForceJoinMethod::ExecuteEager(
    const VectorRecord &query_record, int query_slot, size_t /*subtask_index*/) {
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


} // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::BRUTEFORCE,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "BruteForce",
        "Ground truth baseline with brute-force scan. "
        "Provides 100% recall rate. Suitable for small windows or as reference.",
        sageFlow::JoinAlgorithm::BRUTEFORCE,
        true,   // supports_eager
        true,   // supports_lazy
        sageFlow::PartitionStrategy::ROUND_ROBIN,
        sageFlow::WindowStateType::SHARED,
        ""      // paper_reference
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int left_idx,
       int right_idx) {
        return std::make_unique<sageFlow::BruteForceJoinMethod>(
            left_idx, right_idx, config.similarity_threshold, cm);
    });

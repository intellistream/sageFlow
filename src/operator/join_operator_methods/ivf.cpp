#include "operator/join_operator_methods/ivf.h"
#include "operator/utils/join_method_registry.h"
#include "utils/logger.h"
#include <deque>

namespace sageFlow {

std::vector<std::unique_ptr<VectorRecord>> IvfJoinMethod::ExecuteEager(
    const VectorRecord &query_record, int query_slot, size_t /*subtask_index*/) {
  std::vector<std::unique_ptr<VectorRecord>> results;
  if (!concurrency_manager_) {
    SAGEFLOW_LOG_ERROR("JOIN_IVF", "ExecuteEager: concurrency_manager_ is NULL!");
    return results;
  }
  int idx = otherIndexId(query_slot);
  if (idx == -1) [[unlikely]] {
    SAGEFLOW_LOG_ERROR("JOIN_IVF", "ExecuteEager: invalid index id (slot={} left={} right={})",
                       query_slot, left_index_id_, right_index_id_);
    return results;
  }
  SAGEFLOW_LOG_INFO("JOIN_IVF", "ExecuteEager: querying index={} for uid={} threshold={:.4f}",
                    idx, query_record.uid_, join_similarity_threshold_);
  auto candidates = concurrency_manager_->query_for_join(idx, query_record, join_similarity_threshold_, similarity_alpha_);
  SAGEFLOW_LOG_INFO("JOIN_IVF", "ExecuteEager: index={} returned {} candidates", idx, candidates.size());
  SAGEFLOW_LOG_DEBUG("JOIN_IVF", "eager_query slot={} candidates={} ", query_slot, candidates.size());
  // LOG输出匹配上的向量和到达向量具体是什么
  SAGEFLOW_LOG_DEBUG("JOIN_IVF", "eager_query input uid={} ", query_record.uid_);
  for (auto &c : candidates) {
    if (c) {
  SAGEFLOW_LOG_DEBUG("JOIN_IVF", "eager_query matched candidate uid={} ", c->uid_);
    }
  }
  results.reserve(candidates.size());
  for (auto &c : candidates) {
    if (c) results.emplace_back(std::make_unique<VectorRecord>(*c));
  }
  return results;
}


} // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::IVF,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "IVF",
        "IVF (Inverted File Index) based approximate nearest neighbor join. "
        "Uses k-means clustering for space partitioning. "
        "Balanced recall-speed tradeoff.",
        sageFlow::JoinAlgorithm::IVF,
        true,   // supports_eager
        true,   // supports_lazy
        sageFlow::PartitionStrategy::ROUND_ROBIN,
        sageFlow::WindowStateType::SHARED,
        "Faiss, IEEE TBD 2017"
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int left_idx,
       int right_idx) {
        return std::make_unique<sageFlow::IvfJoinMethod>(
            left_idx, right_idx, config.similarity_threshold, cm);
    });

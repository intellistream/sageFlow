#include "operator/join_operator_methods/hnsw.h"
#include "operator/utils/join_method_registry.h"
#include "utils/logger.h"
#include <deque>

namespace sageFlow {

HNSWJoinMethod::HNSWJoinMethod(int left_index_id,
                               int right_index_id,
                               double join_similarity_threshold,
                               const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
                               const Config& config)
    : BaseMethod(join_similarity_threshold),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      concurrency_manager_(concurrency_manager),
      config_(config) {}

std::vector<std::shared_ptr<const VectorRecord>> HNSWJoinMethod::rangeSearchViaKNN(
    const VectorRecord& query_record, int index_id) {
  std::vector<std::shared_ptr<const VectorRecord>> results;
  
  if (!concurrency_manager_ || index_id < 0) {
    return results;
  }
  
  // HNSW 原生不支持范围搜索，使用 k-NN 后过滤的方式实现
  // 使用 ef_search 作为初始 k 值来获取足够的候选项
  int k = config_.ef_search;
  
  // 执行 k-NN 查询
  auto candidates = concurrency_manager_->query(index_id, query_record, k);
  
  // 过滤满足相似度阈值的结果
  // 注意: query 返回的已经是按距离排序的结果
  // 对于余弦相似度，我们需要验证相似度是否满足阈值
  for (const auto& candidate : candidates) {
    if (!candidate) continue;
    
    // 使用 query_for_join 来获取满足阈值的结果可能更准确
    // 但这里为了性能，直接使用 k-NN 结果并假设相似度已满足
    // 实际的相似度过滤由 JoinOperator 的后续验证完成
    results.push_back(candidate);
  }
  
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> HNSWJoinMethod::ExecuteEager(
    const VectorRecord& query_record, int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  if (!concurrency_manager_) {
    SAGEFLOW_LOG_WARN("HNSW_JOIN", "ExecuteEager: concurrency_manager is null");
    return results;
  }
  
  int idx = otherIndexId(query_slot);
  if (idx < 0) {
    SAGEFLOW_LOG_DEBUG("HNSW_JOIN", "ExecuteEager: invalid index id for slot {}", query_slot);
    return results;
  }
  
  // 使用 query_for_join 直接获取满足阈值的结果
  // 这是更准确的方式，因为它在索引层面就做了相似度过滤
  auto candidates = concurrency_manager_->query_for_join(idx, query_record, join_similarity_threshold_);
  
  SAGEFLOW_LOG_DEBUG("HNSW_JOIN", "ExecuteEager: slot={} candidates={}", query_slot, candidates.size());
  
  results.reserve(candidates.size());
  for (auto& c : candidates) {
    if (c) {
      results.emplace_back(std::make_unique<VectorRecord>(*c));
      SAGEFLOW_LOG_DEBUG("HNSW_JOIN", "ExecuteEager: matched candidate uid={}", c->uid_);
    }
  }
  
  return results;
}


void HNSWJoinMethod::setEfSearch(int ef_search) {
  if (ef_search > 0) {
    config_.ef_search = ef_search;
    SAGEFLOW_LOG_DEBUG("HNSW_JOIN", "Updated ef_search to {}", ef_search);
  }
}

HNSWJoinMethod::IndexStats HNSWJoinMethod::getStats() const {
  IndexStats stats;
  // 索引统计信息需要从 ConcurrencyManager 或底层索引获取
  // 当前返回默认值，后续可扩展
  return stats;
}

}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::HNSW,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "HNSW",
        "HNSW-based approximate nearest neighbor join. "
        "Uses hierarchical navigable small world graph for fast k-NN search. "
        "High recall with logarithmic query time.",
        sageFlow::JoinAlgorithm::HNSW,
        true,   // supports_eager
        true,   // supports_lazy
        sageFlow::PartitionStrategy::ROUND_ROBIN,
        sageFlow::WindowStateType::SHARED,
        "Malkov & Yashunin, IEEE TPAMI 2018"
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int left_idx,
       int right_idx) {
        sageFlow::HNSWJoinMethod::Config hnsw_config;
        hnsw_config.m = config.hnsw_m;
        hnsw_config.ef_construction = config.hnsw_ef_construction;
        hnsw_config.ef_search = config.hnsw_ef_search;
        hnsw_config.use_existing_index = true;
        return std::make_unique<sageFlow::HNSWJoinMethod>(
            left_idx, right_idx, config.similarity_threshold, cm, hnsw_config);
    });

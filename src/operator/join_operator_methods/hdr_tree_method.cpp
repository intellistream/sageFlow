#include "operator/join_operator_methods/hdr_tree_method.h"

#include <unordered_set>

#include "utils/logger.h"

namespace sageFlow {

HDRTreeMethod::HDRTreeMethod(int left_index_id, int right_index_id, double similarity_threshold,
                             const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
                             const Config& config)
    : BaseMethod(similarity_threshold),
      config_(config),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      concurrency_manager_(concurrency_manager) {
  // 更新配置中的相似度阈值
  config_.similarity_threshold = similarity_threshold;

  SAGEFLOW_LOG_DEBUG("HDRTreeMethod", "Created HDRTreeMethod: left_idx={}, right_idx={}, "
                     "threshold={}, projected_dim={}",
                     left_index_id_, right_index_id_,
                     config_.similarity_threshold, config_.projected_dim);
}

auto HDRTreeMethod::ExecuteEager(const VectorRecord& query_record, int query_slot)
    -> std::vector<std::unique_ptr<VectorRecord>> {
  std::vector<std::unique_ptr<VectorRecord>> results;

  if (!concurrency_manager_) {
    SAGEFLOW_LOG_WARN("HDRTreeMethod", "ConcurrencyManager is null");
    return results;
  }

  int idx = otherIndexId(query_slot);
  if (idx == -1) {
    SAGEFLOW_LOG_WARN("HDRTreeMethod", "Invalid index ID for slot {}", query_slot);
    return results;
  }

  SAGEFLOW_LOG_DEBUG("HDRTreeMethod", "ExecuteEager: querying index {} with threshold {}",
                     idx, join_similarity_threshold_);

  // 使用 ConcurrencyManager 进行查询
  // 底层会调用对应索引的 query_for_join 方法
  auto candidates = concurrency_manager_->query_for_join(idx, query_record,
                                                          join_similarity_threshold_);

  results.reserve(candidates.size());
  for (auto& c : candidates) {
    if (c) {
      results.emplace_back(std::make_unique<VectorRecord>(*c));
    }
  }

  SAGEFLOW_LOG_DEBUG("HDRTreeMethod", "ExecuteEager: found {} candidates", results.size());

  return results;
}

auto HDRTreeMethod::ExecuteLazy(const std::deque<std::unique_ptr<VectorRecord>>& query_records,
                                 int query_slot)
    -> std::vector<std::unique_ptr<VectorRecord>> {
  std::vector<std::unique_ptr<VectorRecord>> all_results;

  if (!concurrency_manager_) {
    SAGEFLOW_LOG_WARN("HDRTreeMethod", "ConcurrencyManager is null");
    return all_results;
  }

  int idx = otherIndexId(query_slot);
  if (idx == -1) {
    SAGEFLOW_LOG_WARN("HDRTreeMethod", "Invalid index ID for slot {}", query_slot);
    return all_results;
  }

  SAGEFLOW_LOG_DEBUG("HDRTreeMethod", "ExecuteLazy: processing {} queries on index {}",
                     query_records.size(), idx);

  // 使用 set 去重结果
  std::unordered_set<uint64_t> seen_uids;

  for (const auto& qr : query_records) {
    if (!qr) {
      continue;
    }

    auto candidates = concurrency_manager_->query_for_join(idx, *qr,
                                                            join_similarity_threshold_);

    for (auto& c : candidates) {
      if (c && seen_uids.find(c->uid_) == seen_uids.end()) {
        seen_uids.insert(c->uid_);
        all_results.emplace_back(std::make_unique<VectorRecord>(*c));
      }
    }
  }

  SAGEFLOW_LOG_DEBUG("HDRTreeMethod", "ExecuteLazy: found {} unique candidates from {} queries",
                     all_results.size(), query_records.size());

  return all_results;
}

}  // namespace sageFlow

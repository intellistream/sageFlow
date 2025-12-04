#include "operator/join_operator_methods/hdr_tree_method.h"
#include "operator/join_method_registry.h"

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


}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::HDR_TREE,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "HDR-Tree",
        "HDR-Tree baseline with PCA dimensionality reduction "
        "and R-tree spatial indexing. Optimized for dynamic updates.",
        sageFlow::JoinAlgorithm::HDR_TREE,
        true,   // supports_eager
        true,   // supports_lazy
        sageFlow::PartitionStrategy::KEY_HASH,
        sageFlow::WindowStateType::PARTITIONED,
        "Ukey et al., ADC 2022, DOI: 10.1007/978-3-031-15512-3_5"
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int left_idx,
       int right_idx) {
        sageFlow::HDRTreeMethod::Config hdr_config;
        hdr_config.similarity_threshold = config.similarity_threshold;
        hdr_config.projected_dim = config.hdr_projected_dim;
        hdr_config.pca_sample_size = config.hdr_pca_sample_size;
        return std::make_unique<sageFlow::HDRTreeMethod>(
            left_idx, right_idx, config.similarity_threshold, cm, hdr_config);
    });

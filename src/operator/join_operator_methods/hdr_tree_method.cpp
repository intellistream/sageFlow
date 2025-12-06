#include "operator/join_operator_methods/hdr_tree_method.h"
#include "operator/join_method_registry.h"
#include "utils/logger.h"

namespace sageFlow {

HDRTreeMethod::HDRTreeMethod(int left_index_id, int right_index_id, 
                             double join_similarity_threshold,
                             std::shared_ptr<ConcurrencyManager> concurrency_manager,
                             const Config& config)
    : BaseMethod(join_similarity_threshold),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      concurrency_manager_(std::move(concurrency_manager)),
      config_(config) {
}

std::vector<std::unique_ptr<VectorRecord>> HDRTreeMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // Determine which index to query
    // If query comes from Left (slot 0), query Right index
    // If query comes from Right (slot 1), query Left index
    int target_index_id = (query_slot == 0) ? right_index_id_ : left_index_id_;
    
    if (target_index_id == -1) {
        SAGEFLOW_LOG_WARN("HDRTree", "Target index id is -1 for slot {}", query_slot);
        return results;
    }
    
    // Use ConcurrencyManager to query the HDR Forest index
    // The index implementation handles pruning and PCA logic
    auto shared_results = concurrency_manager_->query_for_join(
        target_index_id, query_record, join_similarity_threshold_);
        
    // Convert shared_ptr to unique_ptr (copy)
    results.reserve(shared_results.size());
    for (const auto& rec : shared_results) {
        if (rec) {
            results.push_back(std::make_unique<VectorRecord>(*rec));
        }
    }
    
    return results;
}

// Register the method
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::HDR_TREE,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "HDRTree",
        "HDR Forest Baseline with Lazy/Batch Updates and Pruning",
        sageFlow::JoinAlgorithm::HDR_TREE,
        true,  // supports_eager
        false, // supports_lazy
        sageFlow::PartitionStrategy::VECTOR_HASH, // Recommended partition strategy
        sageFlow::WindowStateType::PARTITIONED,   // Recommended window state
        "Efficient continuous kNN join over dynamic high-dimensional data"
    }),
    [](const JoinStrategyConfig& config, 
       std::shared_ptr<ConcurrencyManager> cm, 
       int dim, 
       int left_idx, 
       int right_idx) {
        HDRTreeMethod::Config hdr_config;
        hdr_config.similarity_threshold = config.similarity_threshold;
        hdr_config.projected_dim = config.hdr_projected_dim;
        hdr_config.pca_sample_size = config.hdr_pca_sample_size;
        
        return std::make_unique<HDRTreeMethod>(
            left_idx, right_idx, 
            config.similarity_threshold, 
            cm, 
            hdr_config);
    }
);

} // namespace sageFlow

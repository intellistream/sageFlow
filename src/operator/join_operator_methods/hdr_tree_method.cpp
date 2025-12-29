#include "operator/join_operator_methods/hdr_tree_method.h"
#include "operator/utils/join_method_registry.h"
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
    int query_slot,
    size_t /*subtask_index*/) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 确定要查询的索引
    // 如果查询来自左侧（slot 0），则查询右侧索引
    // 如果查询来自右侧（slot 1），则查询左侧索引
    int target_index_id = (query_slot == 0) ? right_index_id_ : left_index_id_;
    
    if (target_index_id == -1) {
        SAGEFLOW_LOG_WARN("HDRTree", "Target index id is -1 for slot {}", query_slot);
        return results;
    }
    
    // 使用 ConcurrencyManager 查询 HDR Forest 索引
    // 索引实现处理剪枝和 PCA 逻辑
    auto shared_results = concurrency_manager_->query_for_join(
        target_index_id, query_record, join_similarity_threshold_);
        
    // 将 shared_ptr 转换为 unique_ptr（复制）
    results.reserve(shared_results.size());
    for (const auto& rec : shared_results) {
        if (rec) {
            results.push_back(std::make_unique<VectorRecord>(*rec));
        }
    }
    
    return results;
}

// 注册方法
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::HDR_TREE,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "HDRTree",
        "具有延迟/批量更新和剪枝功能的 HDR Forest 基线",
        sageFlow::JoinAlgorithm::HDR_TREE,
        true,  // 支持 eager 模式
        false, // 支持 lazy 模式
        sageFlow::PartitionStrategy::VECTOR_HASH, // 推荐的分区策略
        sageFlow::WindowStateType::PARTITIONED,   // 推荐的窗口状态
        "动态高维数据上的高效连续 kNN 连接"
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

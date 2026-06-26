#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "operator/utils/join_method_registry.h"
#include "compute_engine/compute_engine.h"
#include "utils/logger.h"

#include <algorithm>

namespace sageFlow {

BruteForceBaseline::BruteForceBaseline(double threshold)
    : BaseMethod(threshold),
      similarity_mode_(SimilarityMode::FIXED_ALPHA),
      similarity_alpha_(0.1) {
    // 验证阈值范围
    if (threshold < 0.0 || threshold > 1.0) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Threshold {} out of range [0.0, 1.0], clamping", threshold);
        join_similarity_threshold_ = std::clamp(threshold, 0.0, 1.0);
    }
}

BruteForceBaseline::BruteForceBaseline(double threshold, 
                                       SimilarityMode similarity_mode,
                                       double similarity_alpha)
    : BaseMethod(threshold),
      similarity_mode_(similarity_mode),
      similarity_alpha_(similarity_alpha) {
    // 验证阈值范围
    if (threshold < 0.0 || threshold > 1.0) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Threshold {} out of range [0.0, 1.0], clamping", threshold);
        join_similarity_threshold_ = std::clamp(threshold, 0.0, 1.0);
    }
    
    SAGEFLOW_LOG_INFO("BruteForceBaseline", 
        "Initialized with threshold={:.4f}, mode={}, alpha={:.6f}",
        join_similarity_threshold_,
        toString(similarity_mode_),
        similarity_alpha_);
}

void BruteForceBaseline::open(
    const RuntimeContext& context,
    WindowState* left_state,
    WindowState* right_state) {
    
    subtask_index_ = context.getSubtaskIndex();
    parallelism_ = context.getParallelism();
    left_state_ = left_state;
    right_state_ = right_state;
    initialized_ = true;
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline", 
        "Initialized {} with threshold={:.4f}, left_state={}, right_state={}",
        context.getTaskName(),
        join_similarity_threshold_,
        (left_state_ ? "valid" : "null"),
        (right_state_ ? "valid" : "null"));
}

std::vector<RecordView> BruteForceBaseline::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
    
    std::vector<RecordView> results;
    
    // 获取对侧窗口的记录
    // query_slot == 0 表示查询来自左流，需要搜索右流窗口
    // query_slot == 1 表示查询来自右流，需要搜索左流窗口
    WindowState* target_state = (query_slot == 0) ? right_state_ : left_state_;
    
    if (!target_state) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "ExecuteEager called with null target state for slot {}", query_slot);
        return results;
    }
    
    // 获取目标窗口的快照（线程安全）
    // 使用传入的 subtask_index 而不是内部存储的 subtask_index_
    // 使用 getRecordsSnapshot 而不是 getRecords，因为 SharedWindowState::getRecords 
    // 返回的引用在锁释放后可能被其他线程修改
    auto records_snapshot = target_state->getRecordsSnapshot(subtask_index);
    
    // 调试：记录窗口大小和状态指针
    static std::atomic<uint64_t> query_count{0};
    static std::atomic<uint64_t> total_window_size{0};
    uint64_t qc = query_count.fetch_add(1, std::memory_order_relaxed);
    total_window_size.fetch_add(records_snapshot.size(), std::memory_order_relaxed);
    if (qc % 500 == 0) {
        SAGEFLOW_LOG_INFO("BruteForceBaseline",
            "ExecuteEager: subtask={}/{} query_uid={} slot={} window_size={} avg_window={:.1f} shared={} state_ptr={}",
            subtask_index_, parallelism_, query_record.uid_, query_slot, 
            records_snapshot.size(), 
            static_cast<double>(total_window_size.load()) / (qc + 1),
            target_state->isShared(),
            static_cast<void*>(target_state));
    }
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline",
        "ExecuteEager: query_uid={}, slot={}, searching {} records",
        query_record.uid_, query_slot, records_snapshot.size());
    
    // 执行暴力搜索
    results = searchInRecordsSnapshot(query_record, records_snapshot);
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline",
        "ExecuteEager: found {} matches for query_uid={}",
        results.size(), query_record.uid_);
    
    return results;
}


void BruteForceBaseline::close() {
    left_state_ = nullptr;
    right_state_ = nullptr;
    initialized_ = false;
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline", "Closed");
}

std::vector<RecordView> BruteForceBaseline::searchInRecordsSnapshot(
    const VectorRecord& query,
    const std::vector<RecordView>& records) const {
    
    std::vector<RecordView> results;
    
    if (records.empty()) {
        return results;
    }
    
    if (query.data_.dim_ <= 0 || !query.data_.data_) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Query vector is empty for uid={}", query.uid_);
        return results;
    }
    
    ComputeEngine compute_engine;

    // 遍历所有记录，计算相似度
    for (const auto& record : records) {
        if (!record) {
            continue;
        }
        
        // 跳过自匹配（同一条记录）
        if (record->uid_ == query.uid_) {
            continue;
        }
        
        if (record->data_.dim_ <= 0 || !record->data_.data_) {
            continue;
        }
        if (record->data_.dim_ != query.data_.dim_ ||
            record->data_.type_ != query.data_.type_) {
            SAGEFLOW_LOG_WARN("BruteForceBaseline",
                "Vector shape mismatch: query uid={} dim={} type={} candidate uid={} dim={} type={}",
                query.uid_, query.data_.dim_, static_cast<int>(query.data_.type_),
                record->uid_, record->data_.dim_, static_cast<int>(record->data_.type_));
            continue;
        }
        
        const double similarity =
            similarity_mode_ == SimilarityMode::NORMALIZED
                ? compute_engine.NormalizedSimilarity(query.data_, record->data_, similarity_alpha_)
                : compute_engine.Similarity(query.data_, record->data_, similarity_alpha_);
        
        if (similarity >= join_similarity_threshold_) {
            // 共享视图，零拷贝（引用计数+1，不复制向量数据）
            results.push_back(record);
        }
    }
    
    return results;
}

std::vector<RecordView> BruteForceBaseline::searchInRecords(
    const VectorRecord& query,
    const std::deque<RecordView>& records) const {
    
    std::vector<RecordView> results;
    
    if (records.empty()) {
        return results;
    }
    
    if (query.data_.dim_ <= 0 || !query.data_.data_) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Query vector is empty for uid={}", query.uid_);
        return results;
    }
    
    ComputeEngine compute_engine;

    // 遍历所有记录，计算相似度
    for (const auto& record : records) {
        if (!record) {
            continue;
        }
        
        // 跳过自匹配（同一条记录）
        if (record->uid_ == query.uid_) {
            continue;
        }
        
        if (record->data_.dim_ <= 0 || !record->data_.data_) {
            continue;
        }
        if (record->data_.dim_ != query.data_.dim_ ||
            record->data_.type_ != query.data_.type_) {
            SAGEFLOW_LOG_WARN("BruteForceBaseline",
                "Vector shape mismatch: query uid={} dim={} type={} candidate uid={} dim={} type={}",
                query.uid_, query.data_.dim_, static_cast<int>(query.data_.type_),
                record->uid_, record->data_.dim_, static_cast<int>(record->data_.type_));
            continue;
        }
        
        const double similarity =
            similarity_mode_ == SimilarityMode::NORMALIZED
                ? compute_engine.NormalizedSimilarity(query.data_, record->data_, similarity_alpha_)
                : compute_engine.Similarity(query.data_, record->data_, similarity_alpha_);
        
        if (similarity >= join_similarity_threshold_) {
            results.push_back(record);
        }
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
       std::shared_ptr<sageFlow::ConcurrencyManager> /*cm*/,
       int /*dim*/,
       int /*left_idx*/,
       int /*right_idx*/) {
        return std::make_unique<sageFlow::BruteForceBaseline>(
            config.similarity_threshold,
            config.similarity_mode,
            config.similarity_alpha);
    });

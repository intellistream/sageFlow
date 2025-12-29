#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "compute_engine/simd_distance.h"
#include "utils/logger.h"

#include <cmath>
#include <algorithm>
#include <cstring>

namespace sageFlow {

namespace {

/**
 * @brief 从 VectorRecord 提取 float 向量
 * @param record 向量记录
 * @return float 向量
 */
std::vector<float> extractVector(const VectorRecord& record) {
    const auto& vector_data = record.data_;
    int32_t dim = vector_data.dim_;
    
    if (dim <= 0) {
        return {};
    }
    
    const float* float_ptr = reinterpret_cast<const float*>(vector_data.data_.get());
    std::vector<float> result(static_cast<size_t>(dim));
    std::memcpy(result.data(), float_ptr, static_cast<size_t>(dim) * sizeof(float));
    return result;
}

} // anonymous namespace

BruteForceBaseline::BruteForceBaseline(double threshold)
    : BaseMethod(threshold) {
    // 验证阈值范围
    if (threshold < 0.0 || threshold > 1.0) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Threshold {} out of range [0.0, 1.0], clamping", threshold);
        join_similarity_threshold_ = std::clamp(threshold, 0.0, 1.0);
    }
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

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
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
    
    // 调试：记录窗口大小
    static std::atomic<uint64_t> query_count{0};
    static std::atomic<uint64_t> total_window_size{0};
    uint64_t qc = query_count.fetch_add(1, std::memory_order_relaxed);
    total_window_size.fetch_add(records_snapshot.size(), std::memory_order_relaxed);
    if (qc % 500 == 0) {
        SAGEFLOW_LOG_INFO("BruteForceBaseline",
            "ExecuteEager: subtask={}/{} query_uid={} slot={} window_size={} avg_window={:.1f} shared={}",
            subtask_index_, parallelism_, query_record.uid_, query_slot, 
            records_snapshot.size(), 
            static_cast<double>(total_window_size.load()) / (qc + 1),
            target_state->isShared());
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

double BruteForceBaseline::computeSimilarity(
    const std::vector<float>& a, 
    const std::vector<float>& b) const {
    
    if (a.empty() || b.empty()) {
        return 0.0;
    }
    
    if (a.size() != b.size()) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline",
            "Vector dimension mismatch: {} vs {}", a.size(), b.size());
        return 0.0;
    }
    
    // 使用 L2 距离 + 指数衰减转换为相似度
    // 与 ComputeEngine::Similarity 保持一致
    double distance_sq = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
        distance_sq += diff * diff;
    }
    double distance = std::sqrt(distance_sq);
    
    // alpha = 0.1 是默认值，与 ComputeEngine 一致
    constexpr double kAlpha = 0.1;
    return std::exp(-kAlpha * distance);
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::searchInRecordsSnapshot(
    const VectorRecord& query,
    const std::vector<std::shared_ptr<const VectorRecord>>& records) const {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    if (records.empty()) {
        return results;
    }
    
    // 获取查询向量
    std::vector<float> query_vec = extractVector(query);
    if (query_vec.empty()) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Query vector is empty for uid={}", query.uid_);
        return results;
    }
    
    // 遍历所有记录，计算相似度
    for (const auto& record : records) {
        if (!record) {
            continue;
        }
        
        // 跳过自匹配（同一条记录）
        if (record->uid_ == query.uid_) {
            continue;
        }
        
        std::vector<float> record_vec = extractVector(*record);
        if (record_vec.empty()) {
            continue;
        }
        
        double similarity = computeSimilarity(query_vec, record_vec);
        
        if (similarity >= join_similarity_threshold_) {
            // 创建匹配记录的副本
            results.push_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::searchInRecords(
    const VectorRecord& query,
    const std::deque<std::unique_ptr<VectorRecord>>& records) const {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    if (records.empty()) {
        return results;
    }
    
    // 获取查询向量
    std::vector<float> query_vec = extractVector(query);
    if (query_vec.empty()) {
        SAGEFLOW_LOG_WARN("BruteForceBaseline", 
            "Query vector is empty for uid={}", query.uid_);
        return results;
    }
    
    // 遍历所有记录，计算相似度
    for (const auto& record : records) {
        if (!record) {
            continue;
        }
        
        // 跳过自匹配（同一条记录）
        if (record->uid_ == query.uid_) {
            continue;
        }
        
        std::vector<float> record_vec = extractVector(*record);
        if (record_vec.empty()) {
            continue;
        }
        
        double similarity = computeSimilarity(query_vec, record_vec);
        
        if (similarity >= join_similarity_threshold_) {
            // 创建匹配记录的副本
            results.push_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

} // namespace sageFlow

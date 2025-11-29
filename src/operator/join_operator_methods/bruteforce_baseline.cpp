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
    int query_slot) {
    
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
    
    // 获取目标窗口中的所有记录
    const auto& records = target_state->getRecords(subtask_index_);
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline",
        "ExecuteEager: query_uid={}, slot={}, searching {} records",
        query_record.uid_, query_slot, records.size());
    
    // 执行暴力搜索
    results = searchInRecords(query_record, records);
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline",
        "ExecuteEager: found {} matches for query_uid={}",
        results.size(), query_record.uid_);
    
    return results;
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::ExecuteLazy(
    const std::deque<std::unique_ptr<VectorRecord>>& query_records,
    int query_slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> all_results;
    
    if (query_records.empty()) {
        return all_results;
    }
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline",
        "ExecuteLazy: processing {} queries for slot {}",
        query_records.size(), query_slot);
    
    // 预估结果数量以减少内存重分配
    all_results.reserve(query_records.size() * 2);
    
    // 对每个查询执行 Eager 匹配
    for (const auto& query : query_records) {
        if (!query) {
            continue;
        }
        
        auto matches = ExecuteEager(*query, query_slot);
        for (auto& match : matches) {
            all_results.push_back(std::move(match));
        }
    }
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline",
        "ExecuteLazy: total {} matches from {} queries",
        all_results.size(), query_records.size());
    
    return all_results;
}

void BruteForceBaseline::close() {
    left_state_ = nullptr;
    right_state_ = nullptr;
    initialized_ = false;
    
    SAGEFLOW_LOG_DEBUG("BruteForceBaseline", "Closed");
}

double BruteForceBaseline::computeCosineSimilarity(
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
    
    // 使用 SIMD 优化的余弦相似度计算
    float similarity = SIMDDistance::cosineSimilarity(
        a.data(), b.data(), a.size());
    
    return static_cast<double>(similarity);
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
        
        double similarity = computeCosineSimilarity(query_vec, record_vec);
        
        if (similarity >= join_similarity_threshold_) {
            // 创建匹配记录的副本
            results.push_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

} // namespace sageFlow

#include "operator/join_operator_methods/ivf_method.h"
#include "compute_engine/simd_distance.h"
#include "utils/logger.h"

#include <cmath>
#include <algorithm>
#include <numeric>
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

IVFMethod::IVFMethod(const Config& config)
    : BaseMethod(config.similarity_threshold),
      config_(config) {
    // 验证参数
    if (config_.similarity_threshold < 0.0 || config_.similarity_threshold > 1.0) {
        SAGEFLOW_LOG_WARN("IVFMethod", 
            "Threshold {} out of range [0.0, 1.0], clamping", 
            config_.similarity_threshold);
        config_.similarity_threshold = std::clamp(config_.similarity_threshold, 0.0, 1.0);
        join_similarity_threshold_ = config_.similarity_threshold;
    }
    
    if (config_.nlist <= 0) {
        SAGEFLOW_LOG_WARN("IVFMethod", 
            "Invalid nlist={}, using default 100", config_.nlist);
        config_.nlist = 100;
    }
    
    if (config_.nprobes <= 0 || config_.nprobes > config_.nlist) {
        SAGEFLOW_LOG_WARN("IVFMethod", 
            "Invalid nprobes={} (nlist={}), clamping to [1, nlist]", 
            config_.nprobes, config_.nlist);
        config_.nprobes = std::clamp(config_.nprobes, 1, config_.nlist);
    }
    
    if (config_.rebuild_threshold <= 0.0 || config_.rebuild_threshold > 1.0) {
        SAGEFLOW_LOG_WARN("IVFMethod",
            "Invalid rebuild_threshold={}, using default 0.2",
            config_.rebuild_threshold);
        config_.rebuild_threshold = 0.2;
    }
}

IVFMethod::IVFMethod(double threshold)
    : IVFMethod(Config{threshold}) {
}

void IVFMethod::open(
    const RuntimeContext& context,
    WindowState* left_state,
    WindowState* right_state,
    ConcurrencyManager* concurrency_manager) {
    
    subtask_index_ = context.getSubtaskIndex();
    parallelism_ = context.getParallelism();
    left_state_ = left_state;
    right_state_ = right_state;
    concurrency_manager_ = concurrency_manager;
    
    // 如果提供了 ConcurrencyManager，尝试使用已有索引
    if (concurrency_manager_ && config_.use_existing_index) {
        // 查找已有索引或创建新索引
        // 索引命名约定：ivf_method_left_<subtask>, ivf_method_right_<subtask>
        std::string left_index_name = "ivf_method_left_" + std::to_string(subtask_index_);
        std::string right_index_name = "ivf_method_right_" + std::to_string(subtask_index_);
        
        // 注意：实际使用中，索引通常由 JoinOperator 创建并传递 ID
        // 这里我们假设索引已经通过 ConcurrencyManager 创建
        // 如果需要创建新索引，可以调用 create_index
        SAGEFLOW_LOG_DEBUG("IVFMethod",
            "Initialized with ConcurrencyManager, subtask={}, nlist={}, nprobes={}",
            subtask_index_, config_.nlist, config_.nprobes);
    }
    
    initialized_ = true;
    last_rebuild_size_.store(0);
    current_size_.store(0);
    
    SAGEFLOW_LOG_DEBUG("IVFMethod",
        "Opened {} with threshold={:.4f}, nlist={}, nprobes={}, use_index={}",
        context.getTaskName(),
        config_.similarity_threshold,
        config_.nlist,
        config_.nprobes,
        (concurrency_manager_ != nullptr));
}

void IVFMethod::open(
    const RuntimeContext& context,
    WindowState* left_state,
    WindowState* right_state) {
    // 不使用索引的降级模式
    open(context, left_state, right_state, nullptr);
}

std::vector<std::unique_ptr<VectorRecord>> IVFMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot,
    size_t subtask_index) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 获取对侧窗口状态
    WindowState* target_state = (query_slot == 0) ? right_state_ : left_state_;
    
    if (!target_state) {
        SAGEFLOW_LOG_WARN("IVFMethod",
            "ExecuteEager called with null target state for slot {}", query_slot);
        return results;
    }
    
    // 获取对侧索引 ID
    int32_t target_index_id = getOppositeIndexId(query_slot);
    
    // ====== IVF 索引查询策略 ======
    // 优先使用 IVF 索引进行近似查询：
    // - 如果 concurrency_manager_ 和索引 ID 有效，使用索引加速
    // - 否则降级为暴力搜索
    // 
    // IVF 索引特性：
    // - 通过聚类减少搜索空间，nprobes 控制搜索的簇数量
    // - 是近似索引，召回率取决于 nprobes/nlist 比例
    // - 适合大规模数据的高效搜索
    // =======================
    
    if (concurrency_manager_ && target_index_id >= 0) {
        // 使用 IVF 索引进行查询
        auto candidates = rangeSearchWithIndex(query_record, target_index_id);
        
        SAGEFLOW_LOG_DEBUG("IVFMethod",
            "ExecuteEager: query_uid={}, slot={}, index_id={}, found {} candidates via IVF index",
            query_record.uid_, query_slot, target_index_id, candidates.size());
        
        // 转换为 unique_ptr 结果
        results.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            if (candidate && candidate->uid_ != query_record.uid_) {
                results.push_back(std::make_unique<VectorRecord>(*candidate));
            }
        }
    } else {
        // 降级模式：无索引时使用暴力搜索
        auto records = target_state->getRecordsSnapshot(subtask_index_);
        
        SAGEFLOW_LOG_DEBUG("IVFMethod",
            "ExecuteEager: query_uid={}, slot={}, fallback to bruteforce, searching {} records",
            query_record.uid_, query_slot, records.size());
        
        results = rangeSearchBruteForceSnapshot(query_record, records);
    }
    
    SAGEFLOW_LOG_DEBUG("IVFMethod",
        "ExecuteEager: found {} matches for query_uid={}",
        results.size(), query_record.uid_);
    
    return results;
}

void IVFMethod::close() {
    left_state_ = nullptr;
    right_state_ = nullptr;
    concurrency_manager_ = nullptr;
    left_index_id_ = -1;
    right_index_id_ = -1;
    initialized_ = false;
    
    SAGEFLOW_LOG_DEBUG("IVFMethod", "Closed");
}

void IVFMethod::setNprobes(int nprobes) {
    if (nprobes <= 0 || nprobes > config_.nlist) {
        SAGEFLOW_LOG_WARN("IVFMethod",
            "Invalid nprobes={} (nlist={}), ignoring",
            nprobes, config_.nlist);
        return;
    }
    config_.nprobes = nprobes;
    
    SAGEFLOW_LOG_DEBUG("IVFMethod", "Set nprobes to {}", nprobes);
}

IVFMethod::IndexStats IVFMethod::getStats() const {
    IndexStats stats;
    
    // 从窗口状态获取统计信息
    if (left_state_) {
        stats.num_elements += left_state_->size(subtask_index_);
    }
    if (right_state_) {
        stats.num_elements += right_state_->size(subtask_index_);
    }
    
    // 如果使用索引，可以从索引获取更详细的统计
    // 目前返回基本信息
    stats.num_clusters = config_.nlist;
    
    return stats;
}

std::vector<std::shared_ptr<const VectorRecord>> IVFMethod::rangeSearchWithIndex(
    const VectorRecord& query,
    int32_t index_id) {
    
    std::vector<std::shared_ptr<const VectorRecord>> results;
    
    if (!concurrency_manager_ || index_id < 0) {
        return results;
    }
    
    // 使用 ConcurrencyManager 的 query_for_join 接口
    // 该接口返回满足相似度阈值的所有候选
    auto candidates = concurrency_manager_->query_for_join(
        index_id, query, config_.similarity_threshold, similarity_alpha_);
    
    results.reserve(candidates.size());
    for (auto& candidate : candidates) {
        if (candidate) {
            results.push_back(std::move(candidate));
        }
    }
    
    return results;
}

std::vector<std::unique_ptr<VectorRecord>> IVFMethod::rangeSearchBruteForceSnapshot(
    const VectorRecord& query,
    const std::vector<std::shared_ptr<const VectorRecord>>& records) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    if (records.empty()) {
        return results;
    }
    
    // 获取查询向量
    std::vector<float> query_vec = extractVector(query);
    if (query_vec.empty()) {
        SAGEFLOW_LOG_WARN("IVFMethod",
            "Query vector is empty for uid={}", query.uid_);
        return results;
    }
    
    // 遍历所有记录，计算相似度
    for (const auto& record : records) {
        if (!record) {
            continue;
        }
        
        // 跳过自匹配
        if (record->uid_ == query.uid_) {
            continue;
        }
        
        std::vector<float> record_vec = extractVector(*record);
        if (record_vec.empty()) {
            continue;
        }
        
        double similarity = computeSimilarity(query_vec, record_vec);
        
        if (similarity >= config_.similarity_threshold) {
            results.push_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

std::vector<std::unique_ptr<VectorRecord>> IVFMethod::rangeSearchBruteForce(
    const VectorRecord& query,
    const std::deque<std::unique_ptr<VectorRecord>>& records) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    if (records.empty()) {
        return results;
    }
    
    // 获取查询向量
    std::vector<float> query_vec = extractVector(query);
    if (query_vec.empty()) {
        SAGEFLOW_LOG_WARN("IVFMethod",
            "Query vector is empty for uid={}", query.uid_);
        return results;
    }
    
    // 遍历所有记录，计算相似度
    for (const auto& record : records) {
        if (!record) {
            continue;
        }
        
        // 跳过自匹配
        if (record->uid_ == query.uid_) {
            continue;
        }
        
        std::vector<float> record_vec = extractVector(*record);
        if (record_vec.empty()) {
            continue;
        }
        
        double similarity = computeSimilarity(query_vec, record_vec);
        
        if (similarity >= config_.similarity_threshold) {
            results.push_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

double IVFMethod::computeSimilarity(
    const std::vector<float>& a,
    const std::vector<float>& b) const {
    
    if (a.empty() || b.empty()) {
        return 0.0;
    }
    
    if (a.size() != b.size()) {
        SAGEFLOW_LOG_WARN("IVFMethod",
            "Vector dimension mismatch: {} vs {}", a.size(), b.size());
        return 0.0;
    }
    
    // IVFMethod 的候选获取通常走索引层 query_for_join()，相似度过滤在 Index 内部完成。
    // 这里的 computeSimilarity 仅用于少数 fallback（例如无索引/窗口快照暴力过滤）场景。
    double distance_sq = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
        distance_sq += diff * diff;
    }
    double distance = std::sqrt(distance_sq);

    // alpha 统一从 ConcurrencyManager -> StorageManager::engine_ 获取
    // （JoinStrategyFactory::create 会在运行时 setSimilarityAlpha）。
    return std::exp(-similarity_alpha_ * distance);
}

} // namespace sageFlow

#include "operator/join_operator_methods/s3j_method.h"
#include "operator/utils/join_method_registry.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>

#include "spdlog/spdlog.h"
#include "compute_engine/simd_distance.h" 
#include "state/partitioned_vector_state.h" 

namespace sageFlow {

S3JMethod::S3JMethod(int left_index_id,
                     int right_index_id,
                     double threshold,
                     const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
                     const S3JConfig& config)
    : BaseMethod(threshold),
      config_(config),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      concurrency_manager_(concurrency_manager) {
    
    // 更新配置中的阈值
    config_.similarity_threshold = threshold;
    
    // 初始化自适应分区器
    if (config_.enable_adaptive) {
        AdaptivePartitionerConfig adapt_config;
        adapt_config.initial_partitions = config_.num_partitions;
        adapt_config.adapt_interval_ms = config_.adapt_interval_ms;
        adapt_config.load_threshold = config_.load_threshold;
        
        partitioner_ = std::make_shared<AdaptivePartitioner>(
            config_.dimension, adapt_config, 42);
    }
    
    // 初始化索引选择器
    AdaptiveIndexSelectorConfig selector_config;
    selector_config.switch_threshold = config_.index_switch_threshold;
    index_selector_ = std::make_shared<AdaptiveIndexSelector>(selector_config);
    
    // 初始化指标收集器
    metrics_collector_.start_time = std::chrono::steady_clock::now();
}

S3JMethod::S3JMethod(double threshold, const S3JConfig& config)
    : S3JMethod(-1, -1, threshold, nullptr, config) {}

void S3JMethod::open(const RuntimeContext& context,
                     WindowState* left_state,
                     WindowState* right_state) {
    subtask_index_ = context.getSubtaskIndex();
    parallelism_ = context.getParallelism();
    left_state_ = left_state;
    right_state_ = right_state;
    
    // 重置指标
    metrics_collector_.reset();
    
    initialized_ = true;
    
    SPDLOG_DEBUG("S3JMethod::open - {} initialized with threshold={}", 
                 context.getTaskName(), config_.similarity_threshold);
}

//  辅助函数：安全获取 float*
const float* S3JMethod::getRawData(const VectorRecord& record) const {
    if (record.data_.dim_ <= 0 || !record.data_.data_) return nullptr;
    return reinterpret_cast<const float*>(record.data_.data_.get());
}

std::vector<std::unique_ptr<VectorRecord>> S3JMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {

    auto start = std::chrono::steady_clock::now();
    std::vector<std::unique_ptr<VectorRecord>> results;

    // 1. 确定目标状态 (Target State)
    WindowState* raw_target_state = (query_slot == 0) ? right_state_ : left_state_;
    auto* target_state = dynamic_cast<PartitionedVectorState*>(raw_target_state);

    // 计算距离阈值 t
    // 注意：假设 similarity_threshold 是相似度 (0~1)，转为距离阈值
    float t = 1.0f - static_cast<float>(config_.similarity_threshold);
    float t_half = t / 2.0f;
    int dim = config_.dimension;

    // 预先获取 Query 指针
    const float* query_ptr = getRawData(query_record);
    
    // 如果是 S3J 状态且 Query 数据有效
    if (target_state && query_ptr) {
        // [S3J Core Logic] Workset-based Search & Pruning
        
        // 获取所有 Workset 的快照
        auto worksets = target_state->getWorksetsSnapshot();

        for (auto* workset : worksets) {
            if (!workset || !workset->centroid) continue;

            const float* centroid_ptr = getRawData(*workset->centroid);
            if (!centroid_ptr) continue;
            
            //  使用 SIMD 库计算到质心的距离
            float dist_to_centroid = SIMDDistance::l2Distance(query_ptr, centroid_ptr, dim);

            // Step 2: Inner Set 判定 (剪枝优化核心)
            // IF dist(query, c_i) <= t/2:
            if (dist_to_centroid <= t_half) {
                // -> 归入 Inner Set (逻辑上)
                // -> [CRITICAL] 剪枝优化：直接输出 Inner Set 所有数据作为结果 (无需计算距离!)
                if (workset->inner_set) {
                    auto inner_records = workset->inner_set->getAllRecords(0);
                    for (const auto* rec : inner_records) {
                        results.emplace_back(std::make_unique<VectorRecord>(*rec));
                    }
                }
                // -> 仅需与 Outer Set 和 Outliers 进行距离计算
                if (workset->outer_set) scanTierForMatches(query_record, workset->outer_set.get(), t, results);
                if (workset->outliers)  scanTierForMatches(query_record, workset->outliers.get(), t, results);
            }
            // Step 5: 边界复制/邻居检查 (简化版逻辑)
            // 如果 query 虽然不在 Inner Set，但离质心足够近，可能匹配 Outer Set 或 Outliers
            // 这里的 3.0*t 是一个宽松的边界，确保不错过匹配
            else if (dist_to_centroid <= 3.0f * t) {
                if (workset->inner_set) scanTierForMatches(query_record, workset->inner_set.get(), t, results);
                if (workset->outer_set) scanTierForMatches(query_record, workset->outer_set.get(), t, results);
                if (workset->outliers)  scanTierForMatches(query_record, workset->outliers.get(), t, results);
            }
            // ELSE: 距离太远 (> 3t)，根据三角不等式，该 Workset 不可能有匹配点，跳过 (Pruned)
        }

    } 
    // 方法1：使用 ConcurrencyManager（如果可用，且没有走上面的 S3J 逻辑）
    else if (concurrency_manager_) {
        int idx = otherIndexId(query_slot);
        if (idx != -1) {
            auto candidates = concurrency_manager_->query_for_join(
                idx, query_record, join_similarity_threshold_);
            
            results.reserve(candidates.size());
            for (const auto& c : candidates) {
                if (c) {
                    results.emplace_back(std::make_unique<VectorRecord>(*c));
                }
            }
        }
    }
    // 方法2：使用窗口状态（如果没有 ConcurrencyManager 且非 PartitionedVectorState）
    else if (left_state_ && right_state_) {
        results = searchInWindowState(query_record, query_slot);
    }
    
    // 更新指标
    if (config_.enable_metrics) {
        auto end = std::chrono::steady_clock::now();
        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        
        metrics_collector_.query_count.fetch_add(1, std::memory_order_relaxed);
        metrics_collector_.total_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
        metrics_collector_.match_count.fetch_add(results.size(), std::memory_order_relaxed);
        
        // 更新分区统计（仅在分区器已初始化时）
        if (partitioner_ && partitioner_->isInitialized()) {
            size_t partition = partitioner_->partition(query_record, config_.num_partitions);
            partitioner_->updateStats(partition, latency_us, 1);
        }
    }
    
    // 检查是否需要自适应调整
    if (config_.enable_adaptive) {
        maybeAdapt();
    }
    
    return results;
}

//  辅助函数实现：扫描具体层的匹配项
void S3JMethod::scanTierForMatches(const VectorRecord& query, 
                                   TwoTierWindowState* tier, 
                                   float threshold,
                                   std::vector<std::unique_ptr<VectorRecord>>& results) {
    if (!tier) return;
    
    const float* query_ptr = getRawData(query);
    if (!query_ptr) return;

    int dim = config_.dimension;
    
    auto candidates = tier->getAllRecords(0);
    for (const auto* candidate : candidates) {
        const float* cand_ptr = getRawData(*candidate);
        if (!cand_ptr) continue;
        
        //  使用 SIMD 库计算距离
        float dist = SIMDDistance::l2Distance(query_ptr, cand_ptr, dim);
        
        if (dist <= threshold) {
            results.emplace_back(std::make_unique<VectorRecord>(*candidate));
        }
    }
}

void S3JMethod::close() {
    initialized_ = false;
    SPDLOG_DEBUG("S3JMethod::close - Method closed");
}

S3JMetrics S3JMethod::getMetrics() const {
    S3JMetrics metrics;
    
    // 基本统计
    metrics.total_queries = metrics_collector_.query_count.load();
    metrics.total_matches = metrics_collector_.match_count.load();
    
    // 平均延迟
    if (metrics.total_queries > 0) {
        metrics.avg_latency_ms = static_cast<double>(
            metrics_collector_.total_latency_us.load()) / metrics.total_queries / 1000.0;
    }
    
    // 吞吐量
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - metrics_collector_.start_time).count();
    if (elapsed > 0) {
        metrics.throughput_qps = static_cast<double>(metrics.total_queries) / elapsed;
    }
    
    // 估算召回率（基于匹配数/查询数）
    if (metrics.total_queries > 0) {
        metrics.recall_estimate = std::min(1.0, 
            static_cast<double>(metrics.total_matches) / metrics.total_queries);
    }
    
    // 分区信息
    if (partitioner_) {
        metrics.current_partitions = partitioner_->getCurrentNumPartitions();
        metrics.adapt_history = partitioner_->getHistory();
    } else {
        metrics.current_partitions = config_.num_partitions;
    }
    
    // 索引类型
    metrics.current_index_type = AdaptiveIndexSelector::indexTypeToString(current_index_type_);
    
    return metrics;
}

void S3JMethod::forceAdapt() {
    if (!config_.enable_adaptive || !partitioner_) {
        return;
    }
    
    bool adapted = partitioner_->forceAdapt();
    
    if (adapted) {
        SPDLOG_DEBUG("S3JMethod::forceAdapt - Partitioner adapted, new partition count: {}",
                     partitioner_->getCurrentNumPartitions());
    }
    
    // 检查是否需要切换索引类型
    if (index_selector_ && config_.enable_metrics) {
        size_t data_size = metrics_collector_.query_count.load();
        IndexPerformance current_perf;
        current_perf.sample_count = data_size;
        if (data_size > 0) {
            current_perf.avg_latency_us = static_cast<double>(
                metrics_collector_.total_latency_us.load()) / data_size;
        }
        
        IndexType recommended = index_selector_->shouldSwitchIndex(
            current_index_type_, current_perf, data_size, config_.dimension);
        
        if (recommended != current_index_type_) {
            switchIndex(recommended);
        }
    }
}

void S3JMethod::setConcurrencyManager(const std::shared_ptr<ConcurrencyManager>& manager) {
    concurrency_manager_ = manager;
}

void S3JMethod::setWindowStates(WindowState* left_state, WindowState* right_state) {
    left_state_ = left_state;
    right_state_ = right_state;
}

int S3JMethod::otherIndexId(int slot) const {
    return (slot == 0) ? right_index_id_ : left_index_id_;
}

void S3JMethod::maybeAdapt() {
    if (!partitioner_ || !partitioner_->isInitialized()) return;
    
    bool adapted = partitioner_->checkAndAdapt();
    
    if (adapted) {
        SPDLOG_DEBUG("S3JMethod::maybeAdapt - Automatic adaptation triggered");
    }
}

bool S3JMethod::switchIndex(IndexType new_type) {
    if (new_type == current_index_type_) {
        return false;
    }
    
    SPDLOG_INFO("S3JMethod::switchIndex - Switching from {} to {}",
                AdaptiveIndexSelector::indexTypeToString(current_index_type_),
                AdaptiveIndexSelector::indexTypeToString(new_type));
    
    current_index_type_ = new_type;
    
    // 注意：实际的索引切换需要重建索引，这里只记录状态变化
    // 完整实现需要与 ConcurrencyManager 协调重建索引
    
    return true;
}

std::vector<std::shared_ptr<const VectorRecord>> S3JMethod::searchInPartition(
    const VectorRecord& query, int slot, double threshold) {
    
    std::vector<std::shared_ptr<const VectorRecord>> results;
    
    if (!concurrency_manager_) {
        return results;
    }
    
    int idx = otherIndexId(slot);
    if (idx == -1) {
        return results;
    }
    
    return concurrency_manager_->query_for_join(idx, query, threshold);
}

std::vector<std::unique_ptr<VectorRecord>> S3JMethod::searchInWindowState(
    const VectorRecord& query, int slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 选择对侧窗口状态
    WindowState* target_state = (slot == 0) ? right_state_ : left_state_;
    
    if (!target_state) {
        return results;
    }
    
    // 获取查询向量
    std::vector<float> query_vec = extractFloatVector(query);
    
    // 遍历窗口内记录
    const auto& records = target_state->getRecords(subtask_index_);
    
    for (const auto& record : records) {
        if (!record) continue;
        
        std::vector<float> candidate_vec = extractFloatVector(*record);
        double similarity = computeCosineSimilarity(query_vec, candidate_vec);
        
        if (similarity >= join_similarity_threshold_) {
            results.emplace_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

double S3JMethod::computeCosineSimilarity(
    const std::vector<float>& a, 
    const std::vector<float>& b) const {
    
    if (a.size() != b.size() || a.empty()) {
        return 0.0;
    }
    
    double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
    
    for (size_t i = 0; i < a.size(); ++i) {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    
    double denom = std::sqrt(norm_a) * std::sqrt(norm_b);
    if (denom < 1e-10) {
        return 0.0;
    }
    
    return dot / denom;
}

std::vector<float> S3JMethod::extractFloatVector(const VectorRecord& record) const {
    const auto& data = record.data_;
    int dim = data.dim_;
    
    if (dim <= 0) {
        return {};
    }
    
    const float* float_ptr = reinterpret_cast<const float*>(data.data_.get());
    return std::vector<float>(float_ptr, float_ptr + dim);
}

}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::S3J,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "S3J",
        "DEBS'23 Adaptive Distributed Streaming Similarity Joins. "
        "Uses centroid-based partitioning and adaptive zone grouping. "
        "Supports load-aware self-adjustment.",
        sageFlow::JoinAlgorithm::S3J,
        true,   // supports_eager
        true,   // supports_lazy
        sageFlow::PartitionStrategy::CENTROID,
        sageFlow::WindowStateType::PARTITIONED,
        "Siachamis et al., DEBS 2023, DOI: 10.1145/3583678.3596891"
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int left_idx,
       int right_idx) {
        sageFlow::S3JConfig s3j_config;
        s3j_config.similarity_threshold = config.similarity_threshold;
        s3j_config.num_partitions = config.s3j_num_centroids;
        s3j_config.adapt_interval_ms = config.s3j_adapt_interval_ms;
        s3j_config.load_threshold = config.s3j_load_threshold;
        s3j_config.enable_adaptive = config.s3j_enable_adaptive;
        s3j_config.dimension = config.dimension;
        s3j_config.nlist = config.ivf_nlist;
        s3j_config.nprobes = config.ivf_nprobes;
        return std::make_unique<sageFlow::S3JMethod>(
            left_idx, right_idx, config.similarity_threshold, cm, s3j_config);
    });
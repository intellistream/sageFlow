#include "operator/join_operator_methods/s3j_method.h"
#include "operator/utils/join_method_registry.h"
#include <cmath>
#include <algorithm>
#include "utils/logger.h"
#include "compute_engine/simd_distance.h"
#include "state/partitioned_vector_state.h"
#include "state/two_tier_window_state.h"
#include <chrono>
#include <thread>

namespace sageFlow {

S3JMethod::S3JMethod(double threshold, const S3JConfig& config)
    : BaseMethod(threshold), config_(config) {
}

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
}

void S3JMethod::open(const RuntimeContext& context,
                     WindowState* left_state,
                     WindowState* right_state) {
    subtask_index_ = context.getSubtaskIndex();
    parallelism_ = context.getParallelism();
    left_state_ = left_state;
    right_state_ = right_state;
    initialized_ = true;
    
    metrics_collector_.reset();
    
    // Set up S3J distance threshold (t)
    double alpha = similarity_alpha_;
    if (alpha <= 1e-9) alpha = 0.1;
    double dist_thresh = -std::log(join_similarity_threshold_) / alpha;
    if (dist_thresh < 0) dist_thresh = 0;
    float s3j_dist_threshold = static_cast<float>(dist_thresh);

    SAGEFLOW_LOG_INFO("S3J", "Converted Similarity Thresh {} to Distance Thresh {} (alpha={})", 
                      join_similarity_threshold_, dist_thresh, alpha);
    
    auto* p_left = dynamic_cast<PartitionedVectorState*>(left_state_);
    if (p_left) p_left->setS3JThreshold(s3j_dist_threshold);
    
    auto* p_right = dynamic_cast<PartitionedVectorState*>(right_state_);
    if (p_right) p_right->setS3JThreshold(s3j_dist_threshold);

    // [Fix- Step 2] Start Background Adaptation Thread for Starved Workers
    if (config_.enable_adaptive) {
        running_ = true;
        adaptation_thread_ = std::thread([this]() {
            while (running_) {
                // Sleep for a fraction of the adapt interval to check frequently enough
                // but not burn CPU. Using 100ms or 1/10th of interval.
                int64_t sleep_ms = std::max<int64_t>(100, config_.adapt_interval_ms / 10);
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                if (!running_) break;
                
                // Call maybeAdapt() or directly partitioner check
                // We use maybeAdapt() to reuse logic, but maybeAdapt logs too much?
                // Direct call is cleaner for background thread.
                if (partitioner_) {
                    if (partitioner_->checkAndAdapt()) {
                         SAGEFLOW_LOG_INFO("S3J", "Background thread triggered adaptation on subtask={}", subtask_index_);
                    }
                }
            }
        });
    }
}

void S3JMethod::setWindowStates(WindowState* left_state, WindowState* right_state) {
    left_state_ = left_state;
    right_state_ = right_state;
}

void S3JMethod::setConcurrencyManager(const std::shared_ptr<ConcurrencyManager>& manager) {
    concurrency_manager_ = manager;
}

void S3JMethod::setWorksetDirectory(std::shared_ptr<WorksetDirectory> dir) {
    workset_directory_ = std::move(dir);
}

// Exponential Similarity: exp(-alpha * Distance)
double S3JMethod::computeSimilarity(const float* a, const float* b, size_t dim) const {
    float dist = SIMDDistance::l2Distance(a, b, dim);
    double alpha = similarity_alpha_;
    if (alpha <= 1e-9) alpha = 0.1;
    return std::exp(-alpha * dist);
}

std::vector<std::unique_ptr<VectorRecord>> S3JMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot, size_t subtask_index) {
    
    // [Fix-Step 1] Sync Point Instrumentation and Trigger
    // Still useful to call here for eager updates from active workers
    maybeAdapt();

    metrics_collector_.query_count++;
    auto results = searchInWindowState(query_record, query_slot, subtask_index);
    metrics_collector_.match_count += results.size();
    return results;
}

void S3JMethod::scanTierForMatches(const VectorRecord& query, 
                        TwoTierWindowState* tier, 
                        float threshold,
                        std::vector<std::unique_ptr<VectorRecord>>& results) {
    if (!tier) return;
    
    auto records = tier->getAllRecords(0); 
    size_t dim = query.data_.dim_;
    const float* q_vec = reinterpret_cast<const float*>(query.data_.data_.get());

    for (const auto* candidate : records) {
         if (candidate->data_.dim_ != dim) continue;
         const float* c_vec = reinterpret_cast<const float*>(candidate->data_.data_.get());
         
         double similarity = computeSimilarity(q_vec, c_vec, dim);
         
         if (similarity >= threshold) {
             results.push_back(std::make_unique<VectorRecord>(*candidate));
         }
    }
}

std::vector<std::unique_ptr<VectorRecord>> S3JMethod::searchInWindowState(
    const VectorRecord& query, int slot, size_t subtask_index) {
    
    WindowState* target_state = (slot == 0) ? right_state_ : left_state_;
    if (!target_state) return {};
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    size_t dim = query.data_.dim_;
    
    auto* s3j_state = dynamic_cast<PartitionedVectorState*>(target_state);
    
    std::vector<S3JWorkset*> worksets;
    if (s3j_state) {
        worksets = s3j_state->getWorksetsSnapshot();
    }

    if (s3j_state && !worksets.empty()) {
        const float* q_vec = reinterpret_cast<const float*>(query.data_.data_.get());
        
        double alpha = similarity_alpha_;
        if (alpha <= 1e-9) alpha = 0.1;
        // t: distance threshold converted from similarity threshold
        double t = -std::log(join_similarity_threshold_) / alpha;
        if (t < 0) t = 0;
        
        double t_half = t / 2.0;
        double t_double = t * 2.0;
        
        for (auto* ws : worksets) {
            ws->computation_cost.fetch_add(1, std::memory_order_relaxed);

            // [S3J Paper Section 7] Triangle inequality based pruning
            // Determine which sets to scan based on dist(query, centroid)
            bool scan_inner = false;
            bool scan_outer = false;
            
            if (ws->centroid) {
                const float* c_vec = reinterpret_cast<const float*>(ws->centroid->data_.data_.get());
                float dist_qc = SIMDDistance::l2Distance(q_vec, c_vec, dim);
                
                // Case 1: dist(q,c) <= t/2 -> Only scan Inner Set
                // All matches guaranteed in Inner Set by triangle inequality
                if (dist_qc <= t_half) {
                    scan_inner = true;
                    scan_outer = false;
                }
                // Case 2: t/2 < dist(q,c) <= t -> Scan both Inner and Outer
                else if (dist_qc <= t) {
                    scan_inner = true;
                    scan_outer = true;
                }
                // Case 3: t < dist(q,c) <= 2t -> Only scan Outer Set
                // Inner Set points are too close to centroid to match
                else if (dist_qc <= t_double) {
                    scan_inner = false;
                    scan_outer = true;
                }
                // Case 4: dist(q,c) > 2t -> Skip this Workset entirely
                else {
                    scan_inner = false;
                    scan_outer = false;
                }
            } else {
                // No centroid, conservatively scan both
                scan_inner = true;
                scan_outer = true;
            }
            
            if (scan_inner) {
                scanTierForMatches(query, ws->inner_set.get(), join_similarity_threshold_, results);
            }
            if (scan_outer) {
                scanTierForMatches(query, ws->outer_set.get(), join_similarity_threshold_, results);
            }
            // Outliers: Always scan (they don't follow workset geometry)
            scanTierForMatches(query, ws->outliers.get(), join_similarity_threshold_, results);
        }
    } else {
        auto snapshot = target_state->getRecordsSnapshot(subtask_index);
        const float* q_vec = reinterpret_cast<const float*>(query.data_.data_.get());
        
        for (const auto& candidate : snapshot) {
            if (candidate->data_.dim_ != dim) continue;
            const float* c_vec = reinterpret_cast<const float*>(candidate->data_.data_.get());
            double similarity = computeSimilarity(q_vec, c_vec, dim);
            if (similarity >= join_similarity_threshold_) {
                results.push_back(std::make_unique<VectorRecord>(*candidate));
            }
        }
    }
    
    return results;
}

void S3JMethod::close() {
    running_ = false;
    if (adaptation_thread_.joinable()) {
        adaptation_thread_.join();
    }
    initialized_ = false;
}

S3JMetrics S3JMethod::getMetrics() const {
    S3JMetrics m;
    m.total_queries = metrics_collector_.query_count;
    m.total_matches = metrics_collector_.match_count;
    m.current_partitions = partitioner_ ? partitioner_->getCurrentNumPartitions() : config_.num_partitions;
    return m;
}

void S3JMethod::forceAdapt() { 
    if (partitioner_) partitioner_->forceAdapt();
}

int S3JMethod::otherIndexId(int slot) const { return (slot == 0) ? right_index_id_ : left_index_id_; }

void S3JMethod::maybeAdapt() { 
    if (!config_.enable_adaptive) return;

    static thread_local int log_skips = 0;
    if (log_skips++ % 1000 == 0) {
        SAGEFLOW_LOG_DEBUG("S3J", "maybeAdapt check: subtask={}", subtask_index_);
    }

    if (partitioner_) {
        if (partitioner_->checkAndAdapt()) {
             SAGEFLOW_LOG_INFO("S3J", "Adaptive partitioner triggered adaptation on subtask={}", subtask_index_);
        }
    }
}

std::pair<const float*, size_t> S3JMethod::getRawVectorView(const VectorRecord& record) const {
    return {reinterpret_cast<const float*>(record.data_.data_.get()), static_cast<size_t>(record.data_.dim_)};
}

} // namespace sageFlow

// S3J method registration
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::S3J,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "S3J",
        "S3J (Scalable Similarity Stream Join) algorithm from DEBS'23. "
        "Adaptive partitioning with dynamic workset rebalancing. "
        "Uses CENTROID partitioning strategy with PARTITIONED window state.",
        sageFlow::JoinAlgorithm::S3J,
        true,   // supports_eager
        false,  // supports_lazy (deprecated)
        sageFlow::PartitionStrategy::CENTROID,
        sageFlow::WindowStateType::PARTITIONED,
        "DEBS'23: Scalable Similarity Stream Join"
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int /*left_idx*/,
       int /*right_idx*/) {
        // Configure S3JMethod
        sageFlow::S3JConfig s3j_config;
        s3j_config.similarity_threshold = config.similarity_threshold;
        s3j_config.dimension = config.dimension;
        s3j_config.num_partitions = config.num_partitions;
        s3j_config.enable_adaptive = true;
        s3j_config.enable_metrics = true;
        
        auto method = std::make_unique<sageFlow::S3JMethod>(
            config.similarity_threshold, s3j_config);
        method->setConcurrencyManager(cm);
        return method;
    }
);

#include "operator/join_operator_methods/s3j_method.h"
#include <cmath>
#include <algorithm>
#include "utils/logger.h"
#include "compute_engine/simd_distance.h"
#include "state/partitioned_vector_state.h"
#include "state/two_tier_window_state.h"

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
    // Relationship: Sim >= Thresh  <==>  Dist <= (1 - Thresh)
    float s3j_dist_threshold = 1.0f - static_cast<float>(join_similarity_threshold_);
    if (s3j_dist_threshold < 0.0f) s3j_dist_threshold = 0.0f;
    
    auto* p_left = dynamic_cast<PartitionedVectorState*>(left_state_);
    if (p_left) p_left->setS3JThreshold(s3j_dist_threshold);
    
    auto* p_right = dynamic_cast<PartitionedVectorState*>(right_state_);
    if (p_right) p_right->setS3JThreshold(s3j_dist_threshold);
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

// Linear Similarity: 1.0 - Distance
// Ensures that Dist <= 0.1 <==> Sim >= 0.9 (when thresh=0.9)
double S3JMethod::computeSimilarity(const float* a, const float* b, size_t dim) const {
    float dist = SIMDDistance::l2Distance(a, b, dim);
    return std::max(0.0f, 1.0f - dist);
}

std::vector<std::unique_ptr<VectorRecord>> S3JMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
    
    metrics_collector_.query_count++;
    auto results = searchInWindowState(query_record, query_slot);
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
    const VectorRecord& query, int slot) {
    
    // Safety check for unit tests
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
        
        double dist_threshold = 1.0 - join_similarity_threshold_;
        if (dist_threshold < 0.0) dist_threshold = 0.0;
        
        double pruning_limit = 4.0 * dist_threshold; 
        
        for (auto* ws : worksets) {
            // [Fix] Track computation cost
            ws->computation_cost.fetch_add(1, std::memory_order_relaxed);

            // Pruning Check
            bool skip_inner_outer = false;
            
            if (ws->centroid) {
                const float* c_vec = reinterpret_cast<const float*>(ws->centroid->data_.data_.get());
                float dist_qc = SIMDDistance::l2Distance(q_vec, c_vec, dim);
                
                if (dist_qc > pruning_limit) {
                    skip_inner_outer = true;
                }
            }
            
            if (!skip_inner_outer) {
                scanTierForMatches(query, ws->inner_set.get(), join_similarity_threshold_, results);
                scanTierForMatches(query, ws->outer_set.get(), join_similarity_threshold_, results);
            }
            // Always scan outliers as they are unbounded
            scanTierForMatches(query, ws->outliers.get(), join_similarity_threshold_, results);
        }
    } else {
        // Fallback: Flat Scan
        auto snapshot = target_state->getRecordsSnapshot(subtask_index_);
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
    initialized_ = false;
}

S3JMetrics S3JMethod::getMetrics() const {
    S3JMetrics m;
    m.total_queries = metrics_collector_.query_count;
    m.total_matches = metrics_collector_.match_count;
    return m;
}

void S3JMethod::forceAdapt() { }
int S3JMethod::otherIndexId(int slot) const { return (slot == 0) ? right_index_id_ : left_index_id_; }
void S3JMethod::maybeAdapt() { }
std::pair<const float*, size_t> S3JMethod::getRawVectorView(const VectorRecord& record) const {
    return {reinterpret_cast<const float*>(record.data_.data_.get()), static_cast<size_t>(record.data_.dim_)};
}

} // namespace sageFlow

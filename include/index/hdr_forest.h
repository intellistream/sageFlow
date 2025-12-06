#pragma once

#include "index/index.h"
#include "index/hdr_tree.h"
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <set>
#include <cmath>
#include <algorithm>

namespace sageFlow {

// Placeholder for PCA projection matrix/state
struct PCAState {
    std::vector<std::vector<float>> projection_matrix;
    std::vector<float> mean;
    
    // Helper to project a vector
    std::vector<float> project(const std::vector<float>& vec) const {
        if (vec.empty() || projection_matrix.empty()) return {};
        // Simple matrix multiplication placeholder
        // In real impl, use BLAS or similar
        std::vector<float> result(projection_matrix.size(), 0.0f);
        for (size_t i = 0; i < projection_matrix.size(); ++i) {
            for (size_t j = 0; j < vec.size(); ++j) {
                if (j < projection_matrix[i].size()) {
                    result[i] += (vec[j] - (j < mean.size() ? mean[j] : 0)) * projection_matrix[i][j];
                }
            }
        }
        return result;
    }
};

struct LocalHDRTree {
    int tree_id;
    PCAState pca_state;
    
    // Users managed by this local tree
    std::set<uint64_t> user_ids;
    
    // The actual R-Tree structure for this local partition
    std::shared_ptr<HDRTree> rtree_index;

    // Bounds for pruning (distance from cluster center)
    float min_dist = 0.0f;
    float max_dist = 0.0f;
    float max_dknn = 0.0f; // Max kNN distance in this tree
    
    bool is_dirty = false; // For lazy updates
    
    // Center of the section/cluster
    std::vector<float> center;
};

class HDRForest : public Index {
public:
    HDRForest(int n_clusters = 10, int f_sections = 5) : n_clusters_(n_clusters), f_sections_(f_sections) {
        index_type_ = IndexType::HDRForest;
    }
    ~HDRForest() override = default;

    auto insert(uint64_t id) -> bool override;
    auto erase(uint64_t id) -> bool override;
    auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
    auto query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> override;

    // Specific methods for HDR Forest
    void build_forest(const std::vector<std::shared_ptr<VectorRecord>>& initial_data); 
    
private:
    std::vector<std::shared_ptr<LocalHDRTree>> forest_;
    std::vector<std::vector<float>> cluster_centroids_;
    
    // RkNN Table for Deletions: Item ID -> List of User IDs
    std::unordered_map<uint64_t, std::vector<uint64_t>> rknn_table_; 
    
    // PCA Precomputation Cache
    // Map from Item ID -> (Tree ID -> Projected Vector)
    // Note: Paper mentions "Layer", but here we simplify to Tree level or assume 1 layer for local tree
    std::unordered_map<uint64_t, std::unordered_map<int, std::vector<float>>> pca_cache_;
    
    std::mutex mutex_;
    int n_clusters_;
    int f_sections_;

    // Helper for Lazy Updates
    void mark_dirty_path(int tree_id);
    
    // Helper for Batch Updates
    // We might buffer updates and process them
    std::vector<uint64_t> insert_buffer_;
    void process_batch_updates();
    
    // Helper for Pruning-based kNN Recomputation
    // Returns new kNN for the user
    std::vector<uint64_t> recompute_knn(uint64_t user_id, int k);
    
    // Helper to find friend users (users in the same local tree)
    std::vector<uint64_t> get_friend_users(uint64_t user_id);
};

} // namespace sageFlow

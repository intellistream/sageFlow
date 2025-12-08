#pragma once

#include "index/index.h"
#include "index/hdr_tree.h"
#include <vector>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <set>
#include <cmath>
#include <algorithm>

namespace sageFlow {

// PCA 投影矩阵/状态的占位符
struct PCAState {
    std::vector<std::vector<float>> projection_matrix;
    std::vector<float> mean;
    
    // 投影向量的辅助函数
    std::vector<float> project(const std::vector<float>& vec) const {
        if (vec.empty() || projection_matrix.empty()) return {};
        // 简单的矩阵乘法占位符
        // 在实际实现中，应使用 BLAS 或类似库
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
    
    // 由此本地树管理的用户
    std::set<uint64_t> user_ids;
    
    // 此本地分区的实际 R-Tree 结构
    std::shared_ptr<HDRTree> rtree_index;

    // 剪枝边界（距聚类中心的距离）
    float min_dist = 0.0f;
    float max_dist = 0.0f;
    float max_dknn = 0.0f; // 此树中的最大 kNN 距离
    
    bool is_dirty = false; // 用于延迟更新
    
    // 分区/聚类的中心
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

    // HDR Forest 的特定方法
    void build_forest(const std::vector<std::shared_ptr<VectorRecord>>& initial_data); 
    
private:
    std::vector<std::shared_ptr<LocalHDRTree>> forest_;
    std::vector<std::vector<float>> cluster_centroids_;
    
    // 用于删除的 RkNN 表：Item ID -> User IDs 列表
    std::unordered_map<uint64_t, std::vector<uint64_t>> rknn_table_; 
    
    // PCA 预计算缓存
    // 映射：Item ID -> (Tree ID -> 投影向量)
    // 注意：论文提到了 "Layer"，但这里我们简化为 Tree 级别或假设本地树只有 1 层
    std::unordered_map<uint64_t, std::unordered_map<int, std::vector<float>>> pca_cache_;
    std::unordered_map<uint64_t, float> user_dknn_;
    
    std::shared_mutex mutex_;
    int n_clusters_;
    int f_sections_;

    // 延迟更新的辅助函数
    void mark_dirty_path(int tree_id);
    
    // 批量更新的辅助函数
    // 我们可能会缓冲更新并进行处理
    std::vector<uint64_t> insert_buffer_;
    void process_batch_updates();
    
    // 基于剪枝的 kNN 重计算辅助函数
    // 返回用户的新 kNN
    std::vector<uint64_t> recompute_knn(uint64_t user_id, int k);
    
    // 内部无锁查询函数
    std::vector<uint64_t> query_internal(const VectorRecord &record, int k);

    // 查找好友用户（同一本地树中的用户）的辅助函数
    std::vector<uint64_t> get_friend_users(uint64_t user_id);
};

} // namespace sageFlow

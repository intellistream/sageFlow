#include "index/hdr_forest.h"
#include "storage/storage_manager.h"
#include "common/data_types.h"
#include "compute_engine/pca.h"
#include "utils/logger.h"
#include <limits>
#include <cmath>
#include <cstring>
#include <algorithm>
#include <vector>
#include <random>
#include <numeric>
#include <map>
#include <shared_mutex>

namespace sageFlow {

// 计算 L2 距离的辅助函数
float compute_l2_dist(const float* a, const float* b, int dim) {
    float dist = 0.0f;
    for(int i=0; i<dim; ++i) {
        float diff = a[i] - b[i];
        dist += diff * diff;
    }
    return std::sqrt(dist);
}

// K-Means 聚类的辅助函数
std::vector<std::vector<float>> perform_kmeans(
    const std::vector<std::shared_ptr<VectorRecord>>& data, 
    int k, 
    int max_iters = 10) {
    
    if (data.empty()) return {};
    int dim = data[0]->data_.dim_;
    int n = static_cast<int>(data.size());
    if (n < k) k = n;

    // 随机初始化质心
    std::vector<std::vector<float>> centroids(k, std::vector<float>(dim));
    std::vector<int> indices(n);
    std::iota(indices.begin(), indices.end(), 0);
    
    // 简单的随机初始化
    for(int i=0; i<k; ++i) {
        const float* ptr = reinterpret_cast<const float*>(data[indices[i]]->data_.data_.get());
        for(int d=0; d<dim; ++d) centroids[i][d] = ptr[d];
    }

    std::vector<int> assignments(n);
    
    for(int iter=0; iter<max_iters; ++iter) {
        bool changed = false;
        std::vector<std::vector<float>> new_centroids(k, std::vector<float>(dim, 0.0f));
        std::vector<int> counts(k, 0);

        for(int i=0; i<n; ++i) {
            const float* ptr = reinterpret_cast<const float*>(data[i]->data_.data_.get());
            float min_dist = std::numeric_limits<float>::max();
            int best_c = 0;
            
            for(int c=0; c<k; ++c) {
                float dist = compute_l2_dist(ptr, centroids[c].data(), dim);
                if(dist < min_dist) {
                    min_dist = dist;
                    best_c = c;
                }
            }
            
            if(assignments[i] != best_c) {
                assignments[i] = best_c;
                changed = true;
            }
            
            for(int d=0; d<dim; ++d) new_centroids[best_c][d] += ptr[d];
            counts[best_c]++;
        }

        for(int c=0; c<k; ++c) {
            if(counts[c] > 0) {
                for(int d=0; d<dim; ++d) centroids[c][d] /= counts[c];
            }
        }
        
        if(!changed) break;
    }
    
    return centroids;
}

auto HDRForest::insert(uint64_t id) -> bool {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    insert_buffer_.push_back(id);
    
    // 为了测试目的立即处理
    process_batch_updates();
    return true;
}

void HDRForest::process_batch_updates() {
    // 如果森林为空（未构建），创建一个默认的单树作为回退
    if (forest_.empty()) {
         auto tree = std::make_shared<LocalHDRTree>();
         tree->tree_id = 0;
         tree->min_dist = 0.0f;
         tree->max_dist = std::numeric_limits<float>::max();
         
         // 初始化 HDRTree
         if (storage_manager_) {
             // 假设维度为 128 或从数据中获取
             int dim = 128; 
             if (!insert_buffer_.empty()) {
                 auto rec = storage_manager_->getVectorByUid(insert_buffer_[0]);
                 if (rec) dim = rec->data_.dim_;
             }
             
             HDRTree::Config config;
             config.projected_dim = std::min(dim, 24); // Revert to 16
             config.pca_sample_size = std::max(10, dim * 2);
             config.distance_bound_ratio = 4.0f;
             tree->rtree_index = std::make_shared<HDRTree>(dim, config);
             tree->rtree_index->storage_manager_ = storage_manager_;
         }

         // 如果可能，尝试将中心初始化为 0 向量
         if (!insert_buffer_.empty() && storage_manager_) {
             auto rec = storage_manager_->getVectorByUid(insert_buffer_[0]);
             if (rec) {
                 tree->center.resize(rec->data_.dim_, 0.0f);
                 cluster_centroids_.push_back(tree->center);
             }
         }
         forest_.push_back(tree);
    }

    for (auto item_id : insert_buffer_) {
        auto rec = storage_manager_->getVectorByUid(item_id);
        if (!rec) { SAGEFLOW_LOG_WARN("HDRForest", "Vector record not found for uid={}", item_id); continue; }
        
        const float* vec_data = reinterpret_cast<const float*>(rec->data_.data_.get());
        int dim = rec->data_.dim_;

        // 1. 寻找最近的聚类中心（路由）
        int best_cluster = 0;
        float min_dist_to_center = std::numeric_limits<float>::max();
        
        if (!cluster_centroids_.empty()) {
            for(size_t c=0; c<cluster_centroids_.size(); ++c) {
                float d = compute_l2_dist(vec_data, cluster_centroids_[c].data(), dim);
                if (d < min_dist_to_center) {
                    min_dist_to_center = d;
                    best_cluster = static_cast<int>(c);
                }
            }
        }

        // 2. 在该聚类中寻找对应的分区
        size_t start_idx = static_cast<size_t>(best_cluster * f_sections_);
        size_t end_idx = start_idx + f_sections_;
        
        if (start_idx >= forest_.size()) {
            // 索引越界回退
            if (forest_[0]->rtree_index) forest_[0]->rtree_index->insert(item_id);
            forest_[0]->user_ids.insert(item_id);
            continue;
        }

        std::shared_ptr<LocalHDRTree> target_tree = nullptr;
        for(size_t i=start_idx; i<end_idx && i<forest_.size(); ++i) {
            auto& tree = forest_[i];
            if (min_dist_to_center >= tree->min_dist && min_dist_to_center <= tree->max_dist) {
                target_tree = tree;
                break;
            }
        }
        
        // 如果超出所有范围，放入该聚类的最后一个树中
        if (!target_tree && end_idx > 0) {
            size_t last_tree_idx = std::min(forest_.size()-1, end_idx-1);
            target_tree = forest_[last_tree_idx];
        }
        
        if (target_tree) {
            target_tree->user_ids.insert(item_id);
            target_tree->is_dirty = true;

            if (target_tree->rtree_index) {
                // 优化：移除 pca_cache_，直接插入
                target_tree->rtree_index->insert(item_id);
            }
            
            if (min_dist_to_center > target_tree->max_dist) {
                target_tree->max_dist = min_dist_to_center;
            }
            recompute_knn(item_id, 20);
        }
    }
    insert_buffer_.clear();
}

void HDRForest::build_forest(const std::vector<std::shared_ptr<VectorRecord>>& initial_data) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    forest_.clear();
    cluster_centroids_.clear();
    
    if (initial_data.empty()) return;

    // 1. 聚类
    cluster_centroids_ = perform_kmeans(initial_data, n_clusters_);
    
    // 2. 将点分配给聚类
    struct PointInfo {
        int index;
        float dist;
    };
    std::vector<std::vector<PointInfo>> cluster_points(cluster_centroids_.size());
    
    int dim = initial_data[0]->data_.dim_;
    for(size_t i=0; i<initial_data.size(); ++i) {
        const float* ptr = reinterpret_cast<const float*>(initial_data[i]->data_.data_.get());
        
        int best_c = 0;
        float min_d = std::numeric_limits<float>::max();
        for(size_t c=0; c<cluster_centroids_.size(); ++c) {
            float d = compute_l2_dist(ptr, cluster_centroids_[c].data(), dim);
            if(d < min_d) {
                min_d = d;
                best_c = static_cast<int>(c);
            }
        }
        cluster_points[best_c].push_back({static_cast<int>(i), min_d});
    }
    
    // 3. 创建分区并训练本地 PCA
    int tree_id_counter = 0;
    for(size_t c=0; c<cluster_centroids_.size(); ++c) {
        auto& points = cluster_points[c];
        
        // 按到中心的距离排序
        std::sort(points.begin(), points.end(), [](const PointInfo& a, const PointInfo& b){
            return a.dist < b.dist;
        });
        
        int total_points = static_cast<int>(points.size());
        int section_size = (total_points + f_sections_ - 1) / f_sections_;
        if (section_size == 0) section_size = 1;
        
        for(int s=0; s<f_sections_; ++s) {
            auto tree = std::make_shared<LocalHDRTree>();
            tree->tree_id = tree_id_counter++;
            tree->center = cluster_centroids_[c];
            
            // 初始化 HDRTree
            HDRTree::Config config;
            config.projected_dim = std::min(dim, 24);
            config.pca_sample_size = std::max(10, dim * 2);
            config.distance_bound_ratio = 4.0f;
            tree->rtree_index = std::make_shared<HDRTree>(dim, config);
            tree->rtree_index->storage_manager_ = storage_manager_;
            
            int start = s * section_size;
            int end = std::min(start + section_size, total_points);
            
            std::vector<std::vector<float>> training_samples;
            
            if (start >= total_points) {
                // 空分区
                tree->min_dist = (s==0) ? 0.0f : forest_.back()->max_dist;
                tree->max_dist = tree->min_dist;
            } else {
                tree->min_dist = points[start].dist;
                tree->max_dist = points[end-1].dist;
                
                for(int k=start; k<end; ++k) {
                    auto& rec = initial_data[points[k].index];
                    tree->user_ids.insert(rec->uid_);
                    
                    // 收集训练样本
                    std::vector<float> vec(dim);
                    const float* ptr = reinterpret_cast<const float*>(rec->data_.data_.get());
                    std::copy(ptr, ptr + dim, vec.begin());
                    training_samples.push_back(std::move(vec));
                }
            }
            
            // 训练本地 PCA 并插入数据
            if (!training_samples.empty() && training_samples.size() >= static_cast<size_t>(config.pca_sample_size)) {
                tree->rtree_index->trainPCA(training_samples);
                for(int k=start; k<end; ++k) {
                    tree->rtree_index->insert(initial_data[points[k].index]->uid_);
                }
            }
            
            // 确保覆盖连续性
            if (s == 0) tree->min_dist = 0.0f;
            if (s == f_sections_ - 1) tree->max_dist = std::numeric_limits<float>::max();
            
            forest_.push_back(tree);
        }
    }
}

auto HDRForest::erase(uint64_t id) -> bool {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // RkNN 表：加速删除
    // 优化：使用 vector
    if (id < rknn_table_.size() && !rknn_table_[id].empty()) {
        int default_k = 20; // 默认维护的 k 值
        for (auto uid : rknn_table_[id]) {
            recompute_knn(uid, default_k); 
        }
        rknn_table_[id].clear();
    }
    
    // 清理 user_dknn_
    if (id < user_dknn_.size()) {
        user_dknn_[id] = 0.0f;
    }

    for(auto& tree : forest_) {
        tree->user_ids.erase(id);
        if (tree->rtree_index) {
            tree->rtree_index->erase(id);
        }
    }
    
    auto it = std::remove(insert_buffer_.begin(), insert_buffer_.end(), id);
    insert_buffer_.erase(it, insert_buffer_.end());
    
    return true;
}

auto HDRForest::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return query_internal(record, k);
}

auto HDRForest::query_internal(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    std::vector<uint64_t> result;
    
    if (!storage_manager_) { SAGEFLOW_LOG_ERROR("HDRForest", "Storage manager not set in query_internal"); return result; }
    
    // 收集所有候选
    std::vector<uint64_t> all_candidates;
    
    // 遍历所有树
    for(const auto& tree : forest_) {
        // 剪枝逻辑（定理 4）
        if (!tree->center.empty()) {
            const float* query_data = reinterpret_cast<const float*>(record.data_.data_.get());
            float dist_to_center = compute_l2_dist(query_data, tree->center.data(), record.data_.dim_);
            
            // 仅当 max_dknn 设置（非零）时剪枝
            if (tree->max_dknn > 0) {
                if (dist_to_center > tree->max_dist + tree->max_dknn || 
                    dist_to_center < tree->min_dist - tree->max_dknn) {
                    continue; // 已剪枝！
                }
            }
        }

        if (tree->rtree_index && tree->rtree_index->isPCATrained()) {
            // 优化：即时投影，不使用缓存
            std::vector<float> projected = tree->rtree_index->projectVector(record.data_);
            auto local_results = tree->rtree_index->query(record, projected, k);
            all_candidates.insert(all_candidates.end(), local_results.begin(), local_results.end());
        } else {
            // 回退到此树的全扫描
            all_candidates.insert(all_candidates.end(), tree->user_ids.begin(), tree->user_ids.end());
        }
    }
    all_candidates.insert(all_candidates.end(), insert_buffer_.begin(), insert_buffer_.end());
    
    // 去重
    std::sort(all_candidates.begin(), all_candidates.end());
    all_candidates.erase(std::unique(all_candidates.begin(), all_candidates.end()), all_candidates.end());
    
    // 全局验证和排序
    std::vector<std::pair<float, uint64_t>> final_candidates;
    const float* query_data = reinterpret_cast<const float*>(record.data_.data_.get());
    int dim = record.data_.dim_;
    
    for(auto uid : all_candidates) {
        auto rec_ptr = storage_manager_->getVectorByUid(uid);
        if(rec_ptr) {
            const float* vec_data = reinterpret_cast<const float*>(rec_ptr->data_.data_.get());
            float dist = compute_l2_dist(query_data, vec_data, dim);
            final_candidates.push_back({dist, uid});
        }
    }
    
    std::sort(final_candidates.begin(), final_candidates.end());
    
    for(int i=0; i<k && i<static_cast<int>(final_candidates.size()); ++i) {
        result.push_back(final_candidates[i].second);
    }
    
    return result;
}

auto HDRForest::query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<uint64_t> results;
    
    for (const auto& tree : forest_) {

        if (tree->rtree_index && tree->rtree_index->isPCATrained()) {
            // 优化：即时投影
            std::vector<float> projected = tree->rtree_index->projectVector(record.data_);
            auto local_results = tree->rtree_index->query_for_join(record, projected, join_similarity_threshold);
            results.insert(results.end(), local_results.begin(), local_results.end());
        } else {
            // 回退
            for (auto uid : tree->user_ids) {
                if (storage_manager_ && storage_manager_->engine_) {
                    auto rec = storage_manager_->getVectorByUid(uid);
                    if (rec) {
                        float sim = storage_manager_->engine_->Similarity(record.data_, rec->data_);
                        if (sim >= join_similarity_threshold) {
                            results.push_back(uid);
                        }
                    }
                } else {
                    results.push_back(uid);
                }
            }
        }
    }
    
    for(auto uid : insert_buffer_) {
        if (storage_manager_ && storage_manager_->engine_) {
            auto rec = storage_manager_->getVectorByUid(uid);
            if (rec) {
                float sim = storage_manager_->engine_->Similarity(record.data_, rec->data_);
                if (sim >= join_similarity_threshold) {
                    results.push_back(uid);
                }
            }
        } else {
            results.push_back(uid);
        }
    }
    
    std::sort(results.begin(), results.end());
    results.erase(std::unique(results.begin(), results.end()), results.end());
    
    return results;
}

std::vector<uint64_t> HDRForest::recompute_knn(uint64_t user_id, int k) {
    if (!storage_manager_) { SAGEFLOW_LOG_ERROR("HDRForest", "Storage manager not set in recompute_knn"); return {}; }
    auto rec = storage_manager_->getVectorByUid(user_id);
    if (!rec) return {};
    
    // 使用内部无锁查询，因为 recompute_knn 通常在持有锁的情况下调用
    auto results = query_internal(*rec, k);
    
    // 更新 RkNN 表 (Vector)
    for (auto item_id : results) {
        if (item_id >= rknn_table_.size()) {
            rknn_table_.resize(item_id + 1000); // 预分配
        }
        rknn_table_[item_id].push_back(user_id);
    }

    // Update max_dknn
    if (!results.empty()) {
        auto kth_id = results.back();
        auto kth_rec = storage_manager_->getVectorByUid(kth_id);
        if (kth_rec) {
            float dist = compute_l2_dist(
                reinterpret_cast<const float*>(rec->data_.data_.get()),
                reinterpret_cast<const float*>(kth_rec->data_.data_.get()),
                rec->data_.dim_
            );
            
            if (user_id >= user_dknn_.size()) {
                user_dknn_.resize(user_id + 1000, 0.0f);
            }
            user_dknn_[user_id] = dist;
            
            for(auto& tree : forest_) {
                if (tree->user_ids.count(user_id)) {
                    tree->max_dknn = std::max(tree->max_dknn, dist);
                    break;
                }
            }
        }
    }
    
    return results;
}

std::vector<uint64_t> HDRForest::get_friend_users(uint64_t user_id) {
    for (const auto& tree : forest_) {
        if (tree->user_ids.count(user_id)) {
            std::vector<uint64_t> friends(tree->user_ids.begin(), tree->user_ids.end());
            return friends;
        }
    }
    return {};
}

} // namespace sageFlow

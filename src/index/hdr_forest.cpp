#include "index/hdr_forest.h"
#include "storage/storage_manager.h"
#include "common/data_types.h"
#include <iostream>
#include <limits>
#include <cmath>
#include <cstring>
#include <algorithm>
#include <vector>
#include <random>
#include <numeric>
#include <map>

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

// K-Means 聚类辅助函数
std::vector<std::vector<float>> perform_kmeans(
    const std::vector<std::shared_ptr<VectorRecord>>& data, 
    int k, 
    int max_iters = 10) {
    
    if (data.empty() || k <= 0) return {};
    int dim = data[0]->data_.dim_;
    int n = static_cast<int>(data.size());
    if (n < k) k = n;

    // 随机初始化质心
    std::vector<std::vector<float>> centroids(k, std::vector<float>(dim));
    std::vector<int> indices(n);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937{std::random_device{}()});
    
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
    std::lock_guard<std::mutex> lock(mutex_);
    insert_buffer_.push_back(id);
    
    // 立即处理以支持测试
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
         
         // 尝试初始化中心为 0 向量
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
        if (!rec) continue;
        
        const float* vec_data = reinterpret_cast<const float*>(rec->data_.data_.get());
        int dim = rec->data_.dim_;

        // 1. 找到最近的簇中心 (Routing)
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

        // 2. 找到该簇中对应的分段 (Section)
        // 假设森林按簇顺序排列：[c * f, (c+1) * f)
        size_t start_idx = static_cast<size_t>(best_cluster * f_sections_);
        size_t end_idx = start_idx + f_sections_;
        
        if (start_idx >= forest_.size()) {
            // 索引越界回退
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
        
        // 如果超出所有范围（通常是大于最后一个 max_dist），放入该簇的最后一个树
        if (!target_tree && end_idx > 0) {
            size_t last_tree_idx = std::min(forest_.size()-1, end_idx-1);
            target_tree = forest_[last_tree_idx];
        }
        
        if (target_tree) {
            target_tree->user_ids.insert(item_id);
            // 动态扩展边界
            if (min_dist_to_center > target_tree->max_dist) {
                target_tree->max_dist = min_dist_to_center;
            }
        }
    }
    insert_buffer_.clear();
}

void HDRForest::build_forest(const std::vector<std::shared_ptr<VectorRecord>>& initial_data) {
    std::lock_guard<std::mutex> lock(mutex_);
    forest_.clear();
    cluster_centroids_.clear();
    
    if (initial_data.empty()) return;

    // 1. 聚类 (Clustering)
    cluster_centroids_ = perform_kmeans(initial_data, n_clusters_);
    
    // 2. 将点分配给簇
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
    
    // 3. 创建分段 (Sectioning)
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
            
            int start = s * section_size;
            int end = std::min(start + section_size, total_points);
            
            if (start >= total_points) {
                // 空分段
                tree->min_dist = (s==0) ? 0.0f : forest_.back()->max_dist;
                tree->max_dist = tree->min_dist;
            } else {
                tree->min_dist = points[start].dist;
                tree->max_dist = points[end-1].dist;
                
                for(int k=start; k<end; ++k) {
                    tree->user_ids.insert(initial_data[points[k].index]->uid_);
                }
            }
            
            // 确保覆盖范围连续
            if (s == 0) tree->min_dist = 0.0f;
            if (s == f_sections_ - 1) tree->max_dist = std::numeric_limits<float>::max();
            
            forest_.push_back(tree);
        }
    }
}

auto HDRForest::erase(uint64_t id) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for(auto& tree : forest_) {
        tree->user_ids.erase(id);
    }
    
    auto it = std::remove(insert_buffer_.begin(), insert_buffer_.end(), id);
    insert_buffer_.erase(it, insert_buffer_.end());
    
    return true;
}

auto HDRForest::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint64_t> result;
    
    if (!storage_manager_) return result;
    
    std::vector<std::pair<float, uint64_t>> candidates;
    
    const float* query_data = reinterpret_cast<const float*>(record.data_.data_.get());
    int dim = record.data_.dim_;
    
    // 收集所有 ID (目前仍遍历所有树，后续可优化剪枝)
    std::vector<uint64_t> all_ids;
    for(const auto& tree : forest_) {
        all_ids.insert(all_ids.end(), tree->user_ids.begin(), tree->user_ids.end());
    }
    all_ids.insert(all_ids.end(), insert_buffer_.begin(), insert_buffer_.end());
    
    std::sort(all_ids.begin(), all_ids.end());
    all_ids.erase(std::unique(all_ids.begin(), all_ids.end()), all_ids.end());
    
    for(auto uid : all_ids) {
        auto rec_ptr = storage_manager_->getVectorByUid(uid);
        if(rec_ptr) {
            const float* vec_data = reinterpret_cast<const float*>(rec_ptr->data_.data_.get());
            float dist = compute_l2_dist(query_data, vec_data, dim);
            candidates.push_back({dist, uid});
        }
    }
    
    std::sort(candidates.begin(), candidates.end());
    
    for(int i=0; i<k && i<static_cast<int>(candidates.size()); ++i) {
        result.push_back(candidates[i].second);
    }
    
    return result;
}

auto HDRForest::query_for_join(const VectorRecord &record, double join_similarity_threshold) -> std::vector<uint64_t> {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint64_t> results;
    
    for (const auto& tree : forest_) {
        for (auto uid : tree->user_ids) {
            results.push_back(uid);
        }
    }
    
    for(auto uid : insert_buffer_) {
        results.push_back(uid);
    }
    
    std::sort(results.begin(), results.end());
    results.erase(std::unique(results.begin(), results.end()), results.end());
    
    return results;
}

std::vector<uint64_t> HDRForest::recompute_knn(uint64_t user_id, int k) {
    return {}; 
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

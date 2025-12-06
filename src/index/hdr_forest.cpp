#include "index/hdr_forest.h"
#include "storage/storage_manager.h"
#include "common/data_types.h"
#include <iostream>
#include <limits>
#include <cmath>
#include <cstring>
#include <algorithm>
#include <vector>

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

auto HDRForest::insert(uint64_t id) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    insert_buffer_.push_back(id);
    
    // 为了测试正确性，立即处理，不等待批量更新
    process_batch_updates();
    return true;
}

void HDRForest::process_batch_updates() {
    if (forest_.empty()) {
        auto tree = std::make_shared<LocalHDRTree>();
        tree->tree_id = 0;
        // 初始化边界，避免初始时修剪所有内容
        tree->min_dist = 0.0f;
        tree->max_dist = std::numeric_limits<float>::max();
        tree->max_dknn = std::numeric_limits<float>::max(); 
        forest_.push_back(tree);
    }
    
    auto& tree = forest_[0];
    for (auto item_id : insert_buffer_) {
        tree->user_ids.insert(item_id);
    }
    insert_buffer_.clear();
}

auto HDRForest::erase(uint64_t id) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 从森林中移除
    for(auto& tree : forest_) {
        tree->user_ids.erase(id);
    }
    
    // 如果存在于缓冲区中，则移除
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
    
    // 收集所有 ID
    std::vector<uint64_t> all_ids;
    for(const auto& tree : forest_) {
        all_ids.insert(all_ids.end(), tree->user_ids.begin(), tree->user_ids.end());
    }
    all_ids.insert(all_ids.end(), insert_buffer_.begin(), insert_buffer_.end());
    
    // 去重
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
    
    // 按距离排序
    std::sort(candidates.begin(), candidates.end());
    
    // 返回前 k 个结果
    for(int i=0; i<k && i<candidates.size(); ++i) {
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
    
    // 同时添加缓冲区中的内容
    for(auto uid : insert_buffer_) {
        results.push_back(uid);
    }
    
    // 去重
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

void HDRForest::build_forest(const std::vector<std::shared_ptr<VectorRecord>>& initial_data) {
    auto tree = std::make_shared<LocalHDRTree>();
    tree->tree_id = 0;
    for (const auto& rec : initial_data) {
        tree->user_ids.insert(rec->uid_);
    }
    
    // 初始化边界
    tree->min_dist = 0.0f;
    tree->max_dist = std::numeric_limits<float>::max();
    tree->max_dknn = std::numeric_limits<float>::max();
    
    forest_.push_back(tree);
}

} // namespace sageFlow

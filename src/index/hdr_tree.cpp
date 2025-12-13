#include "index/hdr_tree.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <queue>
#include <stdexcept>
#include <numeric>
#include <iostream>

#include "utils/logger.h"

namespace sageFlow {

// ============================================================================
// RTreeNode 实现
// ============================================================================

HDRTree::RTreeNode::RTreeNode(int dim)
    : mbr_low(dim, std::numeric_limits<float>::max()),
      mbr_high(dim, std::numeric_limits<float>::lowest()),
      is_leaf(true) {}

auto HDRTree::RTreeNode::area() const -> double {
  double area = 1.0;
  for (size_t i = 0; i < mbr_low.size(); ++i) {
    double width = static_cast<double>(mbr_high[i]) - static_cast<double>(mbr_low[i]);
    // 防止宽度为0导致面积为0，影响计算
    if (width < 0) return 0.0; 
    area *= width;
  }
  return area;
}

auto HDRTree::RTreeNode::enlargement(const std::vector<float>& point) const -> double {
  double new_area = 1.0;
  double current_area = 1.0;
  
  for (size_t i = 0; i < mbr_low.size(); ++i) {
    double width = static_cast<double>(mbr_high[i]) - static_cast<double>(mbr_low[i]);
    if (width < 0) width = 0;
    current_area *= width;

    double new_low = std::min(static_cast<double>(mbr_low[i]), static_cast<double>(point[i]));
    double new_high = std::max(static_cast<double>(mbr_high[i]), static_cast<double>(point[i]));
    new_area *= (new_high - new_low);
  }
  return new_area - current_area;
}

auto HDRTree::RTreeNode::enlargement(const RTreeNode& other) const -> double {
  double new_area = 1.0;
  double current_area = 1.0;

  for (size_t i = 0; i < mbr_low.size(); ++i) {
    double width = static_cast<double>(mbr_high[i]) - static_cast<double>(mbr_low[i]);
    if (width < 0) width = 0;
    current_area *= width;

    double new_low = std::min(static_cast<double>(mbr_low[i]), static_cast<double>(other.mbr_low[i]));
    double new_high = std::max(static_cast<double>(mbr_high[i]), static_cast<double>(other.mbr_high[i]));
    new_area *= (new_high - new_low);
  }
  return new_area - current_area;
}

auto HDRTree::RTreeNode::intersects(const std::vector<float>& query, float threshold) const
    -> bool {
  float min_dist_sq = 0.0F;
  for (size_t i = 0; i < query.size(); ++i) {
    if (query[i] < mbr_low[i]) {
      float d = mbr_low[i] - query[i];
      min_dist_sq += d * d;
    } else if (query[i] > mbr_high[i]) {
      float d = query[i] - mbr_high[i];
      min_dist_sq += d * d;
    }
  }
  return min_dist_sq <= threshold * threshold;
}

void HDRTree::RTreeNode::expandMBR(const std::vector<float>& point) {
  for (size_t i = 0; i < point.size(); ++i) {
    mbr_low[i] = std::min(mbr_low[i], point[i]);
    mbr_high[i] = std::max(mbr_high[i], point[i]);
  }
}

void HDRTree::RTreeNode::expandMBR(const RTreeNode& other) {
  for (size_t i = 0; i < mbr_low.size(); ++i) {
    mbr_low[i] = std::min(mbr_low[i], other.mbr_low[i]);
    mbr_high[i] = std::max(mbr_high[i], other.mbr_high[i]);
  }
}

// ============================================================================
// HDRTree 实现
// ============================================================================

HDRTree::HDRTree(int dimension, const Config& config)
    : config_(config) {
  dimension_ = dimension;
  index_type_ = IndexType::HDRTree;

  if (config_.projected_dim > dimension) {
    throw std::invalid_argument("Projected dimension cannot exceed original dimension");
  }
  if (config_.projected_dim <= 0) {
    throw std::invalid_argument("Projected dimension must be positive");
  }

  pca_ = std::make_unique<PCA>(dimension, config_.projected_dim);
  sample_buffer_.reserve(config_.pca_sample_size);

  SAGEFLOW_LOG_DEBUG("HDRTree", "Created HDRTree: dim={}, projected_dim={}",
                     dimension, config_.projected_dim);
}

void HDRTree::trainPCA(const std::vector<std::vector<float>>& samples) {
  std::unique_lock lock(mutex_);
  if (samples.empty()) throw std::invalid_argument("Training samples cannot be empty");

  pca_->fit(samples);
  pca_training_done_ = true;
  rtree_root_ = std::make_unique<RTreeNode>(config_.projected_dim);

  SAGEFLOW_LOG_INFO("HDRTree", "PCA trained with {} samples", samples.size());
}

void HDRTree::tryAutoTrainPCA() {
  if (pca_training_done_) return;

  if (static_cast<int>(sample_buffer_.size()) >= config_.pca_sample_size) {
    SAGEFLOW_LOG_INFO("HDRTree", "Auto-training PCA with {} samples", sample_buffer_.size());
    pca_->fit(sample_buffer_);
    pca_training_done_ = true;
    rtree_root_ = std::make_unique<RTreeNode>(config_.projected_dim);
    
    for (size_t i = 0; i < sample_buffer_.size(); ++i) {
        auto projected = pca_->transform(sample_buffer_[i]);
        uint64_t uid = sample_uids_[i];
        uid_to_projected_[uid] = projected;
        insertToRTree(uid, projected);
    }

    sample_buffer_.clear();
    sample_buffer_.shrink_to_fit();
    sample_uids_.clear();
    sample_uids_.shrink_to_fit();
  }
}

auto HDRTree::extractFloatVector(const VectorData& data) -> std::vector<float> {
  if (!data.data_) throw std::runtime_error("VectorData data_ is null");
  std::vector<float> result(data.dim_);

  switch (data.type_) {
    case DataType::Float32: {
      const auto* ptr = reinterpret_cast<const float*>(data.data_.get());
      for (int i = 0; i < data.dim_; ++i) result[i] = ptr[i];
      break;
    }
    case DataType::Float64: {
      const auto* ptr = reinterpret_cast<const double*>(data.data_.get());
      for (int i = 0; i < data.dim_; ++i) result[i] = static_cast<float>(ptr[i]);
      break;
    }
    case DataType::Int32: {
      const auto* ptr = reinterpret_cast<const int32_t*>(data.data_.get());
      for (int i = 0; i < data.dim_; ++i) result[i] = static_cast<float>(ptr[i]);
      break;
    }
    default:
      throw std::runtime_error("Unsupported data type for HDRTree");
  }
  return result;
}

auto HDRTree::projectVector(const VectorData& data) const -> std::vector<float> {
  if (!pca_ || !pca_->isFitted()) throw std::runtime_error("PCA not trained yet");
  auto vec = extractFloatVector(data);
  return pca_->transform(vec);
}

auto HDRTree::insert(uint64_t uid) -> bool {
  return insert(uid, {});
}

auto HDRTree::insert(uint64_t uid, const std::vector<float>& precomputed_projection) -> bool {
  if (!storage_manager_) return false;

  std::unique_lock lock(mutex_);

  if (!pca_training_done_) {
    auto record = storage_manager_->getVectorByUid(uid);
    if (!record) return false;

    auto vec = extractFloatVector(record->data_);
    if (static_cast<int>(sample_buffer_.size()) < config_.pca_sample_size) {
      sample_buffer_.push_back(vec);
      sample_uids_.push_back(uid);
    }
    tryAutoTrainPCA();
    if (!pca_training_done_) return true; // 暂存缓冲区，等待训练
  }

  std::vector<float> projected;
  if (!precomputed_projection.empty()) {
    projected = precomputed_projection;
  } else {
    auto record = storage_manager_->getVectorByUid(uid);
    if (!record) return false;
    projected = projectVector(record->data_);
  }

  uid_to_projected_[uid] = projected;
  insertToRTree(uid, projected);
  return true;
}

void HDRTree::insertToRTree(uint64_t uid, const std::vector<float>& projected) {
  if (!rtree_root_) {
    rtree_root_ = std::make_unique<RTreeNode>(config_.projected_dim);
  }

  auto new_node = insertRecursive(rtree_root_.get(), uid, projected);

  // 如果根节点分裂，创建新根
  if (new_node) {
    auto new_root = std::make_unique<RTreeNode>(config_.projected_dim);
    new_root->is_leaf = false;
    new_root->expandMBR(*rtree_root_);
    new_root->expandMBR(*new_node);
    new_root->children.push_back(std::move(rtree_root_));
    new_root->children.push_back(std::move(new_node));
    rtree_root_ = std::move(new_root);
  }
}

std::unique_ptr<HDRTree::RTreeNode> HDRTree::insertRecursive(RTreeNode* node, uint64_t uid, const std::vector<float>& point)
 {                                                                                                                            
  if (!node) {
      SAGEFLOW_LOG_ERROR("HDRTree", "insertRecursive called with null node");
      return nullptr;
  }

  // 2. 如果是叶子节点，直接插入
  if (node->is_leaf) {
    node->entries.push_back(uid);
    if (static_cast<int>(node->entries.size()) > config_.rtree_max_entries) {
      return splitLeafNode(node);
    }
    return nullptr;
  }

  // 3. 如果是内部节点，选择最佳子节点下探
  RTreeNode* best_child = chooseLeaf(node, point);
  if (!best_child) {
      SAGEFLOW_LOG_ERROR("HDRTree", "chooseLeaf returned null! node={}, children={}", (void*)node, node->children.size());
      if (!node->children.empty()) {
          best_child = node->children[0].get();
      } else {
          return nullptr; 
      }
  }
  
  auto new_child = insertRecursive(best_child, uid, point);

  // 4. 如果子节点分裂，将新节点加入当前节点
  if (new_child) {
    node->children.push_back(std::move(new_child));
    if (static_cast<int>(node->children.size()) > config_.rtree_max_entries) {
      return splitInternalNode(node);
    }
  }

  return nullptr;
}

auto HDRTree::chooseLeaf(RTreeNode* node, const std::vector<float>& point) -> RTreeNode* {
  if (node->is_leaf) return node;
  if (node->children.empty()) return nullptr;

  double min_enlargement = std::numeric_limits<double>::max();
  double min_area = std::numeric_limits<double>::max();
  RTreeNode* best_child = nullptr;

  for (size_t i = 0; i < node->children.size(); ++i) {
    const auto& child = node->children[i];
    if (!child) {
        SAGEFLOW_LOG_ERROR("HDRTree", "Null child in chooseLeaf! index={}", i);
        continue;
    }
    double enlargement = child->enlargement(point);
    double area = child->area();

    if (std::isinf(enlargement) || std::isnan(enlargement)) enlargement = std::numeric_limits<double>::max();
    if (std::isinf(area) || std::isnan(area)) area = std::numeric_limits<double>::max();

    if (!best_child || enlargement < min_enlargement) {
      min_enlargement = enlargement;
      min_area = area;
      best_child = child.get();
    } else if (std::abs(enlargement - min_enlargement) < 1e-9 && area < min_area) {
      min_area = area;
      best_child = child.get();
    }
  }
  
  if (!best_child && !node->children.empty()) {
      best_child = node->children[0].get();
  }
  
  return best_child;
}

std::unique_ptr<HDRTree::RTreeNode> HDRTree::splitLeafNode(RTreeNode* node) {
  auto new_node = std::make_unique<RTreeNode>(config_.projected_dim);
  new_node->is_leaf = true;

  const auto& entries = node->entries;
  if (entries.size() < 2) return nullptr;

  // 1. 选择种子节点
  size_t seed1 = 0, seed2 = 1;
  float max_wasted_area = -1.0F;

  for (size_t i = 0; i < entries.size(); ++i) {
    for (size_t j = i + 1; j < entries.size(); ++j) {
      if (uid_to_projected_.find(entries[i]) == uid_to_projected_.end()) continue;
      if (uid_to_projected_.find(entries[j]) == uid_to_projected_.end()) continue;
      
      float dist = euclideanDistance(uid_to_projected_[entries[i]], uid_to_projected_[entries[j]]);
      if (dist > max_wasted_area) {
        max_wasted_area = dist;
        seed1 = i;
        seed2 = j;
      }
    }
  }

  std::vector<uint64_t> group1, group2;

  // 优化：如果所有点重合 (max_wasted_area ~ 0)，强制平分
  if (max_wasted_area <= std::numeric_limits<float>::epsilon()) {
      size_t mid = entries.size() / 2;
      for(size_t i=0; i<entries.size(); ++i) {
          if (i < mid) group1.push_back(entries[i]);
          else group2.push_back(entries[i]);
      }
      
      if (!group1.empty()) {
          if (uid_to_projected_.count(group1[0])) {
            node->mbr_low = uid_to_projected_[group1[0]];
            node->mbr_high = node->mbr_low;
          }
      }
      if (!group2.empty()) {
          if (uid_to_projected_.count(group2[0])) {
            new_node->mbr_low = uid_to_projected_[group2[0]];
            new_node->mbr_high = new_node->mbr_low;
          }
      }
      
      node->entries = std::move(group1);
      new_node->entries = std::move(group2);
      return new_node;
  }

  group1.push_back(entries[seed1]);
  group2.push_back(entries[seed2]);

  // 初始化 MBR
  std::vector<float> mbr1_low = uid_to_projected_[entries[seed1]];
  std::vector<float> mbr1_high = mbr1_low;
  std::vector<float> mbr2_low = uid_to_projected_[entries[seed2]];
  std::vector<float> mbr2_high = mbr2_low;

  std::vector<bool> assigned(entries.size(), false);
  assigned[seed1] = true;
  assigned[seed2] = true;
  int assigned_count = 2;

  while (assigned_count < static_cast<int>(entries.size())) {
    if (config_.rtree_min_entries - static_cast<int>(group1.size()) == static_cast<int>(entries.size()) - assigned_count) {
      for (size_t i = 0; i < entries.size(); ++i) {
        if (!assigned[i]) group1.push_back(entries[i]);
      }
      break;
    }
    if (config_.rtree_min_entries - static_cast<int>(group2.size()) == static_cast<int>(entries.size()) - assigned_count) {
      for (size_t i = 0; i < entries.size(); ++i) {
        if (!assigned[i]) group2.push_back(entries[i]);
      }
      break;
    }

    int best_idx = -1;
    for (size_t i = 0; i < entries.size(); ++i) {
      if (assigned[i]) continue;
      best_idx = i;
      break; 
    }

    if (best_idx != -1) {
      const auto& pt = uid_to_projected_[entries[best_idx]];
      float d1 = euclideanDistance(pt, uid_to_projected_[entries[seed1]]);
      float d2 = euclideanDistance(pt, uid_to_projected_[entries[seed2]]);
      
      if (d1 < d2) {
        group1.push_back(entries[best_idx]);
        for(size_t k=0; k<mbr1_low.size(); ++k) {
            mbr1_low[k] = std::min(mbr1_low[k], pt[k]);
            mbr1_high[k] = std::max(mbr1_high[k], pt[k]);
        }
      } else {
        group2.push_back(entries[best_idx]);
        for(size_t k=0; k<mbr2_low.size(); ++k) {
            mbr2_low[k] = std::min(mbr2_low[k], pt[k]);
            mbr2_high[k] = std::max(mbr2_high[k], pt[k]);
        }
      }
      assigned[best_idx] = true;
      assigned_count++;
    }
  }

  node->entries = std::move(group1);
  node->mbr_low = mbr1_low;
  node->mbr_high = mbr1_high;

  new_node->entries = std::move(group2);
  new_node->mbr_low = mbr2_low;
  new_node->mbr_high = mbr2_high;

  return new_node;
}
std::unique_ptr<HDRTree::RTreeNode> HDRTree::splitInternalNode(RTreeNode* node) {
  auto new_node = std::make_unique<RTreeNode>(config_.projected_dim);
  new_node->is_leaf = false;

  auto& children = node->children;
  if (children.size() < 2) return nullptr;

  // 1. 选择种子节点 (Max wasted area)
  size_t seed1 = 0, seed2 = 1;
  double max_wasted = -1.0;

  for (size_t i = 0; i < children.size(); ++i) {
    for (size_t j = i + 1; j < children.size(); ++j) {
      // 使用 enlargement 互算
      double waste = children[i]->enlargement(*children[j]);
      if (waste > max_wasted) {
        max_wasted = waste;
        seed1 = i;
        seed2 = j;
      }
    }
  }

  std::vector<std::unique_ptr<RTreeNode>> group1, group2;
  // 注意：移动后原 vector 位置变为空，需小心处理
  // 为简单起见，先分类再移动
  std::vector<int> assignment(children.size(), 0); // 1 or 2
  assignment[seed1] = 1;
  assignment[seed2] = 2;

  // 初始化 MBR
  RTreeNode mbr1(config_.projected_dim);
  mbr1.mbr_low = children[seed1]->mbr_low;
  mbr1.mbr_high = children[seed1]->mbr_high;
  
  RTreeNode mbr2(config_.projected_dim);
  mbr2.mbr_low = children[seed2]->mbr_low;
  mbr2.mbr_high = children[seed2]->mbr_high;

  for (size_t i = 0; i < children.size(); ++i) {
    if (i == seed1 || i == seed2) continue;

    double inc1 = mbr1.enlargement(*children[i]);
    double inc2 = mbr2.enlargement(*children[i]);

    if (inc1 < inc2) {
      assignment[i] = 1;
      mbr1.expandMBR(*children[i]);
    } else {
      assignment[i] = 2;
      mbr2.expandMBR(*children[i]);
    }
  }

  for (size_t i = 0; i < children.size(); ++i) {
    if (assignment[i] == 1) {
      group1.push_back(std::move(children[i]));
    } else {
      group2.push_back(std::move(children[i]));
    }
  }

  node->children = std::move(group1);
  node->mbr_low = mbr1.mbr_low;
  node->mbr_high = mbr1.mbr_high;

  new_node->children = std::move(group2);
  new_node->mbr_low = mbr2.mbr_low;
  new_node->mbr_high = mbr2.mbr_high;

  return new_node;
}

auto HDRTree::erase(uint64_t uid) -> bool {
  std::unique_lock lock(mutex_);
  auto it = uid_to_projected_.find(uid);
  if (it == uid_to_projected_.end()) return false;
  uid_to_projected_.erase(it);

  // 简化实现：R-Tree 删除较复杂（需处理下溢合并），此处仅重建或标记删除
  // 生产环境应实现标准删除逻辑
  // 这里暂时只从叶子节点移除 UID，不进行树调整
  if (rtree_root_) {
      // 需遍历查找
      std::vector<RTreeNode*> q;
      q.push_back(rtree_root_.get());
      while(!q.empty()){
          auto* curr = q.back(); q.pop_back();
          if(curr->is_leaf){
              auto& ents = curr->entries;
              auto eit = std::find(ents.begin(), ents.end(), uid);
              if(eit != ents.end()){
                  ents.erase(eit);
                  // 此时应更新 MBR 和处理下溢
                  return true;
              }
          } else {
              for(auto& child : curr->children) q.push_back(child.get());
          }
      }
  }
  return true;
}

auto HDRTree::euclideanDistance(const std::vector<float>& v1, const std::vector<float>& v2)
    -> float {
  float sum = 0.0F;
  for (size_t i = 0; i < v1.size(); ++i) {
    float diff = v1[i] - v2[i];
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

auto HDRTree::estimateDistanceUpperBound(float projected_dist) const -> float {
  return projected_dist * config_.distance_bound_ratio;
}

auto HDRTree::searchRTree(const std::vector<float>& projected_query, float threshold) const
    -> std::vector<uint64_t> {
  std::vector<uint64_t> candidates;
  if (!rtree_root_) return candidates;
  searchRTreeNode(rtree_root_.get(), projected_query, threshold, candidates);
  return candidates;
}

void HDRTree::searchRTreeNode(const RTreeNode* node, const std::vector<float>& query,
                                float threshold, std::vector<uint64_t>& candidates) const {
  if (!node) return;
  if (!node->intersects(query, threshold)) return;

  if (node->is_leaf) {
    for (auto uid : node->entries) {
      // 进一步检查点到点的投影距离
      auto it = uid_to_projected_.find(uid);
      if (it != uid_to_projected_.end()) {
          float dist = euclideanDistance(query, it->second);
          if (dist <= threshold) {
              candidates.push_back(uid);
          }
      } else {
          candidates.push_back(uid);
      }
    }
  } else {
    for (const auto& child : node->children) {
      searchRTreeNode(child.get(), query, threshold, candidates);
    }
  }
}

auto HDRTree::verifyCandidates(const VectorRecord& query, const std::vector<uint64_t>& candidates,
                                double threshold) const -> std::vector<uint64_t> {
  std::vector<uint64_t> results;
  if (!storage_manager_ || !storage_manager_->engine_) return results;

  for (uint64_t uid : candidates) {
    auto rec = storage_manager_->getVectorByUid(uid);
    if (!rec) continue;
    
    // 计算真实相似度
    float sim = storage_manager_->engine_->Similarity(query.data_, rec->data_);
    if (sim >= threshold) {
      results.push_back(uid);
    }
  }
  return results;
}

auto HDRTree::query(const VectorRecord& record, int k) -> std::vector<uint64_t> {
  return query(record, {}, k);
}

auto HDRTree::query_for_join(const VectorRecord& record, double threshold)
    -> std::vector<uint64_t> {
  return query_for_join(record, {}, threshold);
}


auto HDRTree::query(const VectorRecord& record, const std::vector<float>& projected, int k) -> std::vector<uint64_t> {
  std::shared_lock lock(mutex_);
  if (!pca_training_done_) return {};

  std::vector<float> projected_query;
  if (!projected.empty()) {
      projected_query = projected;
  } else {
      projected_query = projectVector(record.data_);
  }
  
  float search_radius = 10.0f; 
  auto candidates = searchRTree(projected_query, search_radius);
  
  if (static_cast<int>(candidates.size()) < k) {
      candidates = searchRTree(projected_query, search_radius * 5.0f);
  }

  std::vector<std::pair<uint64_t, double>> distances;
  distances.reserve(candidates.size());

  for (uint64_t uid : candidates) {
    auto rec = storage_manager_->getVectorByUid(uid);
    if (!rec) continue;
    float sim = storage_manager_->engine_->Similarity(record.data_, rec->data_);
    distances.emplace_back(uid, 1.0 - sim);
  }

  std::partial_sort(distances.begin(),
                    distances.begin() + std::min(static_cast<size_t>(k), distances.size()),
                    distances.end(),
                    [](const auto& a, const auto& b) { return a.second < b.second; });

  std::vector<uint64_t> results;
  results.reserve(k);
  for (size_t i = 0; i < std::min(static_cast<size_t>(k), distances.size()); ++i) {
    results.push_back(distances[i].first);
  }
  return results;
}

auto HDRTree::query_for_join(const VectorRecord& record, const std::vector<float>& projected, double threshold) -> std::vector<uint64_t> {                                                                                                                std::shared_lock lock(mutex_);
  if (!pca_training_done_) return {};

  std::vector<float> projected_query;
  if (!projected.empty()) {
      projected_query = projected;
  } else {
      projected_query = projectVector(record.data_);
  }
  
  float distance_threshold = std::sqrt(2.0F * (1.0F - static_cast<float>(threshold)));
  float projected_threshold = distance_threshold * config_.distance_bound_ratio; 

  auto candidates = searchRTree(projected_query, projected_threshold);
  return verifyCandidates(record, candidates, threshold);
}

}  // namespace sageFlow

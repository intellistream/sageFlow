#include "index/hdr_tree.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <queue>
#include <stdexcept>

#include "utils/logger.h"

namespace sageFlow {

// ============================================================================
// RTreeNode 实现
// ============================================================================

HDRTree::RTreeNode::RTreeNode(int dim)
    : mbr_low(dim, std::numeric_limits<float>::max()),
      mbr_high(dim, std::numeric_limits<float>::lowest()),
      is_leaf(true) {}

auto HDRTree::RTreeNode::intersects(const std::vector<float>& query, float threshold) const
    -> bool {
  // 计算查询点到 MBR 的最小距离
  float min_dist_sq = 0.0F;
  for (size_t i = 0; i < query.size(); ++i) {
    if (query[i] < mbr_low[i]) {
      float diff = mbr_low[i] - query[i];
      min_dist_sq += diff * diff;
    } else if (query[i] > mbr_high[i]) {
      float diff = query[i] - mbr_high[i];
      min_dist_sq += diff * diff;
    }
  }
  return std::sqrt(min_dist_sq) <= threshold;
}

void HDRTree::RTreeNode::expandMBR(const std::vector<float>& point) {
  for (size_t i = 0; i < point.size(); ++i) {
    mbr_low[i] = std::min(mbr_low[i], point[i]);
    mbr_high[i] = std::max(mbr_high[i], point[i]);
  }
}

// ============================================================================
// HDRTree 实现
// ============================================================================

HDRTree::HDRTree(int dimension, const Config& config)
    : config_(config) {
  dimension_ = dimension;
  index_type_ = IndexType::HDRTree;

  // 验证配置
  if (config_.projected_dim > dimension) {
    throw std::invalid_argument(
        "Projected dimension cannot exceed original dimension");
  }
  if (config_.projected_dim <= 0) {
    throw std::invalid_argument("Projected dimension must be positive");
  }

  // 创建 PCA 实例（但不训练）
  pca_ = std::make_unique<PCA>(dimension, config_.projected_dim);

  // 预留采样缓冲区
  sample_buffer_.reserve(config_.pca_sample_size);

  SAGEFLOW_LOG_DEBUG("HDRTree", "Created HDRTree: dim={}, projected_dim={}, pca_sample_size={}",
                     dimension, config_.projected_dim, config_.pca_sample_size);
}

void HDRTree::trainPCA(const std::vector<std::vector<float>>& samples) {
  std::unique_lock lock(mutex_);

  if (samples.empty()) {
    throw std::invalid_argument("Training samples cannot be empty");
  }

  pca_->fit(samples);
  pca_training_done_ = true;

  // 创建 R-Tree 根节点
  rtree_root_ = std::make_unique<RTreeNode>(config_.projected_dim);

  SAGEFLOW_LOG_INFO("HDRTree", "PCA trained with {} samples, explained variance ratio: {:.2f}%",
                    samples.size(),
                    pca_->getExplainedVarianceRatio().empty()
                        ? 0.0
                        : pca_->getExplainedVarianceRatio()[0] * 100);
}

void HDRTree::tryAutoTrainPCA() {
  // 如果已训练，直接返回
  if (pca_training_done_) {
    return;
  }

  // 检查是否有足够的样本
  if (static_cast<int>(sample_buffer_.size()) >= config_.pca_sample_size) {
    SAGEFLOW_LOG_INFO("HDRTree", "Auto-training PCA with {} samples", sample_buffer_.size());
    pca_->fit(sample_buffer_);
    pca_training_done_ = true;

    // 创建 R-Tree 根节点
    rtree_root_ = std::make_unique<RTreeNode>(config_.projected_dim);

    // 将缓冲区中的向量重新投影并插入 R-Tree
    // 需要从 storage_manager_ 获取完整记录
    // 这里简化处理：清空缓冲区，后续 insert 会正常工作
    sample_buffer_.clear();
    sample_buffer_.shrink_to_fit();
  }
}

auto HDRTree::extractFloatVector(const VectorData& data) -> std::vector<float> {
  std::vector<float> result(data.dim_);

  switch (data.type_) {
    case DataType::Float32: {
      const auto* ptr = reinterpret_cast<const float*>(data.data_.get());
      std::copy(ptr, ptr + data.dim_, result.begin());
      break;
    }
    case DataType::Float64: {
      const auto* ptr = reinterpret_cast<const double*>(data.data_.get());
      for (int i = 0; i < data.dim_; ++i) {
        result[i] = static_cast<float>(ptr[i]);
      }
      break;
    }
    case DataType::Int32: {
      const auto* ptr = reinterpret_cast<const int32_t*>(data.data_.get());
      for (int i = 0; i < data.dim_; ++i) {
        result[i] = static_cast<float>(ptr[i]);
      }
      break;
    }
    default:
      throw std::runtime_error("Unsupported data type for HDRTree");
  }

  return result;
}

auto HDRTree::projectVector(const VectorData& data) const -> std::vector<float> {
  if (!pca_ || !pca_->isFitted()) {
    throw std::runtime_error("PCA not trained yet");
  }

  auto vec = extractFloatVector(data);
  return pca_->transform(vec);
}

auto HDRTree::insert(uint64_t uid) -> bool {
  if (!storage_manager_) {
    SAGEFLOW_LOG_ERROR("HDRTree", "Storage manager not set");
    return false;
  }

  auto record = storage_manager_->getVectorByUid(uid);
  if (!record) {
    SAGEFLOW_LOG_WARN("HDRTree", "Record with uid={} not found in storage", uid);
    return false;
  }

  std::unique_lock lock(mutex_);

  // 如果 PCA 未训练，先采样
  if (!pca_training_done_) {
    auto vec = extractFloatVector(record->data_);
    if (static_cast<int>(sample_buffer_.size()) < config_.pca_sample_size) {
      sample_buffer_.push_back(std::move(vec));
    }
    tryAutoTrainPCA();

    // 如果仍未训练，暂时不插入 R-Tree
    if (!pca_training_done_) {
      // 记录 UID，但不插入 R-Tree（后续重建时处理）
      uid_to_projected_[uid] = {};
      return true;
    }
  }

  // 投影向量
  auto projected = projectVector(record->data_);

  // 存储映射
  uid_to_projected_[uid] = projected;

  // 插入 R-Tree
  insertToRTree(uid, projected);

  return true;
}

void HDRTree::insertToRTree(uint64_t uid, const std::vector<float>& projected) {
  if (!rtree_root_) {
    rtree_root_ = std::make_unique<RTreeNode>(config_.projected_dim);
  }

  // 简化实现：直接将所有条目放入根节点
  // 实际生产环境应实现完整的 R-Tree 分裂逻辑
  rtree_root_->entries.push_back(uid);
  rtree_root_->expandMBR(projected);
}

auto HDRTree::erase(uint64_t uid) -> bool {
  std::unique_lock lock(mutex_);

  auto it = uid_to_projected_.find(uid);
  if (it == uid_to_projected_.end()) {
    return false;
  }

  uid_to_projected_.erase(it);

  // 从 R-Tree 中删除
  if (rtree_root_) {
    auto& entries = rtree_root_->entries;
    auto pos = std::find(entries.begin(), entries.end(), uid);
    if (pos != entries.end()) {
      entries.erase(pos);
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
  // 使用配置的距离上界比例
  // 在论文中，这个比例基于奇异值分布计算
  // 这里使用简化的固定比例
  return projected_dist * config_.distance_bound_ratio;
}

auto HDRTree::searchRTree(const std::vector<float>& projected_query, float threshold) const
    -> std::vector<uint64_t> {
  std::vector<uint64_t> candidates;

  if (!rtree_root_) {
    return candidates;
  }

  searchRTreeNode(rtree_root_.get(), projected_query, threshold, candidates);
  return candidates;
}

void HDRTree::searchRTreeNode(const RTreeNode* node, const std::vector<float>& query,
                               float threshold, std::vector<uint64_t>& candidates) const {
  if (!node) {
    return;
  }

  // 检查 MBR 是否与查询范围相交
  if (!node->intersects(query, threshold)) {
    return;
  }

  if (node->is_leaf) {
    // 叶子节点：检查所有条目
    for (uint64_t uid : node->entries) {
      auto it = uid_to_projected_.find(uid);
      if (it != uid_to_projected_.end()) {
        float dist = euclideanDistance(query, it->second);
        if (dist <= threshold) {
          candidates.push_back(uid);
        }
      }
    }
  } else {
    // 内部节点：递归搜索子节点
    for (const auto& child : node->children) {
      searchRTreeNode(child.get(), query, threshold, candidates);
    }
  }
}

auto HDRTree::verifyCandidates(const VectorRecord& query, const std::vector<uint64_t>& candidates,
                                double threshold) const -> std::vector<uint64_t> {
  std::vector<uint64_t> results;

  if (!storage_manager_ || !storage_manager_->engine_) {
    return results;
  }

  for (uint64_t uid : candidates) {
    auto record = storage_manager_->getVectorByUid(uid);
    if (!record) {
      continue;
    }

    // 计算原始空间的相似度
    double similarity = storage_manager_->engine_->Similarity(query.data_, record->data_);

    if (similarity >= threshold) {
      results.push_back(uid);
    }
  }

  return results;
}

auto HDRTree::query(const VectorRecord& record, int k) -> std::vector<uint64_t> {
  std::shared_lock lock(mutex_);

  if (!pca_training_done_) {
    SAGEFLOW_LOG_WARN("HDRTree", "PCA not trained, falling back to brute force");
    // 回退到暴力搜索
    if (storage_manager_) {
      return storage_manager_->topk(record, k);
    }
    return {};
  }

  // 投影查询向量
  auto projected_query = projectVector(record.data_);

  // 收集所有候选
  std::vector<uint64_t> all_candidates;
  for (const auto& [uid, proj] : uid_to_projected_) {
    all_candidates.push_back(uid);
  }

  // 计算精确距离并排序
  std::vector<std::pair<uint64_t, double>> distances;
  distances.reserve(all_candidates.size());

  for (uint64_t uid : all_candidates) {
    auto candidate = storage_manager_->getVectorByUid(uid);
    if (candidate) {
      double dist = storage_manager_->engine_->EuclideanDistance(record.data_, candidate->data_);
      distances.emplace_back(uid, dist);
    }
  }

  // 按距离排序
  std::partial_sort(distances.begin(),
                    distances.begin() + std::min(static_cast<size_t>(k), distances.size()),
                    distances.end(),
                    [](const auto& a, const auto& b) { return a.second < b.second; });

  // 返回前 k 个
  std::vector<uint64_t> results;
  results.reserve(k);
  for (size_t i = 0; i < std::min(static_cast<size_t>(k), distances.size()); ++i) {
    results.push_back(distances[i].first);
  }

  return results;
}

auto HDRTree::query_for_join(const VectorRecord& record, double threshold)
    -> std::vector<uint64_t> {
  std::shared_lock lock(mutex_);

  if (!pca_training_done_) {
    SAGEFLOW_LOG_WARN("HDRTree", "PCA not trained, falling back to brute force");
    // 回退到暴力搜索
    if (storage_manager_) {
      return storage_manager_->similarityJoinQuery(record, threshold);
    }
    return {};
  }

  // 投影查询向量
  auto projected_query = projectVector(record.data_);

  // 将相似度阈值转换为距离阈值
  // 对于归一化向量，余弦相似度 = 点积，欧氏距离 = sqrt(2 * (1 - 相似度))
  // 当 similarity >= threshold 时, distance <= sqrt(2 * (1 - threshold))
  float distance_threshold = std::sqrt(2.0F * (1.0F - static_cast<float>(threshold)));

  // 在投影空间使用更宽松的阈值
  // PCA 距离是下界，所以我们需要使用原始距离阈值（不除以 bound_ratio）
  // 这样可以确保所有可能的候选都被包含
  float projected_threshold = distance_threshold * config_.distance_bound_ratio;

  // R-Tree 候选搜索 - 使用所有存储的向量作为候选（简化实现）
  // 这确保了高召回率，虽然可能牺牲一些效率
  std::vector<uint64_t> candidates;
  candidates.reserve(uid_to_projected_.size());

  for (const auto& [uid, proj] : uid_to_projected_) {
    float proj_dist = euclideanDistance(projected_query, proj);
    // 使用放大的阈值进行过滤
    if (proj_dist <= projected_threshold) {
      candidates.push_back(uid);
    }
  }

  SAGEFLOW_LOG_DEBUG("HDRTree", "Projected search returned {} candidates for threshold={} "
                     "(distance_threshold={:.4f}, projected_threshold={:.4f})",
                     candidates.size(), threshold, distance_threshold, projected_threshold);

  // 验证候选
  auto results = verifyCandidates(record, candidates, threshold);

  SAGEFLOW_LOG_DEBUG("HDRTree", "Verified {} results from {} candidates",
                     results.size(), candidates.size());

  return results;
}

}  // namespace sageFlow

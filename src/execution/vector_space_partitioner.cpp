#include "execution/vector_space_partitioner.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <stdexcept>

namespace sageFlow {

// =============================================================================
// LSHPartitioner Implementation
// =============================================================================

LSHPartitioner::LSHPartitioner(int dimension, int num_hash_functions, int seed, double boundary_threshold)
    : dimension_(dimension),
      num_hash_functions_(std::min(num_hash_functions, 64)),  // 限制最大64位
      boundary_threshold_(boundary_threshold) {
  if (dimension <= 0) {
    throw std::invalid_argument("LSHPartitioner: dimension must be positive");
  }
  if (num_hash_functions <= 0) {
    throw std::invalid_argument("LSHPartitioner: num_hash_functions must be positive");
  }
  if (boundary_threshold < 0.0 || boundary_threshold > 1.0) {
    throw std::invalid_argument("LSHPartitioner: boundary_threshold must be in [0, 1]");
  }
  initRandomProjections(seed);
}

void LSHPartitioner::initRandomProjections(int seed) {
  std::mt19937 gen(seed);
  std::normal_distribution<float> dist(0.0f, 1.0f);

  random_projections_.resize(num_hash_functions_);

  for (int i = 0; i < num_hash_functions_; ++i) {
    random_projections_[i].resize(dimension_);

    // 生成随机向量
    float norm = 0.0f;
    for (int j = 0; j < dimension_; ++j) {
      random_projections_[i][j] = dist(gen);
      norm += random_projections_[i][j] * random_projections_[i][j];
    }

    // 归一化为单位向量
    norm = std::sqrt(norm);
    if (norm > 0) {
      for (int j = 0; j < dimension_; ++j) {
        random_projections_[i][j] /= norm;
      }
    }
  }
}

uint64_t LSHPartitioner::computeHashCode(const VectorRecord& record) const {
  if (record.data_.dim_ != dimension_) {
    throw std::invalid_argument("LSHPartitioner: vector dimension mismatch");
  }

  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  uint64_t hash_code = 0;

  for (int i = 0; i < num_hash_functions_; ++i) {
    // 计算点积
    float dot_product = 0.0f;
    for (int j = 0; j < dimension_; ++j) {
      dot_product += float_data[j] * random_projections_[i][j];
    }

    // 点积 > 0 则对应位为 1
    if (dot_product > 0) {
      hash_code |= (1ULL << i);
    }
  }

  return hash_code;
}

std::vector<float> LSHPartitioner::computeDistancesToHyperplanes(const VectorRecord& record) const {
  if (record.data_.dim_ != dimension_) {
    throw std::invalid_argument("LSHPartitioner: vector dimension mismatch");
  }

  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  std::vector<float> distances(num_hash_functions_);

  for (int i = 0; i < num_hash_functions_; ++i) {
    // 点积就是到超平面的有符号距离（因为投影向量已归一化）
    float dot_product = 0.0f;
    for (int j = 0; j < dimension_; ++j) {
      dot_product += float_data[j] * random_projections_[i][j];
    }
    distances[i] = dot_product;
  }

  return distances;
}

float LSHPartitioner::computeVectorNorm(const VectorRecord& record) const {
  if (record.data_.dim_ != dimension_) {
    throw std::invalid_argument("LSHPartitioner: vector dimension mismatch");
  }

  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  float norm_sq = 0.0f;

  for (int j = 0; j < dimension_; ++j) {
    norm_sq += float_data[j] * float_data[j];
  }

  return std::sqrt(norm_sq);
}

uint64_t LSHPartitioner::getHashCode(const VectorRecord& record) const { return computeHashCode(record); }

size_t LSHPartitioner::partition(const VectorRecord& record, size_t num_partitions) {
  if (num_partitions == 0) {
    throw std::invalid_argument("LSHPartitioner: num_partitions must be positive");
  }
  uint64_t hash_code = computeHashCode(record);
  return static_cast<size_t>(hash_code % num_partitions);
}

std::vector<size_t> LSHPartitioner::getCandidatePartitions(const VectorRecord& query, size_t num_partitions,
                                                           size_t num_probes) {
  if (num_partitions == 0) {
    throw std::invalid_argument("LSHPartitioner: num_partitions must be positive");
  }

  std::vector<size_t> candidates;
  candidates.reserve(num_probes);

  // 获取主分区
  uint64_t main_hash = computeHashCode(query);
  size_t main_partition = static_cast<size_t>(main_hash % num_partitions);
  candidates.push_back(main_partition);

  if (num_probes <= 1) {
    return candidates;
  }

  // 计算到各超平面的距离
  std::vector<float> distances = computeDistancesToHyperplanes(query);

  // 创建按距离排序的索引（绝对值越小越容易翻转）
  std::vector<int> sorted_indices(num_hash_functions_);
  std::iota(sorted_indices.begin(), sorted_indices.end(), 0);
  std::sort(sorted_indices.begin(), sorted_indices.end(),
            [&distances](int a, int b) { return std::abs(distances[a]) < std::abs(distances[b]); });

  // 通过翻转距离最近的 bit 位获取邻近分区
  std::vector<bool> seen(num_partitions, false);
  seen[main_partition] = true;

  for (size_t i = 0; i < static_cast<size_t>(num_hash_functions_) && candidates.size() < num_probes; ++i) {
    int bit_to_flip = sorted_indices[i];
    uint64_t flipped_hash = main_hash ^ (1ULL << bit_to_flip);
    size_t flipped_partition = static_cast<size_t>(flipped_hash % num_partitions);

    if (!seen[flipped_partition]) {
      seen[flipped_partition] = true;
      candidates.push_back(flipped_partition);
    }
  }

  return candidates;
}

bool LSHPartitioner::isBoundaryVector(const VectorRecord& record, size_t num_partitions) {
  (void)num_partitions;  // 未使用，但保留以保持接口一致性

  float norm = computeVectorNorm(record);
  if (norm < 1e-9f) {
    // 零向量视为边界向量
    return true;
  }

  std::vector<float> distances = computeDistancesToHyperplanes(record);

  // 检查是否有任何超平面距离小于阈值
  float threshold = boundary_threshold_ * norm;
  for (float dist : distances) {
    if (std::abs(dist) < threshold) {
      return true;
    }
  }

  return false;
}

// =============================================================================
// KMeansPartitioner Implementation
// =============================================================================

KMeansPartitioner::KMeansPartitioner(int dimension, int num_clusters, int seed)
    : dimension_(dimension), num_clusters_(num_clusters), seed_(seed), centroids_initialized_(false) {
  if (dimension <= 0) {
    throw std::invalid_argument("KMeansPartitioner: dimension must be positive");
  }
  if (num_clusters <= 0) {
    throw std::invalid_argument("KMeansPartitioner: num_clusters must be positive");
  }

  centroids_.resize(num_clusters);
  cluster_counts_.resize(num_clusters, 0);
}

std::vector<float> KMeansPartitioner::extractFloatVector(const VectorRecord& record) const {
  if (record.data_.dim_ != dimension_) {
    throw std::invalid_argument("KMeansPartitioner: vector dimension mismatch");
  }

  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  return std::vector<float>(float_data, float_data + dimension_);
}

float KMeansPartitioner::computeDistanceToCentroid(const VectorRecord& record, size_t centroid_idx) const {
  if (centroid_idx >= static_cast<size_t>(num_clusters_)) {
    throw std::out_of_range("KMeansPartitioner: centroid index out of range");
  }

  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  const auto& centroid = centroids_[centroid_idx];

  float dist_sq = 0.0f;
  for (int j = 0; j < dimension_; ++j) {
    float diff = float_data[j] - centroid[j];
    dist_sq += diff * diff;
  }

  return std::sqrt(dist_sq);
}

size_t KMeansPartitioner::findNearestCentroid(const VectorRecord& record) const {
  if (!centroids_initialized_) {
    throw std::runtime_error("KMeansPartitioner: centroids not initialized");
  }

  size_t nearest = 0;
  float min_dist = std::numeric_limits<float>::max();

  for (size_t i = 0; i < static_cast<size_t>(num_clusters_); ++i) {
    float dist = computeDistanceToCentroid(record, i);
    if (dist < min_dist) {
      min_dist = dist;
      nearest = i;
    }
  }

  return nearest;
}

void KMeansPartitioner::initCentroids(const std::vector<const VectorRecord*>& samples, int max_iterations) {
  if (samples.empty()) {
    throw std::invalid_argument("KMeansPartitioner: samples cannot be empty");
  }

  std::mt19937 gen(seed_);

  // K-Means++ 初始化
  std::vector<size_t> centroid_indices;
  centroid_indices.reserve(num_clusters_);

  // 随机选择第一个质心
  std::uniform_int_distribution<size_t> first_dist(0, samples.size() - 1);
  centroid_indices.push_back(first_dist(gen));
  centroids_[0] = extractFloatVector(*samples[centroid_indices[0]]);

  // 选择剩余质心
  for (int k = 1; k < num_clusters_; ++k) {
    std::vector<float> min_distances(samples.size());

    for (size_t i = 0; i < samples.size(); ++i) {
      float min_dist = std::numeric_limits<float>::max();
      const auto* float_data = reinterpret_cast<const float*>(samples[i]->data_.data_.get());

      for (size_t c = 0; c < static_cast<size_t>(k); ++c) {
        float dist_sq = 0.0f;
        for (int j = 0; j < dimension_; ++j) {
          float diff = float_data[j] - centroids_[c][j];
          dist_sq += diff * diff;
        }
        min_dist = std::min(min_dist, dist_sq);
      }
      min_distances[i] = min_dist;
    }

    // 按距离的平方加权随机选择下一个质心
    std::discrete_distribution<size_t> weighted_dist(min_distances.begin(), min_distances.end());
    size_t next_idx = weighted_dist(gen);
    centroid_indices.push_back(next_idx);
    centroids_[k] = extractFloatVector(*samples[next_idx]);
  }

  // K-Means 迭代
  for (int iter = 0; iter < max_iterations; ++iter) {
    // 分配样本到最近的质心
    std::vector<std::vector<size_t>> assignments(num_clusters_);

    for (size_t i = 0; i < samples.size(); ++i) {
      size_t nearest = 0;
      float min_dist = std::numeric_limits<float>::max();
      const auto* float_data = reinterpret_cast<const float*>(samples[i]->data_.data_.get());

      for (int c = 0; c < num_clusters_; ++c) {
        float dist_sq = 0.0f;
        for (int j = 0; j < dimension_; ++j) {
          float diff = float_data[j] - centroids_[c][j];
          dist_sq += diff * diff;
        }
        if (dist_sq < min_dist) {
          min_dist = dist_sq;
          nearest = static_cast<size_t>(c);
        }
      }
      assignments[nearest].push_back(i);
    }

    // 更新质心
    bool changed = false;
    for (int c = 0; c < num_clusters_; ++c) {
      if (assignments[c].empty()) {
        continue;
      }

      std::vector<float> new_centroid(dimension_, 0.0f);
      for (size_t idx : assignments[c]) {
        const auto* float_data = reinterpret_cast<const float*>(samples[idx]->data_.data_.get());
        for (int j = 0; j < dimension_; ++j) {
          new_centroid[j] += float_data[j];
        }
      }

      for (int j = 0; j < dimension_; ++j) {
        new_centroid[j] /= static_cast<float>(assignments[c].size());
        if (std::abs(new_centroid[j] - centroids_[c][j]) > 1e-6f) {
          changed = true;
        }
      }

      centroids_[c] = std::move(new_centroid);
      cluster_counts_[c] = assignments[c].size();
    }

    if (!changed) {
      break;
    }
  }

  centroids_initialized_ = true;
}

void KMeansPartitioner::updateCentroids(const VectorRecord& record, double learning_rate) {
  if (!centroids_initialized_) {
    throw std::runtime_error("KMeansPartitioner: centroids not initialized");
  }

  size_t nearest = findNearestCentroid(record);
  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());

  // 增量更新质心
  auto lr = static_cast<float>(learning_rate);
  for (int j = 0; j < dimension_; ++j) {
    centroids_[nearest][j] = (1.0f - lr) * centroids_[nearest][j] + lr * float_data[j];
  }

  cluster_counts_[nearest]++;
}

size_t KMeansPartitioner::partition(const VectorRecord& record, size_t num_partitions) {
  if (num_partitions == 0) {
    throw std::invalid_argument("KMeansPartitioner: num_partitions must be positive");
  }

  size_t cluster = findNearestCentroid(record);
  return cluster % num_partitions;
}

std::vector<size_t> KMeansPartitioner::getCandidatePartitions(const VectorRecord& query, size_t num_partitions,
                                                              size_t num_probes) {
  if (num_partitions == 0) {
    throw std::invalid_argument("KMeansPartitioner: num_partitions must be positive");
  }

  if (!centroids_initialized_) {
    throw std::runtime_error("KMeansPartitioner: centroids not initialized");
  }

  // 计算到所有质心的距离并排序
  std::vector<std::pair<float, size_t>> distances(num_clusters_);
  for (int c = 0; c < num_clusters_; ++c) {
    distances[c] = {computeDistanceToCentroid(query, static_cast<size_t>(c)), static_cast<size_t>(c)};
  }

  std::sort(distances.begin(), distances.end());

  // 返回最近的 num_probes 个不重复分区
  std::vector<size_t> candidates;
  candidates.reserve(num_probes);
  std::vector<bool> seen(num_partitions, false);

  for (const auto& [dist, cluster] : distances) {
    size_t partition_id = cluster % num_partitions;
    if (!seen[partition_id]) {
      seen[partition_id] = true;
      candidates.push_back(partition_id);
      if (candidates.size() >= num_probes) {
        break;
      }
    }
  }

  return candidates;
}

bool KMeansPartitioner::isBoundaryVector(const VectorRecord& record, size_t num_partitions) {
  (void)num_partitions;  // 未使用

  if (!centroids_initialized_) {
    throw std::runtime_error("KMeansPartitioner: centroids not initialized");
  }

  // 找到最近和次近的质心
  float min_dist = std::numeric_limits<float>::max();
  float second_min_dist = std::numeric_limits<float>::max();

  for (int c = 0; c < num_clusters_; ++c) {
    float dist = computeDistanceToCentroid(record, static_cast<size_t>(c));
    if (dist < min_dist) {
      second_min_dist = min_dist;
      min_dist = dist;
    } else if (dist < second_min_dist) {
      second_min_dist = dist;
    }
  }

  // 如果最近和次近距离相差不大，则为边界向量
  if (min_dist < 1e-9f) {
    return false;  // 正好在质心上
  }

  float ratio = (second_min_dist - min_dist) / min_dist;
  return ratio < 0.2f;  // 阈值可调
}

}  // namespace sageFlow

#include "execution/centroid_partitioner.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <chrono>

#include "utils/logger.h"

namespace sageFlow {

CentroidPartitioner::CentroidPartitioner(const Config& config)
    : config_(config)
    , training_samples_threshold_(config.training_samples) {
  if (config_.num_partitions <= 0) {
    throw std::invalid_argument("CentroidPartitioner: num_partitions must be positive");
  }
  if (config_.dimension <= 0) {
    throw std::invalid_argument("CentroidPartitioner: dimension must be positive");
  }
  if (config_.overlap_ratio < 0.0 || config_.overlap_ratio > 1.0) {
    throw std::invalid_argument("CentroidPartitioner: overlap_ratio must be in [0, 1]");
  }
  
  centroids_.resize(config_.num_partitions);
  partition_sizes_ = std::vector<std::atomic<size_t>>(config_.num_partitions);
  for (auto& size : partition_sizes_) {
    size.store(0);
  }
  
  // 预留训练样本缓冲区空间
  if (config_.enable_cold_start) {
    training_buffer_.reserve(config_.training_samples);
  }
}

// ==================== 训练与分区 ====================

void CentroidPartitioner::train(const std::vector<std::vector<float>>& samples) {
  if (samples.empty()) {
    throw std::invalid_argument("CentroidPartitioner: samples cannot be empty");
  }
  
  // 验证维度
  for (const auto& sample : samples) {
    if (static_cast<int>(sample.size()) != config_.dimension) {
      throw std::invalid_argument("CentroidPartitioner: sample dimension mismatch");
    }
  }
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // 初始化质心
  if (config_.init_method == "kmeans++") {
    initKMeansPlusPlus(samples);
  } else {
    initRandom(samples);
  }
  
  // K-Means 迭代
  for (int iter = 0; iter < config_.max_iterations; ++iter) {
    bool converged = runKMeansIteration(samples);
    if (converged) {
      SAGEFLOW_LOG_DEBUG("CentroidPartitioner", "K-Means converged at iteration {}", iter + 1);
      break;
    }
  }
  
  trained_.store(true);
  
  // 打印质心摘要用于调试
  std::string centroid_summary;
  for (size_t i = 0; i < centroids_.size() && i < 4; ++i) {
    if (!centroids_[i].empty()) {
      centroid_summary += std::to_string(centroids_[i][0]) + " ";
    }
  }
  SAGEFLOW_LOG_INFO("CentroidPartitioner", "Training completed with {} partitions, first 4 centroids[0]: {}",
                    config_.num_partitions, centroid_summary);
}

void CentroidPartitioner::train(const std::vector<const VectorRecord*>& samples) {
  std::vector<std::vector<float>> float_samples;
  float_samples.reserve(samples.size());
  
  for (const auto* record : samples) {
    if (record) {
      float_samples.push_back(extractFloatVector(*record));
    }
  }
  
  train(float_samples);
}

int CentroidPartitioner::getPrimaryPartition(const VectorRecord& record) const {
  if (!trained_.load()) {
    throw std::runtime_error("CentroidPartitioner: not trained yet");
  }
  
  auto vec = extractFloatVector(record);
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return findNearestCentroid(vec);
}

std::vector<int> CentroidPartitioner::getPartitions(const VectorRecord& record) const {
  if (!trained_.load()) {
    throw std::runtime_error("CentroidPartitioner: not trained yet");
  }
  
  auto vec = extractFloatVector(record);
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  // ========== K-Nearest 模式：直接复用已有方法 ==========
  if (config_.multicast_k > 0) {
    auto result = findNearestKCentroids(vec, config_.multicast_k);
    return result;
  }
  
  // ========== 阈值模式（原有逻辑） ==========
  std::vector<int> partitions;
  int primary = findNearestCentroid(vec);
  partitions.push_back(primary);
  
  // 如果是边界向量，添加额外分区
  auto distances = computeDistances(vec);
  float min_dist = distances[primary];
  
  if (min_dist < 1e-9f) {
    return partitions;  // 正好在质心上
  }
  
  // 检查边界条件
  for (int i = 0; i < config_.num_partitions; ++i) {
    if (i == primary) continue;
    
    float ratio = (distances[i] - min_dist) / min_dist;
    if (ratio < config_.overlap_ratio) {
      partitions.push_back(i);
    }
  }
  
  return partitions;
}

// ==================== IPartitioner 接口实现 ====================

size_t CentroidPartitioner::partition(const Response& data, size_t num_channels) {
  if (!data.record_) {
    return 0;
  }
  
  // 冷启动阶段：收集样本并返回广播标识
  if (config_.enable_cold_start && !trained_.load()) {
    addTrainingSample(*data.record_);
    // 返回 0，由 ResultPartition 检查 isBroadcast() 决定行为
    return 0;
  }
  
  // 未训练且未启用冷启动时：所有数据路由到 subtask 0
  // 这确保了 ClusteredJoin 的正确性，但会退化为单线程模式
  // TODO: 实现在线训练或使用 LSH 分区来支持并行
  // Issue URL: https://github.com/intellistream/sageFlow/issues/95
  if (!trained_.load()) {
    return 0;
  }
  
  int partition_idx = getPrimaryPartition(*data.record_);
  return static_cast<size_t>(partition_idx) % num_channels;
}

std::vector<size_t> CentroidPartitioner::partitionMulti(const Response& data, size_t num_channels) {
  // 多播未启用或未训练时，降级为单播
  if (!multicast_enabled_ || !trained_.load()) {
    SAGEFLOW_LOG_WARN("CentroidPartitioner", 
        "partitionMulti: degrading to unicast (multicast_enabled={}, trained={})",
        multicast_enabled_, trained_.load());
    return {partition(data, num_channels)};;
  }
  
  if (!data.record_) {
    return {0};
  }
  
  // 获取向量的所有相关分区（主分区 + 边界分区）
  auto partitions = getPartitions(*data.record_);
  
  // 仅在前几次记录时打印日志（避免日志过多）
  static thread_local int log_count = 0;
  if (log_count++ < 5) {
    SAGEFLOW_LOG_INFO("CentroidPartitioner", 
        "partitionMulti: uid={}, getPartitions returned {} partitions, multicast_k={}",
        data.record_->uid_, partitions.size(), config_.multicast_k);
  }
  
  // 如果只有一个分区，直接返回
  if (partitions.size() == 1) {
    return {static_cast<size_t>(partitions[0]) % num_channels};
  }
  
  // 转换为 size_t 并映射到 channel 空间
  std::vector<size_t> result;
  result.reserve(partitions.size());
  
  for (int p : partitions) {
    result.push_back(static_cast<size_t>(p) % num_channels);
  }
  
  // 去重（多个分区可能映射到同一个 channel）
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  
  return result;
}

// ==================== 质心管理 ====================

const std::vector<std::vector<float>>& CentroidPartitioner::getCentroids() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return centroids_;
}

void CentroidPartitioner::updateCentroids(const std::vector<std::vector<float>>& new_samples,
                                          double learning_rate) {
  if (new_samples.empty()) return;
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  for (const auto& sample : new_samples) {
    if (static_cast<int>(sample.size()) != config_.dimension) continue;
    
    int nearest = findNearestCentroid(sample);
    auto lr = static_cast<float>(learning_rate);
    
    for (int j = 0; j < config_.dimension; ++j) {
      centroids_[nearest][j] = (1.0f - lr) * centroids_[nearest][j] + lr * sample[j];
    }
  }
}

void CentroidPartitioner::updateCentroidsIncremental(const std::vector<float>& vec,
                                                     double learning_rate) {
  if (static_cast<int>(vec.size()) != config_.dimension) return;
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  int nearest = findNearestCentroid(vec);
  auto lr = static_cast<float>(learning_rate);
  
  for (int j = 0; j < config_.dimension; ++j) {
    centroids_[nearest][j] = (1.0f - lr) * centroids_[nearest][j] + lr * vec[j];
  }
}

// ==================== 负载均衡 ====================

bool CentroidPartitioner::needsRebalance(const std::vector<size_t>& partition_sizes) const {
  if (partition_sizes.size() != static_cast<size_t>(config_.num_partitions)) {
    return false;
  }
  
  double score = computeBalanceScore(partition_sizes);
  return score < (1.0 - config_.rebalance_threshold);
}

CentroidPartitioner::PartitionStats CentroidPartitioner::getStats() const {
  PartitionStats stats;
  stats.sizes.resize(config_.num_partitions);
  
  for (int i = 0; i < config_.num_partitions; ++i) {
    stats.sizes[i] = partition_sizes_[i].load();
  }
  
  stats.balance_score = computeBalanceScore(stats.sizes);
  return stats;
}

void CentroidPartitioner::updatePartitionSize(int partition_idx, int delta) {
  if (partition_idx < 0 || partition_idx >= config_.num_partitions) return;
  
  if (delta > 0) {
    partition_sizes_[partition_idx].fetch_add(static_cast<size_t>(delta));
  } else if (delta < 0) {
    size_t abs_delta = static_cast<size_t>(-delta);
    size_t current = partition_sizes_[partition_idx].load();
    if (current >= abs_delta) {
      partition_sizes_[partition_idx].fetch_sub(abs_delta);
    } else {
      partition_sizes_[partition_idx].store(0);
    }
  }
}

void CentroidPartitioner::resetPartitionSizes() {
  for (auto& size : partition_sizes_) {
    size.store(0);
  }
}

// ==================== 边界处理 ====================

bool CentroidPartitioner::isBoundaryVector(const VectorRecord& record) const {
  if (!trained_.load()) {
    return false;
  }
  
  auto vec = extractFloatVector(record);
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto distances = computeDistances(vec);
  
  // 找到最近和次近距离
  float min_dist = std::numeric_limits<float>::max();
  float second_min_dist = std::numeric_limits<float>::max();
  
  for (int i = 0; i < config_.num_partitions; ++i) {
    if (distances[i] < min_dist) {
      second_min_dist = min_dist;
      min_dist = distances[i];
    } else if (distances[i] < second_min_dist) {
      second_min_dist = distances[i];
    }
  }
  
  if (min_dist < 1e-9f) {
    return false;  // 正好在质心上
  }
  
  // 边界判定：次近距离与最近距离的比值
  float ratio = (second_min_dist - min_dist) / min_dist;
  return ratio < config_.overlap_ratio;
}

std::vector<int> CentroidPartitioner::getBorderPartitions(const VectorRecord& record) const {
  if (!trained_.load()) {
    return {};
  }
  
  auto vec = extractFloatVector(record);
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto distances = computeDistances(vec);
  int primary = findNearestCentroid(vec);
  float min_dist = distances[primary];
  
  if (min_dist < 1e-9f) {
    return {};  // 正好在质心上
  }
  
  std::vector<int> border_partitions;
  for (int i = 0; i < config_.num_partitions; ++i) {
    if (i == primary) continue;
    
    float ratio = (distances[i] - min_dist) / min_dist;
    if (ratio < config_.overlap_ratio) {
      border_partitions.push_back(i);
    }
  }
  
  return border_partitions;
}

// ==================== 内部方法 ====================

void CentroidPartitioner::initKMeansPlusPlus(const std::vector<std::vector<float>>& samples) {
  std::mt19937 gen(config_.seed);
  
  // 随机选择第一个质心
  std::uniform_int_distribution<size_t> first_dist(0, samples.size() - 1);
  centroids_[0] = samples[first_dist(gen)];
  
  // 选择剩余质心
  for (int k = 1; k < config_.num_partitions; ++k) {
    std::vector<float> min_distances(samples.size());
    
    for (size_t i = 0; i < samples.size(); ++i) {
      float min_dist = std::numeric_limits<float>::max();
      
      for (int c = 0; c < k; ++c) {
        float dist_sq = 0.0f;
        for (int j = 0; j < config_.dimension; ++j) {
          float diff = samples[i][j] - centroids_[c][j];
          dist_sq += diff * diff;
        }
        min_dist = std::min(min_dist, dist_sq);
      }
      min_distances[i] = min_dist;
    }
    
    // 按距离的平方加权随机选择
    std::discrete_distribution<size_t> weighted_dist(min_distances.begin(), min_distances.end());
    size_t next_idx = weighted_dist(gen);
    centroids_[k] = samples[next_idx];
  }
}

void CentroidPartitioner::initRandom(const std::vector<std::vector<float>>& samples) {
  std::mt19937 gen(config_.seed);
  std::uniform_int_distribution<size_t> dist(0, samples.size() - 1);
  
  std::vector<size_t> selected;
  while (static_cast<int>(selected.size()) < config_.num_partitions) {
    size_t idx = dist(gen);
    if (std::find(selected.begin(), selected.end(), idx) == selected.end()) {
      selected.push_back(idx);
      centroids_[selected.size() - 1] = samples[idx];
    }
  }
}

bool CentroidPartitioner::runKMeansIteration(const std::vector<std::vector<float>>& samples) {
  // 分配样本到最近的质心
  std::vector<std::vector<size_t>> assignments(config_.num_partitions);
  
  for (size_t i = 0; i < samples.size(); ++i) {
    int nearest = findNearestCentroid(samples[i]);
    assignments[nearest].push_back(i);
  }
  
  // 更新质心
  bool converged = true;
  for (int c = 0; c < config_.num_partitions; ++c) {
    if (assignments[c].empty()) {
      continue;
    }
    
    std::vector<float> new_centroid(config_.dimension, 0.0f);
    for (size_t idx : assignments[c]) {
      for (int j = 0; j < config_.dimension; ++j) {
        new_centroid[j] += samples[idx][j];
      }
    }
    
    for (int j = 0; j < config_.dimension; ++j) {
      new_centroid[j] /= static_cast<float>(assignments[c].size());
      if (std::abs(new_centroid[j] - centroids_[c][j]) > 1e-6f) {
        converged = false;
      }
    }
    
    centroids_[c] = std::move(new_centroid);
  }
  
  return converged;
}

std::vector<float> CentroidPartitioner::computeDistances(const std::vector<float>& vec) const {
  std::vector<float> distances(config_.num_partitions);
  
  for (int i = 0; i < config_.num_partitions; ++i) {
    distances[i] = computeDistanceToCentroid(vec, i);
  }
  
  return distances;
}

float CentroidPartitioner::computeDistanceToCentroid(const std::vector<float>& vec, int centroid_idx) const {
  if (centroid_idx < 0 || centroid_idx >= config_.num_partitions) {
    return std::numeric_limits<float>::max();
  }
  
  const auto& centroid = centroids_[centroid_idx];
  if (centroid.empty()) {
    return std::numeric_limits<float>::max();
  }
  
  float dist_sq = 0.0f;
  for (int j = 0; j < config_.dimension; ++j) {
    float diff = vec[j] - centroid[j];
    dist_sq += diff * diff;
  }
  
  return std::sqrt(dist_sq);
}

int CentroidPartitioner::findNearestCentroid(const std::vector<float>& vec) const {
  int nearest = 0;
  float min_dist = std::numeric_limits<float>::max();
  
  for (int i = 0; i < config_.num_partitions; ++i) {
    float dist = computeDistanceToCentroid(vec, i);
    if (dist < min_dist) {
      min_dist = dist;
      nearest = i;
    }
  }
  
  return nearest;
}

std::vector<int> CentroidPartitioner::findNearestKCentroids(const std::vector<float>& vec, int k) const {
  std::vector<std::pair<float, int>> dist_idx(config_.num_partitions);
  
  for (int i = 0; i < config_.num_partitions; ++i) {
    dist_idx[i] = {computeDistanceToCentroid(vec, i), i};
  }
  
  std::sort(dist_idx.begin(), dist_idx.end());
  
  std::vector<int> result;
  result.reserve(k);
  for (int i = 0; i < std::min(k, config_.num_partitions); ++i) {
    result.push_back(dist_idx[i].second);
  }
  
  return result;
}

std::vector<float> CentroidPartitioner::extractFloatVector(const VectorRecord& record) const {
  if (record.data_.dim_ != config_.dimension) {
    throw std::invalid_argument("CentroidPartitioner: vector dimension mismatch");
  }
  
  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  return std::vector<float>(float_data, float_data + config_.dimension);
}

double CentroidPartitioner::computeBalanceScore(const std::vector<size_t>& sizes) const {
  if (sizes.empty()) return 1.0;
  
  size_t total = 0;
  for (size_t s : sizes) {
    total += s;
  }
  
  if (total == 0) return 1.0;
  
  double expected = static_cast<double>(total) / sizes.size();
  double variance = 0.0;
  
  for (size_t s : sizes) {
    double diff = static_cast<double>(s) - expected;
    variance += diff * diff;
  }
  variance /= sizes.size();
  
  // 归一化方差到 [0, 1]，0 表示完美均衡
  double normalized_variance = std::sqrt(variance) / expected;
  
  // 转换为均衡得分：1 表示完美均衡
  return std::max(0.0, 1.0 - normalized_variance);
}

// ==================== 冷启动训练实现 ====================

bool CentroidPartitioner::addTrainingSample(const VectorRecord& record) {
  // 已训练或训练已触发，不再收集
  if (trained_.load() || training_triggered_.load()) {
    return false;
  }
  
  // 提取向量并添加到缓冲区
  auto vec = extractFloatVector(record);
  
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    training_buffer_.push_back(std::move(vec));
  }
  
  size_t count = sample_count_.fetch_add(1) + 1;
  
  // 检查是否达到训练阈值
  if (count >= training_samples_threshold_) {
    triggerTrainingInternal();
  }
  
  return true;
}

bool CentroidPartitioner::forceTraining() {
  // 已训练或训练已触发
  if (trained_.load() || training_triggered_.load()) {
    return false;
  }
  
  // 检查是否有足够样本
  if (sample_count_.load() == 0) {
    SAGEFLOW_LOG_WARN("CentroidPartitioner", "No samples collected, cannot force training");
    return false;
  }
  
  triggerTrainingInternal();
  return true;
}

bool CentroidPartitioner::isBroadcast() const {
  return config_.enable_cold_start && !trained_.load();
}

std::pair<size_t, size_t> CentroidPartitioner::getTrainingProgress() const {
  return {sample_count_.load(), training_samples_threshold_};
}

void CentroidPartitioner::triggerTrainingInternal() {
  // 原子性检查并设置，防止重复触发
  bool expected = false;
  if (!training_triggered_.compare_exchange_strong(expected, true)) {
    return;  // 已被其他线程触发
  }
  
  SAGEFLOW_LOG_INFO("CentroidPartitioner", 
    "Cold-start training triggered with {} samples", sample_count_.load());
  
  // 执行训练（复用现有 train() 方法）
  std::vector<std::vector<float>> samples;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    samples = std::move(training_buffer_);
    training_buffer_.clear();
  }
  
  if (!samples.empty()) {
    train(samples);
  }
  
  // 释放内存
  training_buffer_.shrink_to_fit();
  
  SAGEFLOW_LOG_INFO("CentroidPartitioner", "Cold-start training completed");
}

}  // namespace sageFlow

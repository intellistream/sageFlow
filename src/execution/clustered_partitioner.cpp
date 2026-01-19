#include "execution/clustered_partitioner.h"

#include <algorithm>
#include <stdexcept>

#include "utils/logger.h"

namespace sageFlow {

ClusteredPartitioner::ClusteredPartitioner(const Config& config)
    : config_(config) {
  // 参数验证
  if (config_.num_vector_partitions <= 0) {
    throw std::invalid_argument(
        "ClusteredPartitioner: num_vector_partitions must be positive");
  }
  if (config_.threads_per_partition <= 0) {
    throw std::invalid_argument(
        "ClusteredPartitioner: threads_per_partition must be positive");
  }
  if (config_.dimension <= 0) {
    throw std::invalid_argument(
        "ClusteredPartitioner: dimension must be positive");
  }
  if (config_.overlap_ratio < 0.0 || config_.overlap_ratio > 1.0) {
    throw std::invalid_argument(
        "ClusteredPartitioner: overlap_ratio must be in [0, 1]");
  }

  // 创建内部的 CentroidPartitioner
  CentroidPartitioner::Config centroid_config;
  centroid_config.num_partitions = config_.num_vector_partitions;
  centroid_config.overlap_ratio = config_.overlap_ratio;
  centroid_config.dimension = config_.dimension;
  centroid_config.max_iterations = config_.max_iterations;
  centroid_config.seed = config_.seed;

  centroid_partitioner_ = std::make_shared<CentroidPartitioner>(centroid_config);

  // 启用 CentroidPartitioner 的多播支持（用于边界向量）
  if (config_.multicast_enabled) {
    centroid_partitioner_->setMulticastEnabled(true);
  }

  SAGEFLOW_LOG_INFO(
      "ClusteredPartitioner",
      "Created with {} vector partitions, {} threads/partition, multicast={}, "
      "overlap_ratio={}, dimension={}",
      config_.num_vector_partitions, config_.threads_per_partition,
      config_.multicast_enabled, config_.overlap_ratio, config_.dimension);
}

// ==================== IPartitioner 接口实现 ====================

size_t ClusteredPartitioner::partition(const Response& data,
                                       size_t num_channels) {
  if (!data.record_) {
    // 无记录数据，返回轮询分配
    return round_robin_counter_.fetch_add(1, std::memory_order_relaxed) %
           num_channels;
  }

  // 第一级：获取向量空间分区
  size_t vec_partition = 0;
  if (trained_) {
    vec_partition = centroid_partitioner_->partition(
        data, static_cast<size_t>(config_.num_vector_partitions));
  } else {
    // 未训练时使用简单哈希
    vec_partition = data.record_->uid_ %
                    static_cast<size_t>(config_.num_vector_partitions);
  }

  // 第二级：分区内负载均衡
  return mapPartitionToSubtask(vec_partition, num_channels);
}

std::vector<size_t> ClusteredPartitioner::partitionMulti(const Response& data,
                                                         size_t num_channels) {
  // 多播未启用或未训练时，降级为单播
  if (!config_.multicast_enabled || !trained_) {
    return {partition(data, num_channels)};
  }

  if (!data.record_) {
    return {0};
  }

  // 获取向量空间多播分区（主分区 + 边界分区）
  auto vec_partitions = centroid_partitioner_->partitionMulti(
      data, static_cast<size_t>(config_.num_vector_partitions));

  // 如果只有一个分区，直接映射
  if (vec_partitions.size() == 1) {
    size_t subtask = mapPartitionToSubtask(vec_partitions[0], num_channels);
    return {subtask};
  }

  // 将每个向量分区映射到 subtask
  std::vector<size_t> result;
  result.reserve(vec_partitions.size() * config_.threads_per_partition);

  for (size_t vp : vec_partitions) {
    auto subtasks = getSubtasksForPartition(vp);
    for (size_t st : subtasks) {
      // 确保 subtask 在有效范围内
      if (st < num_channels) {
        result.push_back(st);
      } else {
        // N:1 模式下映射到有效的 channel
        result.push_back(st % num_channels);
      }
    }
  }

  // 去重（多个分区可能映射到同一个 channel）
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());

  return result;
}

// ==================== 训练接口 ====================

void ClusteredPartitioner::train(
    const std::vector<const VectorRecord*>& samples) {
  if (samples.empty()) {
    SAGEFLOW_LOG_WARN("ClusteredPartitioner",
                      "Training with empty samples, skipping");
    return;
  }

  // 委托给 CentroidPartitioner 训练
  centroid_partitioner_->train(samples);
  trained_ = true;

  SAGEFLOW_LOG_INFO("ClusteredPartitioner", "Trained with {} samples",
                    samples.size());
}

void ClusteredPartitioner::train(
    const std::vector<std::vector<float>>& samples) {
  if (samples.empty()) {
    SAGEFLOW_LOG_WARN("ClusteredPartitioner",
                      "Training with empty samples, skipping");
    return;
  }

  // 委托给 CentroidPartitioner 训练
  centroid_partitioner_->train(samples);
  trained_ = true;

  SAGEFLOW_LOG_INFO("ClusteredPartitioner", "Trained with {} samples",
                    samples.size());
}

// ==================== 查询接口 ====================

size_t ClusteredPartitioner::getVectorPartition(
    const VectorRecord& record) const {
  if (!trained_) {
    // 未训练时使用简单哈希
    return record.uid_ % static_cast<size_t>(config_.num_vector_partitions);
  }

  return centroid_partitioner_->getPrimaryPartition(record);
}

std::vector<size_t> ClusteredPartitioner::getSubtasksForPartition(
    size_t vec_partition) const {
  std::vector<size_t> result;

  if (config_.threads_per_partition == 1) {
    // 1:1 模式：分区直接映射到 subtask
    result.push_back(vec_partition);
  } else {
    // 1:N 模式：一个分区对应多个 subtask
    size_t base =
        vec_partition * static_cast<size_t>(config_.threads_per_partition);
    for (int i = 0; i < config_.threads_per_partition; ++i) {
      result.push_back(base + i);
    }
  }

  return result;
}

bool ClusteredPartitioner::isBoundaryVector(const VectorRecord& record) const {
  if (!trained_) {
    return false;
  }
  return centroid_partitioner_->isBoundaryVector(record);
}

// ==================== 内部方法 ====================

size_t ClusteredPartitioner::mapPartitionToSubtask(size_t vec_partition,
                                                   size_t num_channels) {
  if (config_.threads_per_partition == 1) {
    // 1:1 模式或 N:1 模式
    // 如果 num_vector_partitions > num_channels，多个分区会映射到同一 channel
    return vec_partition % num_channels;
  }

  // 1:N 模式：分区内轮询
  size_t base =
      vec_partition * static_cast<size_t>(config_.threads_per_partition);
  size_t offset = round_robin_counter_.fetch_add(1, std::memory_order_relaxed) %
                  static_cast<size_t>(config_.threads_per_partition);
  size_t subtask = base + offset;

  // 确保结果在有效范围内
  return subtask % num_channels;
}

}  // namespace sageFlow

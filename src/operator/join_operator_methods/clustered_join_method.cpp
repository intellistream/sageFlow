#include "operator/join_operator_methods/clustered_join_method.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include "utils/logger.h"

namespace sageFlow {

ClusteredJoinMethod::ClusteredJoinMethod(
    int left_index_id,
    int right_index_id,
    const Config& config,
    const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : BaseMethod(config.similarity_threshold),
      config_(config),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      concurrency_manager_(concurrency_manager) {
  
  // 创建质心分区器
  CentroidPartitioner::Config partitioner_config;
  partitioner_config.num_partitions = config_.num_partitions;
  partitioner_config.overlap_ratio = config_.overlap_ratio;
  partitioner_config.rebalance_threshold = config_.rebalance_threshold;
  partitioner_config.dimension = config_.dimension;
  
  partitioner_ = std::make_shared<CentroidPartitioner>(partitioner_config);
  
  SAGEFLOW_LOG_INFO("ClusteredJoin", "Created with {} partitions, threshold={}",
                    config_.num_partitions, config_.similarity_threshold);
}

ClusteredJoinMethod::ClusteredJoinMethod(
    int left_index_id,
    int right_index_id,
    double join_similarity_threshold,
    const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : BaseMethod(join_similarity_threshold),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      concurrency_manager_(concurrency_manager) {
  
  config_.similarity_threshold = join_similarity_threshold;
  
  // 创建默认配置的质心分区器
  CentroidPartitioner::Config partitioner_config;
  partitioner_config.num_partitions = config_.num_partitions;
  partitioner_config.overlap_ratio = config_.overlap_ratio;
  partitioner_config.dimension = config_.dimension;
  
  partitioner_ = std::make_shared<CentroidPartitioner>(partitioner_config);
  
  SAGEFLOW_LOG_INFO("ClusteredJoin", "Created with default config, threshold={}",
                    join_similarity_threshold);
}

// ==================== BaseMethod 接口实现 ====================

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> results;
  
  if (!concurrency_manager_) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", "ConcurrencyManager is null");
    return results;
  }
  
  int target_idx = otherIndexId(query_slot);
  if (target_idx == -1) [[unlikely]] {
    return results;
  }
  
  // 尝试自动训练分区器
  tryAutoTrain(query_record);
  
  // 收集所有候选结果
  std::vector<std::shared_ptr<const VectorRecord>> candidates;
  
  if (partitioner_ && partitioner_->isTrained()) {
    // 使用分区策略搜索
    
    // 1. 搜索主分区
    auto primary_results = searchPrimaryPartition(
        query_record, config_.similarity_threshold, target_idx);
    candidates.insert(candidates.end(), primary_results.begin(), primary_results.end());
    
    // 2. 如果启用边界处理，搜索边界分区
    if (config_.use_border_replication && partitioner_->isBoundaryVector(query_record)) {
      auto border_partitions = partitioner_->getBorderPartitions(query_record);
      if (!border_partitions.empty()) {
        auto border_results = searchBorderPartitions(
            query_record, border_partitions, config_.similarity_threshold, target_idx);
        candidates.insert(candidates.end(), border_results.begin(), border_results.end());
      }
    }
    
    // 去重
    deduplicateResults(candidates);
    
    // 更新分区器
    updatePartitioner(query_record);
  } else {
    // 分区器未训练，回退到全局搜索
    candidates = concurrency_manager_->query_for_join(
        target_idx, query_record, config_.similarity_threshold);
  }
  
  SAGEFLOW_LOG_DEBUG("ClusteredJoin", "Eager query slot={} found {} candidates",
                     query_slot, candidates.size());
  
  // 转换结果
  results.reserve(candidates.size());
  for (const auto& c : candidates) {
    if (c) {
      results.emplace_back(std::make_unique<VectorRecord>(*c));
    }
  }
  
  return results;
}

std::vector<std::unique_ptr<VectorRecord>> ClusteredJoinMethod::ExecuteLazy(
    const std::deque<std::unique_ptr<VectorRecord>>& query_records,
    int query_slot) {
  std::vector<std::unique_ptr<VectorRecord>> all_results;
  
  if (!concurrency_manager_) {
    SAGEFLOW_LOG_WARN("ClusteredJoin", "ConcurrencyManager is null");
    return all_results;
  }
  
  int target_idx = otherIndexId(query_slot);
  if (target_idx == -1) [[unlikely]] {
    return all_results;
  }
  
  for (const auto& qr : query_records) {
    if (!qr) continue;
    
    // 尝试自动训练
    tryAutoTrain(*qr);
    
    std::vector<std::shared_ptr<const VectorRecord>> candidates;
    
    if (partitioner_ && partitioner_->isTrained()) {
      // 使用分区策略搜索
      auto primary_results = searchPrimaryPartition(
          *qr, config_.similarity_threshold, target_idx);
      candidates.insert(candidates.end(), primary_results.begin(), primary_results.end());
      
      if (config_.use_border_replication && partitioner_->isBoundaryVector(*qr)) {
        auto border_partitions = partitioner_->getBorderPartitions(*qr);
        if (!border_partitions.empty()) {
          auto border_results = searchBorderPartitions(
              *qr, border_partitions, config_.similarity_threshold, target_idx);
          candidates.insert(candidates.end(), border_results.begin(), border_results.end());
        }
      }
      
      deduplicateResults(candidates);
      updatePartitioner(*qr);
    } else {
      // 回退到全局搜索
      candidates = concurrency_manager_->query_for_join(
          target_idx, *qr, config_.similarity_threshold);
    }
    
    for (const auto& c : candidates) {
      if (c) {
        all_results.emplace_back(std::make_unique<VectorRecord>(*c));
      }
    }
  }
  
  SAGEFLOW_LOG_DEBUG("ClusteredJoin", "Lazy query slot={} processed {} queries, found {} results",
                     query_slot, query_records.size(), all_results.size());
  
  return all_results;
}

// ==================== ClusteredJoin 特有方法 ====================

void ClusteredJoinMethod::trainPartitioner(const std::vector<std::vector<float>>& samples) {
  if (!partitioner_) {
    SAGEFLOW_LOG_ERROR("ClusteredJoin", "Partitioner is null");
    return;
  }
  
  partitioner_->train(samples);
  SAGEFLOW_LOG_INFO("ClusteredJoin", "Partitioner trained with {} samples", samples.size());
}

void ClusteredJoinMethod::trainPartitioner(const std::vector<const VectorRecord*>& samples) {
  if (!partitioner_) {
    SAGEFLOW_LOG_ERROR("ClusteredJoin", "Partitioner is null");
    return;
  }
  
  partitioner_->train(samples);
  SAGEFLOW_LOG_INFO("ClusteredJoin", "Partitioner trained with {} samples", samples.size());
}

bool ClusteredJoinMethod::isPartitionerTrained() const {
  return partitioner_ && partitioner_->isTrained();
}

void ClusteredJoinMethod::rebalance() {
  if (!partitioner_) return;
  
  auto stats = partitioner_->getStats();
  if (partitioner_->needsRebalance(stats.sizes)) {
    SAGEFLOW_LOG_INFO("ClusteredJoin", "Rebalance triggered, balance_score={}",
                      stats.balance_score);
    
    // 收集训练缓冲区中的样本重新训练
    std::lock_guard<std::mutex> lock(training_mutex_);
    if (!training_buffer_.empty()) {
      partitioner_->train(training_buffer_);
      partitioner_->resetPartitionSizes();
    }
  }
}

CentroidPartitioner::PartitionStats ClusteredJoinMethod::getPartitionStats() const {
  if (!partitioner_) {
    return {{}, 1.0};
  }
  return partitioner_->getStats();
}

void ClusteredJoinMethod::updatePartitioner(const VectorRecord& record) {
  if (!partitioner_ || !partitioner_->isTrained()) return;
  
  auto vec = extractFloatVector(record);
  
  // 增量更新质心
  partitioner_->updateCentroidsIncremental(vec, config_.learning_rate);
  
  // 更新分区大小统计
  int partition = partitioner_->getPrimaryPartition(record);
  partitioner_->updatePartitionSize(partition, 1);
}

// ==================== 内部方法 ====================

std::vector<std::shared_ptr<const VectorRecord>> ClusteredJoinMethod::searchPrimaryPartition(
    const VectorRecord& query,
    double threshold,
    int target_index_id) {
  
  // 当前实现：直接使用全局索引搜索
  // 未来可以扩展为使用分区内的局部索引
  return concurrency_manager_->query_for_join(target_index_id, query, threshold);
}

std::vector<std::shared_ptr<const VectorRecord>> ClusteredJoinMethod::searchBorderPartitions(
    const VectorRecord& query,
    const std::vector<int>& partitions,
    double threshold,
    int target_index_id) {
  
  // 当前实现：对边界分区执行相同的全局搜索
  // 由于使用共享索引，边界处理由 deduplicateResults 完成
  // 未来可以扩展为使用分区内的局部索引
  (void)partitions;  // 当前未使用分区信息
  return concurrency_manager_->query_for_join(target_index_id, query, threshold);
}

void ClusteredJoinMethod::deduplicateResults(
    std::vector<std::shared_ptr<const VectorRecord>>& results) {
  if (results.size() <= 1) return;
  
  std::unordered_set<uint64_t> seen_uids;
  auto new_end = std::remove_if(results.begin(), results.end(),
      [&seen_uids](const std::shared_ptr<const VectorRecord>& record) {
        if (!record) return true;
        if (seen_uids.count(record->uid_)) return true;
        seen_uids.insert(record->uid_);
        return false;
      });
  results.erase(new_end, results.end());
}

void ClusteredJoinMethod::tryAutoTrain(const VectorRecord& record) {
  if (auto_trained_ || (partitioner_ && partitioner_->isTrained())) {
    return;
  }
  
  std::lock_guard<std::mutex> lock(training_mutex_);
  
  // 再次检查（双重锁定检查）
  if (auto_trained_) return;
  
  // 添加到训练缓冲区
  auto vec = extractFloatVector(record);
  training_buffer_.push_back(std::move(vec));
  
  // 达到训练样本数时自动训练
  if (static_cast<int>(training_buffer_.size()) >= config_.training_samples) {
    SAGEFLOW_LOG_INFO("ClusteredJoin", "Auto-training partitioner with {} samples",
                      training_buffer_.size());
    partitioner_->train(training_buffer_);
    auto_trained_ = true;
    training_buffer_.clear();
    training_buffer_.shrink_to_fit();
  }
}

std::vector<float> ClusteredJoinMethod::extractFloatVector(const VectorRecord& record) const {
  int32_t dim = record.data_.dim_;
  const auto* float_data = reinterpret_cast<const float*>(record.data_.data_.get());
  return std::vector<float>(float_data, float_data + dim);
}

}  // namespace sageFlow

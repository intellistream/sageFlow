//
// Created for sageFlow architecture refactoring - Phase 2
// Task A-01: TwoTierWindowState 双层窗口状态
//

#include "state/two_tier_window_state.h"
#include "utils/logger.h"
#include <algorithm>

namespace sageFlow {

TwoTierWindowState::TwoTierWindowState(size_t parallelism,
                                       size_t compact_threshold,
                                       size_t merge_batch_size)
    : partitions_(parallelism),
      compact_threshold_(compact_threshold),
      merge_batch_size_(merge_batch_size) {
    SAGEFLOW_LOG_DEBUG("TwoTierState", 
        "Created TwoTierWindowState with parallelism={}, compact_threshold={}, merge_batch_size={}",
        parallelism, compact_threshold, merge_batch_size);
}

void TwoTierWindowState::addRecord(std::unique_ptr<VectorRecord> record,
                                   size_t subtask_index) {
    std::unique_lock lock(partitions_[subtask_index].mutex_);
    
    auto& tier_pair = partitions_[subtask_index];
    tier_pair.write_tier_.push_back(std::move(record));
    tier_pair.view_dirty_ = true;
    
    // 检查是否需要触发压缩（在锁内检查，避免竞争）
    if (tier_pair.write_tier_.size() >= compact_threshold_) {
        // 释放锁后执行压缩，避免持锁时间过长
        lock.unlock();
        compactTiers(subtask_index);
    }
}

const std::deque<std::unique_ptr<VectorRecord>>& 
TwoTierWindowState::getRecords(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    
    auto& tier_pair = partitions_[subtask_index];
    
    // 如果视图脏了，需要更新
    if (tier_pair.view_dirty_) {
        // 需要升级为独占锁来更新视图
        lock.unlock();
        updateMergedView(subtask_index);
        lock.lock();
    }
    
    return tier_pair.merged_view_;
}

void TwoTierWindowState::evictExpired(int64_t current_timestamp,
                                      int64_t window_size,
                                      size_t subtask_index) {
    std::unique_lock lock(partitions_[subtask_index].mutex_);
    
    auto& tier_pair = partitions_[subtask_index];
    int64_t expiry_threshold = current_timestamp - window_size;
    size_t evicted_count = 0;
    
    // 1. 清理紧凑层（从头部删除，因为头部是旧记录）
    while (!tier_pair.compact_tier_.empty() &&
           tier_pair.compact_tier_.front()->timestamp_ < expiry_threshold) {
        tier_pair.compact_tier_.erase(tier_pair.compact_tier_.begin());
        ++evicted_count;
    }
    
    // 2. 清理写层（从头部删除）
    while (!tier_pair.write_tier_.empty() &&
           tier_pair.write_tier_.front()->timestamp_ < expiry_threshold) {
        tier_pair.write_tier_.pop_front();
        ++evicted_count;
    }
    
    if (evicted_count > 0) {
        tier_pair.view_dirty_ = true;
        SAGEFLOW_LOG_DEBUG("TwoTierState",
            "Evicted {} expired records from partition {}", 
            evicted_count, subtask_index);
    }
}

size_t TwoTierWindowState::size(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    const auto& tier_pair = partitions_[subtask_index];
    return tier_pair.write_tier_.size() + tier_pair.compact_tier_.size();
}

void TwoTierWindowState::compactTiers(size_t subtask_index) {
    std::unique_lock lock(partitions_[subtask_index].mutex_);
    
    auto& tier_pair = partitions_[subtask_index];
    
    // 如果写层记录数不足，不进行压缩
    if (tier_pair.write_tier_.size() < merge_batch_size_) {
        return;
    }
    
    // 将写层中时间戳较早的记录（前 merge_batch_size_ 个）迁移到紧凑层
    size_t records_to_move = std::min(merge_batch_size_, tier_pair.write_tier_.size());
    
    // 预分配空间以提高效率
    tier_pair.compact_tier_.reserve(tier_pair.compact_tier_.size() + records_to_move);
    
    for (size_t i = 0; i < records_to_move; ++i) {
        tier_pair.compact_tier_.push_back(std::move(tier_pair.write_tier_.front()));
        tier_pair.write_tier_.pop_front();
    }
    
    // 对紧凑层按时间戳排序（保持有序以优化查询）
    std::sort(tier_pair.compact_tier_.begin(), tier_pair.compact_tier_.end(),
              [](const std::unique_ptr<VectorRecord>& a, 
                 const std::unique_ptr<VectorRecord>& b) {
                  return a->timestamp_ < b->timestamp_;
              });
    
    tier_pair.view_dirty_ = true;
    
    SAGEFLOW_LOG_DEBUG("TwoTierState",
        "Compacted {} records from write tier to compact tier in partition {}, "
        "write_tier_size={}, compact_tier_size={}",
        records_to_move, subtask_index, 
        tier_pair.write_tier_.size(), tier_pair.compact_tier_.size());
}

const std::vector<std::unique_ptr<VectorRecord>>&
TwoTierWindowState::getCompactRecords(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    return partitions_[subtask_index].compact_tier_;
}

std::vector<const VectorRecord*> 
TwoTierWindowState::getAllRecords(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    
    const auto& tier_pair = partitions_[subtask_index];
    std::vector<const VectorRecord*> result;
    result.reserve(tier_pair.compact_tier_.size() + tier_pair.write_tier_.size());
    
    // 先添加紧凑层记录（已按时间戳排序）
    for (const auto& record : tier_pair.compact_tier_) {
        result.push_back(record.get());
    }
    
    // 再添加写层记录
    for (const auto& record : tier_pair.write_tier_) {
        result.push_back(record.get());
    }
    
    return result;
}

size_t TwoTierWindowState::getWriteTierSize(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    return partitions_[subtask_index].write_tier_.size();
}

size_t TwoTierWindowState::getCompactTierSize(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    return partitions_[subtask_index].compact_tier_.size();
}

bool TwoTierWindowState::needsCompaction(size_t subtask_index) const {
    // 注意：调用者应该持有锁
    return partitions_[subtask_index].write_tier_.size() >= compact_threshold_;
}

void TwoTierWindowState::updateMergedView(size_t subtask_index) const {
    std::unique_lock lock(partitions_[subtask_index].mutex_);
    
    auto& tier_pair = partitions_[subtask_index];
    
    // 双重检查，避免在获取独占锁期间其他线程已更新
    if (!tier_pair.view_dirty_) {
        return;
    }
    
    // 清空旧视图
    tier_pair.merged_view_.clear();
    
    // 由于接口要求返回 unique_ptr 的 deque，我们需要创建深拷贝
    // 先添加紧凑层记录（已排序，时间戳较早）
    for (const auto& record : tier_pair.compact_tier_) {
        // 创建记录的深拷贝
        auto copy = std::make_unique<VectorRecord>(
            record->uid_, 
            record->timestamp_, 
            record->data_
        );
        tier_pair.merged_view_.push_back(std::move(copy));
    }
    
    // 再添加写层记录
    for (const auto& record : tier_pair.write_tier_) {
        auto copy = std::make_unique<VectorRecord>(
            record->uid_, 
            record->timestamp_, 
            record->data_
        );
        tier_pair.merged_view_.push_back(std::move(copy));
    }
    
    tier_pair.view_dirty_ = false;
}

} // namespace sageFlow

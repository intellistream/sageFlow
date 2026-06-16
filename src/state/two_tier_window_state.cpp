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
      merge_batch_size_(merge_batch_size),
      max_seen_timestamps_(parallelism) {
    // 初始化每个分区的时间戳为最小值
    for (size_t i = 0; i < parallelism; ++i) {
        max_seen_timestamps_[i].store(std::numeric_limits<int64_t>::min(), std::memory_order_relaxed);
    }
    SAGEFLOW_LOG_DEBUG("TwoTierState", 
        "Created TwoTierWindowState with parallelism={}, compact_threshold={}, merge_batch_size={}",
        parallelism, compact_threshold, merge_batch_size);
}

void TwoTierWindowState::addRecord(RecordView record,
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

const std::deque<RecordView>&
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

std::vector<RecordView>
TwoTierWindowState::getRecordsSnapshot(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    
    const auto& tier_pair = partitions_[subtask_index];
    std::vector<RecordView> snapshot;
    
    size_t total_size = tier_pair.write_tier_.size() + tier_pair.compact_tier_.size();
    snapshot.reserve(total_size);
    
    // 添加紧凑层记录（零拷贝：仅拷贝 shared_ptr）
    for (const auto& record : tier_pair.compact_tier_) {
        if (record) {
            snapshot.push_back(record);
        }
    }
    
    // 添加写层记录
    for (const auto& record : tier_pair.write_tier_) {
        if (record) {
            snapshot.push_back(record);
        }
    }
    
    return snapshot;
}

bool TwoTierWindowState::containsUid(uint64_t uid, size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    
    const auto& tier_pair = partitions_[subtask_index];
    
    // 首先检查是否已过期
    if (tier_pair.expired_uids_.count(uid) > 0) {
        return false;
    }
    
    // 检查紧凑层
    for (const auto& record : tier_pair.compact_tier_) {
        if (record && record->uid_ == uid) {
            return true;
        }
    }
    
    // 检查写层
    for (const auto& record : tier_pair.write_tier_) {
        if (record && record->uid_ == uid) {
            return true;
        }
    }
    
    return false;
}

std::unordered_set<uint64_t> TwoTierWindowState::getUidSet(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    
    const auto& tier_pair = partitions_[subtask_index];
    std::unordered_set<uint64_t> uid_set;
    
    size_t total_size = tier_pair.write_tier_.size() + tier_pair.compact_tier_.size();
    uid_set.reserve(total_size);
    
    // 添加紧凑层 UIDs
    for (const auto& record : tier_pair.compact_tier_) {
        if (record) {
            uid_set.insert(record->uid_);
        }
    }
    
    // 添加写层 UIDs
    for (const auto& record : tier_pair.write_tier_) {
        if (record) {
            uid_set.insert(record->uid_);
        }
    }
    
    return uid_set;
}

void TwoTierWindowState::evictExpired(int64_t current_timestamp,
                                      int64_t window_size,
                                      size_t subtask_index) {
    std::unique_lock lock(partitions_[subtask_index].mutex_);
    
    auto& tier_pair = partitions_[subtask_index];
    // 计算过期阈值：timestamp < current_timestamp - multiplier * window_size
    int64_t expiry_threshold = current_timestamp - 
        static_cast<int64_t>(eviction_buffer_multiplier_ * window_size);
    size_t evicted_count = 0;
    
    // 1. 清理紧凑层（从头部删除，因为头部是旧记录）
    while (!tier_pair.compact_tier_.empty() &&
           tier_pair.compact_tier_.front()->timestamp_ < expiry_threshold) {
        tier_pair.expired_uids_.insert(tier_pair.compact_tier_.front()->uid_);
        tier_pair.compact_tier_.erase(tier_pair.compact_tier_.begin());
        ++evicted_count;
    }
    
    // 2. 清理写层（从头部删除）
    while (!tier_pair.write_tier_.empty() &&
           tier_pair.write_tier_.front()->timestamp_ < expiry_threshold) {
        tier_pair.expired_uids_.insert(tier_pair.write_tier_.front()->uid_);
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

bool TwoTierWindowState::isExpired(uint64_t uid, size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    return partitions_[subtask_index].expired_uids_.count(uid) > 0;
}

size_t TwoTierWindowState::getExpiredCount(size_t subtask_index) const {
    std::shared_lock lock(partitions_[subtask_index].mutex_);
    return partitions_[subtask_index].expired_uids_.size();
}

std::vector<uint64_t> TwoTierWindowState::flushExpiredUids(size_t subtask_index) {
    std::unique_lock lock(partitions_[subtask_index].mutex_);
    auto& expired = partitions_[subtask_index].expired_uids_;
    std::vector<uint64_t> result(expired.begin(), expired.end());
    expired.clear();
    return result;
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
              [](const RecordView& a,
                 const RecordView& b) {
                  return a->timestamp_ < b->timestamp_;
              });
    
    tier_pair.view_dirty_ = true;
    
    SAGEFLOW_LOG_DEBUG("TwoTierState",
        "Compacted {} records from write tier to compact tier in partition {}, "
        "write_tier_size={}, compact_tier_size={}",
        records_to_move, subtask_index, 
        tier_pair.write_tier_.size(), tier_pair.compact_tier_.size());
}

const std::vector<RecordView>&
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
    
    // 合并视图共享同一记录实例（零拷贝：仅拷贝 shared_ptr）
    // 先添加紧凑层记录（已排序，时间戳较早）
    for (const auto& record : tier_pair.compact_tier_) {
        tier_pair.merged_view_.push_back(record);
    }
    
    // 再添加写层记录
    for (const auto& record : tier_pair.write_tier_) {
        tier_pair.merged_view_.push_back(record);
    }
    
    tier_pair.view_dirty_ = false;
}

// ==================== 时间戳追踪接口实现 ====================

void TwoTierWindowState::updateMaxSeenTimestamp(int64_t timestamp, size_t subtask_index) {
    // 使用 compare_exchange 确保只更新为更大的值
    int64_t current_max = max_seen_timestamps_[subtask_index].load(std::memory_order_relaxed);
    while (timestamp > current_max && 
           !max_seen_timestamps_[subtask_index].compare_exchange_weak(
               current_max, timestamp,
               std::memory_order_release,
               std::memory_order_relaxed)) {
        // 重试直到成功或发现更大的值
    }
}

int64_t TwoTierWindowState::getMaxSeenTimestamp(size_t subtask_index) const {
    return max_seen_timestamps_[subtask_index].load(std::memory_order_acquire);
}

int64_t TwoTierWindowState::getSafeEvictTimestamp(size_t subtask_index, 
                                                   const WindowState* other_state) const {
    constexpr int64_t kMinTimestamp = std::numeric_limits<int64_t>::min();
    int64_t this_max = max_seen_timestamps_[subtask_index].load(std::memory_order_acquire);
    if (!other_state) {
        return this_max;
    }
    int64_t other_max = other_state->getMaxSeenTimestamp(subtask_index);
    if (this_max == kMinTimestamp || other_max == kMinTimestamp) {
        return kMinTimestamp;
    }
    return std::min(this_max, other_max);
}

} // namespace sageFlow

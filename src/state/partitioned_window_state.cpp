//
// Created for sageFlow architecture refactoring - Phase 2
//

#include "state/partitioned_window_state.h"

namespace sageFlow {

PartitionedWindowState::PartitionedWindowState(size_t parallelism)
    : partitions_(parallelism)
    , expired_uids_(parallelism)
    , mutexes_(parallelism)
    , max_seen_timestamps_(parallelism) {
    // 初始化每个分区的时间戳为最小值
    for (size_t i = 0; i < parallelism; ++i) {
        max_seen_timestamps_[i].store(std::numeric_limits<int64_t>::min(), std::memory_order_relaxed);
    }
}

void PartitionedWindowState::addRecord(std::unique_ptr<VectorRecord> record, 
                                       size_t subtask_index) {
    std::unique_lock lock(mutexes_[subtask_index]);
    partitions_[subtask_index].push_back(std::move(record));
}

const std::deque<std::unique_ptr<VectorRecord>>& 
PartitionedWindowState::getRecords(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return partitions_[subtask_index];
}

std::vector<std::shared_ptr<const VectorRecord>> 
PartitionedWindowState::getRecordsSnapshot(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    std::vector<std::shared_ptr<const VectorRecord>> snapshot;
    const auto& partition = partitions_[subtask_index];
    snapshot.reserve(partition.size());
    for (const auto& record : partition) {
        if (record) {
            snapshot.push_back(std::make_shared<const VectorRecord>(*record));
        }
    }
    return snapshot;
}

bool PartitionedWindowState::containsUid(uint64_t uid, size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    // 首先检查是否已过期
    if (expired_uids_[subtask_index].count(uid) > 0) {
        return false;
    }
    const auto& partition = partitions_[subtask_index];
    for (const auto& record : partition) {
        if (record && record->uid_ == uid) {
            return true;
        }
    }
    return false;
}

std::unordered_set<uint64_t> PartitionedWindowState::getUidSet(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    std::unordered_set<uint64_t> uid_set;
    const auto& partition = partitions_[subtask_index];
    uid_set.reserve(partition.size());
    for (const auto& record : partition) {
        if (record) {
            uid_set.insert(record->uid_);
        }
    }
    return uid_set;
}

void PartitionedWindowState::evictExpired(int64_t current_timestamp, 
                                         int64_t window_size,
                                         size_t subtask_index) {
    std::unique_lock lock(mutexes_[subtask_index]);
    auto& partition = partitions_[subtask_index];
    auto& expired = expired_uids_[subtask_index];
    
    // 计算过期阈值：timestamp < current_timestamp - multiplier * window_size
    int64_t expiry_threshold = current_timestamp - 
        static_cast<int64_t>(eviction_buffer_multiplier_ * window_size);
    
    // 将过期记录的 UID 添加到 expired_uids_ buffer 中
    while (!partition.empty() && 
           partition.front()->timestamp_ < expiry_threshold) {
        expired.insert(partition.front()->uid_);
        partition.pop_front();
    }
}

bool PartitionedWindowState::isExpired(uint64_t uid, size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return expired_uids_[subtask_index].count(uid) > 0;
}

size_t PartitionedWindowState::getExpiredCount(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return expired_uids_[subtask_index].size();
}

std::vector<uint64_t> PartitionedWindowState::flushExpiredUids(size_t subtask_index) {
    std::unique_lock lock(mutexes_[subtask_index]);
    auto& expired = expired_uids_[subtask_index];
    std::vector<uint64_t> result(expired.begin(), expired.end());
    expired.clear();
    return result;
}

size_t PartitionedWindowState::size(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return partitions_[subtask_index].size();
}

// ==================== 时间戳追踪接口实现 ====================

void PartitionedWindowState::updateMaxSeenTimestamp(int64_t timestamp, size_t subtask_index) {
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

int64_t PartitionedWindowState::getMaxSeenTimestamp(size_t subtask_index) const {
    return max_seen_timestamps_[subtask_index].load(std::memory_order_acquire);
}

int64_t PartitionedWindowState::getSafeEvictTimestamp(size_t subtask_index, 
                                                       const WindowState* /*other_state*/) const {
    // 分区模式：直接返回该分区的 max_seen_ts，因为分区之间是隔离的
    // other_state 参数在分区模式下不使用
    return max_seen_timestamps_[subtask_index].load(std::memory_order_acquire);
}

} // namespace sageFlow

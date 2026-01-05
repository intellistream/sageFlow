//
// Created for sageFlow architecture refactoring - Phase 2
//

#include "state/shared_window_state.h"

namespace sageFlow {

SharedWindowState::SharedWindowState() = default;

void SharedWindowState::addRecord(std::unique_ptr<VectorRecord> record, 
                                  size_t subtask_index) {
    // subtask_index 在共享状态中被忽略
    std::unique_lock lock(mutex_);
    shared_window_.push_back(std::move(record));
}

const std::deque<std::unique_ptr<VectorRecord>>& 
SharedWindowState::getRecords(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return shared_window_;
}

std::vector<std::shared_ptr<const VectorRecord>> 
SharedWindowState::getRecordsSnapshot(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    std::vector<std::shared_ptr<const VectorRecord>> snapshot;
    snapshot.reserve(shared_window_.size());
    for (const auto& record : shared_window_) {
        if (record) {
            // 创建 shared_ptr 指向 VectorRecord 的拷贝
            snapshot.push_back(std::make_shared<const VectorRecord>(*record));
        }
    }
    return snapshot;
}

bool SharedWindowState::containsUid(uint64_t uid, size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    // 首先检查是否已过期
    if (expired_uids_.count(uid) > 0) {
        return false;
    }
    for (const auto& record : shared_window_) {
        if (record && record->uid_ == uid) {
            return true;
        }
    }
    return false;
}

std::unordered_set<uint64_t> SharedWindowState::getUidSet(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    std::unordered_set<uint64_t> uid_set;
    uid_set.reserve(shared_window_.size());
    for (const auto& record : shared_window_) {
        if (record) {
            uid_set.insert(record->uid_);
        }
    }
    return uid_set;
}

void SharedWindowState::evictExpired(int64_t current_timestamp, 
                                    int64_t window_size,
                                    size_t subtask_index) {
    // subtask_index 在共享状态中被忽略
    std::unique_lock lock(mutex_);
    
    // 计算过期阈值：timestamp < current_timestamp - multiplier * window_size
    int64_t expiry_threshold = current_timestamp - 
        static_cast<int64_t>(eviction_buffer_multiplier_ * window_size);
    
    // 将过期记录的 UID 添加到 expired_uids_ buffer 中
    while (!shared_window_.empty() && 
           shared_window_.front()->timestamp_ < expiry_threshold) {
        // 记录过期 UID（用于后续批量删除）
        expired_uids_.insert(shared_window_.front()->uid_);
        shared_window_.pop_front();
    }
}

bool SharedWindowState::isExpired(uint64_t uid, size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return expired_uids_.count(uid) > 0;
}

size_t SharedWindowState::getExpiredCount(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return expired_uids_.size();
}

std::vector<uint64_t> SharedWindowState::flushExpiredUids(size_t subtask_index) {
    // subtask_index 在共享状态中被忽略
    std::unique_lock lock(mutex_);
    std::vector<uint64_t> result(expired_uids_.begin(), expired_uids_.end());
    expired_uids_.clear();
    return result;
}

size_t SharedWindowState::size(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return shared_window_.size();
}

// ==================== 时间戳追踪接口实现 ====================

void SharedWindowState::updateMaxSeenTimestamp(int64_t timestamp, size_t /*subtask_index*/) {
    // 共享模式：使用单一全局时间戳
    int64_t current_max = max_seen_timestamp_.load(std::memory_order_relaxed);
    while (timestamp > current_max && 
           !max_seen_timestamp_.compare_exchange_weak(
               current_max, timestamp,
               std::memory_order_release,
               std::memory_order_relaxed)) {
        // 重试直到成功或发现更大的值
    }
}

int64_t SharedWindowState::getMaxSeenTimestamp(size_t /*subtask_index*/) const {
    return max_seen_timestamp_.load(std::memory_order_acquire);
}

int64_t SharedWindowState::getSafeEvictTimestamp(size_t /*subtask_index*/, 
                                                  const WindowState* other_state) const {
    // 共享模式：需要取 this 和 other_state 的 min 值
    // 确保两侧都已处理到某个时间点后才能安全 evict
    constexpr int64_t kMinTimestamp = std::numeric_limits<int64_t>::min();
    
    int64_t this_max = max_seen_timestamp_.load(std::memory_order_acquire);
    
    if (!other_state) {
        return this_max;
    }
    
    int64_t other_max = other_state->getMaxSeenTimestamp(0);
    
    // 处理初始状态
    if (this_max == kMinTimestamp && other_max == kMinTimestamp) {
        return kMinTimestamp;
    } else if (this_max == kMinTimestamp) {
        return other_max;
    } else if (other_max == kMinTimestamp) {
        return this_max;
    } else {
        return std::min(this_max, other_max);
    }
}

} // namespace sageFlow

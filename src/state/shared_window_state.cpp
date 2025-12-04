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

} // namespace sageFlow

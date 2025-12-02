//
// Created for sageFlow architecture refactoring - Phase 2
//

#include "state/partitioned_window_state.h"

namespace sageFlow {

PartitionedWindowState::PartitionedWindowState(size_t parallelism)
    : partitions_(parallelism), expired_uids_(parallelism), mutexes_(parallelism) {}

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
    
    // 将过期记录的 UID 添加到 expired_uids_ buffer 中
    while (!partition.empty() && 
           partition.front()->timestamp_ < current_timestamp - window_size) {
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

} // namespace sageFlow

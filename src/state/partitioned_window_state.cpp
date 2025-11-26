//
// Created for sageFlow architecture refactoring - Phase 2
//

#include "state/partitioned_window_state.h"

namespace sageFlow {

PartitionedWindowState::PartitionedWindowState(size_t parallelism)
    : partitions_(parallelism), mutexes_(parallelism) {}

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

void PartitionedWindowState::evictExpired(int64_t current_timestamp, 
                                         int64_t window_size,
                                         size_t subtask_index) {
    std::unique_lock lock(mutexes_[subtask_index]);
    auto& partition = partitions_[subtask_index];
    
    while (!partition.empty() && 
           partition.front()->timestamp_ < current_timestamp - window_size) {
        partition.pop_front();
    }
}

size_t PartitionedWindowState::size(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return partitions_[subtask_index].size();
}

} // namespace sageFlow

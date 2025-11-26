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

void SharedWindowState::evictExpired(int64_t current_timestamp, 
                                    int64_t window_size,
                                    size_t subtask_index) {
    // subtask_index 在共享状态中被忽略
    std::unique_lock lock(mutex_);
    
    while (!shared_window_.empty() && 
           shared_window_.front()->timestamp_ < current_timestamp - window_size) {
        shared_window_.pop_front();
    }
}

size_t SharedWindowState::size(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return shared_window_.size();
}

} // namespace sageFlow

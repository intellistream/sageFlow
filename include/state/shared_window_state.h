//
// Created for sageFlow architecture refactoring - Phase 2
//

#pragma once

#include "state/window_state.h"
#include <shared_mutex>

namespace sageFlow {

/**
 * @brief 共享窗口状态实现
 * 
 * 所有子任务共享同一状态，需要跨任务同步。
 * 适用于共享索引的 Join 方法。
 */
class SharedWindowState : public WindowState {
public:
    SharedWindowState();

    void addRecord(std::unique_ptr<VectorRecord> record, 
                  size_t subtask_index) override;

    const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const override;

    void evictExpired(int64_t current_timestamp, 
                    int64_t window_size,
                    size_t subtask_index) override;

    size_t size(size_t subtask_index) const override;

    bool isShared() const override { return true; }

private:
    // 所有子任务共享的窗口
    std::deque<std::unique_ptr<VectorRecord>> shared_window_;
    
    // 共享状态的读写锁
    mutable std::shared_mutex mutex_;
};

} // namespace sageFlow

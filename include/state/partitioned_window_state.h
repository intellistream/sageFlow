//
// Created for sageFlow architecture refactoring - Phase 2
//

#pragma once

#include "state/window_state.h"
#include <vector>
#include <shared_mutex>

namespace sageFlow {

/**
 * @brief 分区窗口状态实现
 * 
 * 每个子任务有独立的状态分片，无需跨任务同步。
 * 适用于基于分区的 Join 方法。
 */
class PartitionedWindowState : public WindowState {
public:
    explicit PartitionedWindowState(size_t parallelism);

    void addRecord(std::unique_ptr<VectorRecord> record, 
                  size_t subtask_index) override;

    const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const override;

    void evictExpired(int64_t current_timestamp, 
                    int64_t window_size,
                    size_t subtask_index) override;

    size_t size(size_t subtask_index) const override;

    bool isShared() const override { return false; }

private:
    // 每个子任务一个独立的窗口
    std::vector<std::deque<std::unique_ptr<VectorRecord>>> partitions_;
    
    // 每个分区一个独立的互斥锁
    mutable std::vector<std::shared_mutex> mutexes_;
};

} // namespace sageFlow

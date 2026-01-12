//
// Created for sageFlow architecture refactoring - Phase 2
//

#pragma once

#include "state/window_state.h"
#include <shared_mutex>
#include <atomic>
#include <limits>

namespace sageFlow {

/**
 * @brief 共享窗口状态实现
 * 
 * 所有子任务共享同一状态，需要跨任务同步。
 * 适用于共享索引的 Join 方法。
 * 
 * 延迟删除机制：
 * - 过期记录首先被标记（添加到 expired_uids_ 集合）
 * - 查询时可以检查 isExpired() 过滤已过期的候选项
 * - 当过期记录积累到阈值时，调用 flushExpiredUids() 批量返回待删除的 UID
 * 
 * 时间戳追踪：
 * - 全局追踪 max_seen_timestamp
 * - evict 使用 min(this_max, other_state_max) 确保乱序安全
 */
class SharedWindowState : public WindowState {
public:
    SharedWindowState();

    void addRecord(std::unique_ptr<VectorRecord> record, 
                  size_t subtask_index) override;

    const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const override;

    std::vector<std::shared_ptr<const VectorRecord>> 
        getRecordsSnapshot(size_t subtask_index) const override;

    bool containsUid(uint64_t uid, size_t subtask_index) const override;

    std::unordered_set<uint64_t> getUidSet(size_t subtask_index) const override;

    void evictExpired(int64_t current_timestamp, 
                    int64_t window_size,
                    size_t subtask_index) override;

    bool isExpired(uint64_t uid, size_t subtask_index) const override;

    size_t getExpiredCount(size_t subtask_index) const override;

    std::vector<uint64_t> flushExpiredUids(size_t subtask_index) override;

    size_t size(size_t subtask_index) const override;

    bool isShared() const override { return true; }

    // ==================== 时间戳追踪接口 ====================
    
    void updateMaxSeenTimestamp(int64_t timestamp, size_t subtask_index) override;
    
    int64_t getMaxSeenTimestamp(size_t subtask_index) const override;
    
    int64_t getSafeEvictTimestamp(size_t subtask_index, 
                                  const WindowState* other_state = nullptr) const override;

private:
    // 所有子任务共享的窗口
    std::deque<std::unique_ptr<VectorRecord>> shared_window_;
    
    // 已过期但未从 Index/Storage 删除的 UID 集合
    std::unordered_set<uint64_t> expired_uids_;
    
    // 共享状态的读写锁
    mutable std::shared_mutex mutex_;
    
    // 全局最大已见时间戳
    std::atomic<int64_t> max_seen_timestamp_{std::numeric_limits<int64_t>::min()};
};

} // namespace sageFlow

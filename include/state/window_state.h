//
// Created for sageFlow architecture refactoring - Phase 2
//

#pragma once

#include <memory>
#include <vector>
#include <deque>
#include <mutex>
#include "common/data_types.h"

namespace sageFlow {

/**
 * @brief 窗口状态抽象接口
 * 
 * WindowState 提供统一的窗口状态访问接口，支持：
 * 1. 分区状态（Partitioned State）：每个子任务有独立的状态
 * 2. 共享状态（Shared State）：所有子任务共享同一状态，需要同步
 */
class WindowState {
public:
    virtual ~WindowState() = default;

    /**
     * @brief 添加记录到窗口
     * @param record 待添加的记录
     * @param subtask_index 子任务索引（用于分区状态）
     */
    virtual void addRecord(std::unique_ptr<VectorRecord> record, 
                          size_t subtask_index) = 0;

    /**
     * @brief 获取窗口中的所有记录
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 窗口记录的引用（只读）
     */
    virtual const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const = 0;

    /**
     * @brief 清理过期记录
     * @param current_timestamp 当前时间戳
     * @param window_size 窗口大小
     * @param subtask_index 子任务索引（用于分区状态）
     */
    virtual void evictExpired(int64_t current_timestamp, 
                            int64_t window_size,
                            size_t subtask_index) = 0;

    /**
     * @brief 获取窗口大小
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 当前窗口中的记录数
     */
    virtual size_t size(size_t subtask_index) const = 0;

    /**
     * @brief 检查状态是否为共享状态
     * @return true 表示共享状态，false 表示分区状态
     */
    virtual bool isShared() const = 0;
};

} // namespace sageFlow

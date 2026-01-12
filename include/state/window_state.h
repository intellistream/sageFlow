//
// Created for sageFlow architecture refactoring - Phase 2
//

#pragma once

#include <memory>
#include <vector>
#include <deque>
#include <mutex>
#include <unordered_set>
#include "common/data_types.h"

namespace sageFlow {

/**
 * @brief 窗口状态抽象接口
 * 
 * WindowState 提供统一的窗口状态访问接口，支持：
 * 1. 分区状态（Partitioned State）：每个子任务有独立的状态
 * 2. 共享状态（Shared State）：所有子任务共享同一状态，需要同步
 * 
 * 延迟删除机制：
 * - 过期记录首先被标记（添加到 expired_uids_ 集合）
 * - 查询时可以检查 isExpired() 过滤已过期的候选项
 * - 当过期记录积累到阈值时，调用 flushExpiredUids() 批量返回待删除的 UID
 * - 外部（JoinOperator）负责从 Index/Storage 中批量删除这些记录
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
     * @warning 对于 SharedWindowState，返回的引用在多线程下不安全，
     *          应使用 getRecordsSnapshot() 获取线程安全的副本
     */
    virtual const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const = 0;

    /**
     * @brief 获取窗口记录的线程安全快照
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 窗口记录的指针向量副本（线程安全）
     */
    virtual std::vector<std::shared_ptr<const VectorRecord>> 
        getRecordsSnapshot(size_t subtask_index) const = 0;

    /**
     * @brief 检查窗口中是否包含指定 UID 的记录
     * @param uid 要检查的记录 UID
     * @param subtask_index 子任务索引（用于分区状态）
     * @return true 如果记录存在于窗口中
     */
    virtual bool containsUid(uint64_t uid, size_t subtask_index) const = 0;

    /**
     * @brief 获取窗口中存在的 UID 集合（用于批量验证）
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 当前窗口中所有记录的 UID 集合
     */
    virtual std::unordered_set<uint64_t> getUidSet(size_t subtask_index) const = 0;

    /**
     * @brief 清理过期记录（延迟删除版本）
     * 
     * 将过期记录标记为已过期，添加到 expired_uids_ buffer 中，
     * 但不立即从 Index/Storage 中删除。
     * 
     * @param current_timestamp 当前时间戳
     * @param window_size 窗口大小
     * @param subtask_index 子任务索引（用于分区状态）
     */
    virtual void evictExpired(int64_t current_timestamp, 
                            int64_t window_size,
                            size_t subtask_index) = 0;

    /**
     * @brief 检查指定 UID 是否已过期
     * @param uid 要检查的记录 UID
     * @param subtask_index 子任务索引（用于分区状态）
     * @return true 如果记录已被标记为过期
     */
    virtual bool isExpired(uint64_t uid, size_t subtask_index) const = 0;

    /**
     * @brief 获取已过期但未删除的 UID 数量
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 待删除的过期记录数量
     */
    virtual size_t getExpiredCount(size_t subtask_index) const = 0;

    /**
     * @brief 获取并清空过期 UID buffer（用于批量删除）
     * 
     * 返回所有已标记为过期的 UID，并清空内部 buffer。
     * 调用方（JoinOperator）负责从 Index/Storage 中删除这些记录。
     * 
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 待从 Index/Storage 中删除的 UID 列表
     */
    virtual std::vector<uint64_t> flushExpiredUids(size_t subtask_index) = 0;

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

    /**
     * @brief 设置过期缓冲区倍数
     * 
     * evictExpired 的过期条件为: timestamp < current_timestamp - multiplier * window_size
     * 默认值为 2.0，表示 2 倍窗口大小的缓冲区
     * 
     * @param multiplier 缓冲区倍数（必须 >= 1.0）
     */
    void setEvictionBufferMultiplier(double multiplier) {
        eviction_buffer_multiplier_ = std::max(1.0, multiplier);
    }

    /**
     * @brief 获取过期缓冲区倍数
     * @return 当前的缓冲区倍数
     */
    double getEvictionBufferMultiplier() const {
        return eviction_buffer_multiplier_;
    }

    /**
     * @brief 默认的过期缓冲区倍数
     */
    static constexpr double DEFAULT_EVICTION_BUFFER_MULTIPLIER = 2.0;

    // ==================== 时间戳追踪接口 ====================
    
    /**
     * @brief 更新最大已见时间戳
     * 
     * 分区模式：每个 subtask 独立追踪时间戳
     * 共享模式：全局追踪时间戳
     * 
     * @param timestamp 当前记录的时间戳
     * @param subtask_index 子任务索引
     */
    virtual void updateMaxSeenTimestamp(int64_t timestamp, size_t subtask_index) = 0;
    
    /**
     * @brief 获取最大已见时间戳
     * @param subtask_index 子任务索引（分区模式使用）
     * @return 该分区/全局的最大已见时间戳
     */
    virtual int64_t getMaxSeenTimestamp(size_t subtask_index) const = 0;
    
    /**
     * @brief 计算安全的 evict 时间戳
     * 
     * 返回可以安全清理记录的时间戳阈值。
     * - 分区模式：返回该分区的 max_seen_ts
     * - 共享模式：返回全局 min(left_max, right_max)
     * 
     * @param subtask_index 子任务索引
     * @param other_state 对侧窗口状态（用于共享模式取 min）
     * @return 安全 evict 时间戳
     */
    virtual int64_t getSafeEvictTimestamp(size_t subtask_index, 
                                          const WindowState* other_state = nullptr) const = 0;

protected:
    /// 过期缓冲区倍数，默认 2.0 表示 2 倍窗口大小
    double eviction_buffer_multiplier_ = DEFAULT_EVICTION_BUFFER_MULTIPLIER;
};

} // namespace sageFlow

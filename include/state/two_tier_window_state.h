//
// Created for sageFlow architecture refactoring - Phase 2
// Task A-01: TwoTierWindowState 双层窗口状态
//

#pragma once

#include "state/window_state.h"
#include <deque>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <limits>

namespace sageFlow {

/**
 * @brief 双层窗口状态实现
 * 
 * 将窗口分为写友好层（Write-Friendly Tier）和紧凑层（Compact Tier），
 * 优化高频插入和相似性查询的性能。
 * 
 * - Write-Friendly Tier (write_tier_): 使用 deque，快速吸收新插入
 * - Compact Tier (compact_tier_): 使用 vector，按时间戳排序，优化查询
 */
class TwoTierWindowState : public WindowState {
public:
    /**
     * @brief 构造函数
     * @param parallelism 并行度，决定分区数量
     * @param compact_threshold 触发压缩的写层大小阈值
     * @param merge_batch_size 批量合并大小
     */
    explicit TwoTierWindowState(size_t parallelism,
                                size_t compact_threshold = 100,
                                size_t merge_batch_size = 50);

    /**
     * @brief 添加记录到窗口（写层）
     * @param record 待添加的记录
     * @param subtask_index 子任务索引
     */
    void addRecord(RecordView record,
                   size_t subtask_index) override;

    /**
     * @brief 获取窗口中的所有记录（合并视图）
     * @param subtask_index 子任务索引
     * @return 窗口记录的引用（只读）
     */
    const std::deque<RecordView>&
        getRecords(size_t subtask_index) const override;

    /**
     * @brief 获取窗口记录的线程安全快照
     * @param subtask_index 子任务索引
     * @return 窗口记录的指针向量副本（线程安全）
     */
    std::vector<RecordView>
        getRecordsSnapshot(size_t subtask_index) const override;

    /**
     * @brief 检查窗口中是否包含指定 UID 的记录
     * @param uid 要检查的记录 UID
     * @param subtask_index 子任务索引
     * @return true 如果记录存在于窗口中
     */
    bool containsUid(uint64_t uid, size_t subtask_index) const override;

    /**
     * @brief 获取窗口中存在的 UID 集合
     * @param subtask_index 子任务索引
     * @return 当前窗口中所有记录的 UID 集合
     */
    std::unordered_set<uint64_t> getUidSet(size_t subtask_index) const override;

    /**
     * @brief 清理过期记录（同时清理两层）
     * @param current_timestamp 当前时间戳
     * @param window_size 窗口大小
     * @param subtask_index 子任务索引
     */
    void evictExpired(int64_t current_timestamp, 
                      int64_t window_size,
                      size_t subtask_index) override;

    /**
     * @brief 检查指定 UID 是否已过期
     * @param uid 要检查的记录 UID
     * @param subtask_index 子任务索引
     * @return true 如果记录已被标记为过期
     */
    bool isExpired(uint64_t uid, size_t subtask_index) const override;

    /**
     * @brief 获取已过期但未删除的 UID 数量
     * @param subtask_index 子任务索引
     * @return 待删除的过期记录数量
     */
    size_t getExpiredCount(size_t subtask_index) const override;

    /**
     * @brief 获取并清空过期 UID buffer
     * @param subtask_index 子任务索引
     * @return 待从 Index/Storage 中删除的 UID 列表
     */
    std::vector<uint64_t> flushExpiredUids(size_t subtask_index) override;

    /**
     * @brief 获取窗口大小（两层总和）
     * @param subtask_index 子任务索引
     * @return 当前窗口中的记录数
     */
    size_t size(size_t subtask_index) const override;

    /**
     * @brief 检查状态是否为共享状态
     * @return false，表示分区状态
     */
    bool isShared() const override { return false; }

    // ==================== 时间戳追踪接口 ====================
    
    void updateMaxSeenTimestamp(int64_t timestamp, size_t subtask_index) override;
    
    int64_t getMaxSeenTimestamp(size_t subtask_index) const override;
    
    int64_t getSafeEvictTimestamp(size_t subtask_index, 
                                  const WindowState* other_state = nullptr) const override;

    // ========== 新增方法 ==========

    /**
     * @brief 将写层记录压缩迁移到紧凑层
     * @param subtask_index 子任务索引
     */
    void compactTiers(size_t subtask_index);

    /**
     * @brief 获取紧凑层记录（用于优化查询）
     * @param subtask_index 子任务索引
     * @return 紧凑层记录的只读引用
     */
    const std::vector<RecordView>&
        getCompactRecords(size_t subtask_index) const;

    /**
     * @brief 获取所有记录（写层+紧凑层合并视图）
     * @param subtask_index 子任务索引
     * @return 所有记录的指针向量
     */
    std::vector<const VectorRecord*> getAllRecords(size_t subtask_index) const;

    /**
     * @brief 获取写层大小
     * @param subtask_index 子任务索引
     * @return 写层记录数
     */
    size_t getWriteTierSize(size_t subtask_index) const;

    /**
     * @brief 获取紧凑层大小
     * @param subtask_index 子任务索引
     * @return 紧凑层记录数
     */
    size_t getCompactTierSize(size_t subtask_index) const;

private:
    /**
     * @brief 每个分区的双层结构
     */
    struct TierPair {
        std::deque<RecordView> write_tier_;
        std::vector<RecordView> compact_tier_;
        std::unordered_set<uint64_t> expired_uids_;  // 已过期但未删除的 UID
        mutable std::shared_mutex mutex_;
        
        // 用于 getRecords() 返回的临时合并视图
        mutable std::deque<RecordView> merged_view_;
        mutable bool view_dirty_ = true;
    };

    std::vector<TierPair> partitions_;
    size_t compact_threshold_;
    size_t merge_batch_size_;
    
    // 每个分区独立追踪的最大已见时间戳
    std::vector<std::atomic<int64_t>> max_seen_timestamps_;

    /**
     * @brief 检查是否需要压缩
     * @param subtask_index 子任务索引
     * @return true 表示需要压缩
     */
    bool needsCompaction(size_t subtask_index) const;

    /**
     * @brief 更新合并视图
     * @param subtask_index 子任务索引
     */
    void updateMergedView(size_t subtask_index) const;
};

} // namespace sageFlow

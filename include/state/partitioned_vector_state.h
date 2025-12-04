//
// Created for sageFlow architecture refactoring - Phase 2
// Task B-02: PartitionedVectorState 分区向量状态
//

#pragma once

#include "state/window_state.h"
#include "state/two_tier_window_state.h"
#include "execution/vector_space_partitioner.h"
#include "coordination/boundary_tracker.h"

#include <deque>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace sageFlow {

/**
 * @brief 分区向量状态
 *
 * 结合双层窗口和向量空间分区的状态管理。
 * 每个向量空间分区拥有独立的 TwoTierWindowState。
 *
 * 核心特性：
 * 1. 基于向量相似性的分区路由
 * 2. 跨分区查询支持
 * 3. 边界向量追踪
 * 4. 双层窗口优化
 */
class PartitionedVectorState : public WindowState {
public:
    /**
     * @brief 构造函数
     * @param num_partitions 向量空间分区数
     * @param partitioner 向量空间分区器
     * @param compact_threshold 双层窗口压缩阈值
     * @param enable_boundary_tracking 是否启用边界向量追踪
     */
    PartitionedVectorState(size_t num_partitions,
                           std::shared_ptr<VectorSpacePartitioner> partitioner,
                           size_t compact_threshold = 100,
                           bool enable_boundary_tracking = true);

    /**
     * @brief 析构函数
     */
    ~PartitionedVectorState() override = default;

    // ========== WindowState 接口实现 ==========

    /**
     * @brief 添加记录到窗口
     *
     * 使用分区器确定目标分区，并将记录添加到对应分区的状态中。
     * 同时更新 uid 到分区的映射，以及边界向量追踪。
     *
     * @param record 待添加的记录
     * @param subtask_index 子任务索引（在此实现中被忽略，分区由向量内容决定）
     */
    void addRecord(std::unique_ptr<VectorRecord> record,
                   size_t subtask_index) override;

    /**
     * @brief 获取窗口中的所有记录（合并视图）
     *
     * 由于接口要求返回 deque 引用，需要维护一个合并视图。
     * 包含所有分区的记录。
     *
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return 窗口记录的引用（只读）
     */
    const std::deque<std::unique_ptr<VectorRecord>>&
        getRecords(size_t subtask_index) const override;

    /**
     * @brief 获取窗口记录的线程安全快照
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return 窗口记录的指针向量副本（线程安全）
     */
    std::vector<std::shared_ptr<const VectorRecord>> 
        getRecordsSnapshot(size_t subtask_index) const override;

    /**
     * @brief 检查窗口中是否包含指定 UID 的记录
     * @param uid 要检查的记录 UID
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return true 如果记录存在于窗口中
     */
    bool containsUid(uint64_t uid, size_t subtask_index) const override;

    /**
     * @brief 获取窗口中存在的 UID 集合
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return 当前窗口中所有记录的 UID 集合
     */
    std::unordered_set<uint64_t> getUidSet(size_t subtask_index) const override;

    /**
     * @brief 清理过期记录
     *
     * 遍历所有分区进行过期清理，同时更新 uid 映射和边界追踪。
     *
     * @param current_timestamp 当前时间戳
     * @param window_size 窗口大小
     * @param subtask_index 子任务索引（在此实现中被忽略）
     */
    void evictExpired(int64_t current_timestamp,
                      int64_t window_size,
                      size_t subtask_index) override;

    /**
     * @brief 检查指定 UID 是否已过期
     * @param uid 要检查的记录 UID
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return true 如果记录已被标记为过期
     */
    bool isExpired(uint64_t uid, size_t subtask_index) const override;

    /**
     * @brief 获取已过期但未删除的 UID 数量
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return 待删除的过期记录数量
     */
    size_t getExpiredCount(size_t subtask_index) const override;

    /**
     * @brief 获取并清空过期 UID buffer
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return 待从 Index/Storage 中删除的 UID 列表
     */
    std::vector<uint64_t> flushExpiredUids(size_t subtask_index) override;

    /**
     * @brief 获取窗口大小（所有分区总和）
     * @param subtask_index 子任务索引（在此实现中被忽略）
     * @return 当前窗口中的记录数
     */
    size_t size(size_t subtask_index) const override;

    /**
     * @brief 检查状态是否为共享状态
     * @return false，表示分区状态
     */
    bool isShared() const override { return false; }

    /**
     * @brief 设置过期缓冲区倍数（传播到所有子分区）
     * @param multiplier 缓冲区倍数（必须 >= 1.0）
     */
    void setEvictionBufferMultiplier(double multiplier) {
        WindowState::setEvictionBufferMultiplier(multiplier);
        // 传播到所有子分区
        for (auto& partition : partitions_) {
            partition->setEvictionBufferMultiplier(multiplier);
        }
    }

    // ========== 分区特定操作 ==========

    /**
     * @brief 获取查询相关的记录
     *
     * 使用分区器确定需要探测的候选分区，收集这些分区的所有记录。
     * 如果启用边界追踪，还会包含边界向量。
     *
     * @param query 查询向量
     * @param num_probes 探测分区数（默认2）
     * @return 相关分区的所有记录指针
     */
    std::vector<const VectorRecord*> getRecordsForQuery(
        const VectorRecord& query, size_t num_probes = 2) const;

    /**
     * @brief 获取特定分区的记录
     * @param partition_id 分区ID
     * @return 该分区的所有记录指针
     */
    std::vector<const VectorRecord*> getRecordsForPartition(size_t partition_id) const;

    /**
     * @brief 获取边界向量
     * @param partition_id 分区ID
     * @return 该分区的边界向量UID列表
     */
    std::vector<uint64_t> getBoundaryVectors(size_t partition_id) const;

    /**
     * @brief 获取分区数量
     * @return 向量空间分区数
     */
    size_t getNumPartitions() const { return num_partitions_; }

    /**
     * @brief 获取各分区大小
     * @return 各分区的记录数向量
     */
    std::vector<size_t> getPartitionSizes() const;

    /**
     * @brief 获取总记录数
     * @return 所有分区的记录总数
     */
    size_t totalSize() const;

    /**
     * @brief 触发所有分区的层压缩
     */
    void compactAllPartitions();

    /**
     * @brief 检查是否启用边界追踪
     * @return true 表示启用边界追踪
     */
    bool isBoundaryTrackingEnabled() const { return enable_boundary_tracking_; }

    /**
     * @brief 获取向量所属分区
     * @param uid 向量唯一ID
     * @return 分区ID，如果不存在返回 -1
     */
    int64_t getPartitionForUid(uint64_t uid) const;

    /**
     * @brief 根据 UID 查找记录
     * @param uid 向量唯一ID
     * @return 记录指针，如果不存在返回 nullptr
     */
    const VectorRecord* findRecordByUid(uint64_t uid) const;

private:
    size_t num_partitions_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    bool enable_boundary_tracking_;
    size_t compact_threshold_;

    /// 每个向量空间分区的状态（使用 TwoTierWindowState）
    std::vector<std::unique_ptr<TwoTierWindowState>> partitions_;

    /// 边界向量追踪器
    std::unique_ptr<BoundaryTracker> boundary_tracker_;

    /// uid -> partition_id 映射
    std::unordered_map<uint64_t, size_t> uid_partition_map_;
    mutable std::shared_mutex uid_map_mutex_;

    /// 用于 getRecords() 的合并视图
    mutable std::deque<std::unique_ptr<VectorRecord>> merged_view_;
    mutable std::shared_mutex merge_mutex_;
    mutable bool view_dirty_ = true;

    /// 全局已过期 UID 集合（跨分区）
    std::unordered_set<uint64_t> expired_uids_;
    mutable std::shared_mutex expired_mutex_;

    /// uid -> VectorRecord* 映射，用于快速查找（mutable 以支持 const 方法中的缓存更新）
    mutable std::unordered_map<uint64_t, const VectorRecord*> uid_record_map_;
    mutable std::shared_mutex record_map_mutex_;

    /**
     * @brief 确定向量所属分区
     * @param record 向量记录
     * @return 分区ID
     */
    size_t getPartitionId(const VectorRecord& record) const;

    /**
     * @brief 更新边界向量追踪
     * @param record 向量记录
     * @param partition_id 分区ID
     */
    void updateBoundaryTracking(const VectorRecord& record, size_t partition_id);

    /**
     * @brief 更新合并视图
     */
    void updateMergedView() const;

    /**
     * @brief 收集被驱逐的 UID
     * @param partition_id 分区ID
     * @param before_size 驱逐前的大小
     * @param after_size 驱逐后的大小
     * @return 被驱逐的 UID 列表
     */
    std::vector<uint64_t> collectEvictedUids(size_t partition_id,
                                              size_t before_size,
                                              size_t after_size) const;
};

} // namespace sageFlow

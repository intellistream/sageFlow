#pragma once

#include "coordination/boundary_tracker.h"
#include "coordination/late_arrival_handler.h"
#include "execution/vector_space_partitioner.h"

#include <atomic>
#include <memory>
#include <vector>

namespace sageFlow {

/**
 * @brief 记录处理结果
 */
struct ProcessResult {
    ArrivalStatus status;   ///< 到达状态
    size_t partition_id;    ///< 目标分区
    bool is_boundary;       ///< 是否为边界向量
};

/**
 * @brief 分区统计信息
 */
struct PartitionStats {
    size_t partition_id;    ///< 分区ID
    size_t record_count;    ///< 记录数量
    size_t boundary_count;  ///< 边界向量数量
};

/**
 * @brief 分区协调器
 *
 * 协调跨分区查询和延迟到达处理。
 * 管理分区负载均衡和边界向量追踪。
 *
 * 线程安全，支持高并发访问。
 */
class PartitionCoordinator {
 public:
    /**
     * @brief 构造函数
     * @param num_partitions 分区数量
     * @param partitioner 向量空间分区器
     * @param allowed_lateness 允许的延迟时间（毫秒）
     * @param watermark_delay watermark 延迟（毫秒）
     */
    PartitionCoordinator(size_t num_partitions,
                         std::shared_ptr<VectorSpacePartitioner> partitioner,
                         int64_t allowed_lateness = 5000,
                         int64_t watermark_delay = 1000);

    /**
     * @brief 析构函数
     */
    ~PartitionCoordinator() = default;

    // 禁用拷贝
    PartitionCoordinator(const PartitionCoordinator&) = delete;
    auto operator=(const PartitionCoordinator&) -> PartitionCoordinator& = delete;

    // 允许移动
    PartitionCoordinator(PartitionCoordinator&&) noexcept = default;
    auto operator=(PartitionCoordinator&&) noexcept -> PartitionCoordinator& = default;

    /**
     * @brief 路由查询到相关分区
     * @param query 查询向量
     * @param num_probes 探测分区数
     * @return 需要查询的分区ID列表
     */
    [[nodiscard]] auto routeQuery(const VectorRecord& query, size_t num_probes = 2) -> std::vector<size_t>;

    /**
     * @brief 处理到达的记录
     * @param record 到达的记录
     * @return 记录的到达状态、目标分区和边界标记
     */
    auto processRecord(const VectorRecord& record) -> ProcessResult;

    /**
     * @brief 标记边界向量
     * @param uid 向量唯一ID
     * @param partition_id 所属分区
     */
    void markBoundary(uint64_t uid, size_t partition_id);

    /**
     * @brief 取消边界标记
     * @param uid 向量唯一ID
     */
    void unmarkBoundary(uint64_t uid);

    /**
     * @brief 获取分区的边界向量
     * @param partition_id 分区ID
     * @return 边界向量UID列表
     */
    [[nodiscard]] auto getBoundaryVectors(size_t partition_id) const -> std::vector<uint64_t>;

    /**
     * @brief 缓冲延迟记录
     * @param record 延迟记录
     */
    void bufferLateRecord(std::unique_ptr<VectorRecord> record);

    /**
     * @brief 获取并清空延迟缓冲区
     * @return 缓冲的延迟记录
     */
    auto flushLateBuffer() -> std::vector<std::unique_ptr<VectorRecord>>;

    /**
     * @brief 获取延迟缓冲区大小
     * @return 缓冲区中的记录数量
     */
    [[nodiscard]] auto getLateBufferSize() const -> size_t;

    /**
     * @brief 更新分区记录计数
     * @param partition_id 分区ID
     * @param delta 变化量（正数增加，负数减少）
     */
    void updatePartitionCount(size_t partition_id, int64_t delta);

    /**
     * @brief 获取分区统计信息
     * @return 各分区的统计信息
     */
    [[nodiscard]] auto getPartitionStats() const -> std::vector<PartitionStats>;

    /**
     * @brief 检测是否需要重平衡
     * @param imbalance_threshold 不平衡阈值 (max/avg)
     * @return 是否需要重平衡
     */
    [[nodiscard]] auto needsRebalance(double imbalance_threshold = 2.0) const -> bool;

    /**
     * @brief 获取延迟处理统计
     * @return 延迟到达统计信息引用
     */
    [[nodiscard]] auto getLateArrivalStats() const -> const LateArrivalStats&;

    /**
     * @brief 获取当前 watermark
     * @return 当前 watermark 值
     */
    [[nodiscard]] auto getWatermark() const -> int64_t;

    /**
     * @brief 获取分区数量
     * @return 分区数量
     */
    [[nodiscard]] auto getNumPartitions() const -> size_t { return num_partitions_; }

    /**
     * @brief 获取 BoundaryTracker 实例（用于测试）
     * @return BoundaryTracker 指针
     */
    [[nodiscard]] auto getBoundaryTracker() const -> BoundaryTracker* { return boundary_tracker_.get(); }

    /**
     * @brief 获取 LateArrivalHandler 实例（用于测试）
     * @return LateArrivalHandler 指针
     */
    [[nodiscard]] auto getLateArrivalHandler() const -> LateArrivalHandler* { return late_handler_.get(); }

 private:
    /// 分区数量
    size_t num_partitions_;

    /// 向量空间分区器
    std::shared_ptr<VectorSpacePartitioner> partitioner_;

    /// 边界向量追踪器
    std::unique_ptr<BoundaryTracker> boundary_tracker_;

    /// 延迟到达处理器
    std::unique_ptr<LateArrivalHandler> late_handler_;

    /// 分区记录计数
    std::vector<std::atomic<size_t>> partition_counts_;
};

}  // namespace sageFlow

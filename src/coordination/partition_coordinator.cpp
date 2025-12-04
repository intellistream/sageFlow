#include "coordination/partition_coordinator.h"

#include <algorithm>
#include <numeric>
#include <unordered_set>

namespace sageFlow {

PartitionCoordinator::PartitionCoordinator(size_t num_partitions,
                                           std::shared_ptr<VectorSpacePartitioner> partitioner,
                                           int64_t allowed_lateness,
                                           int64_t watermark_delay)
    : num_partitions_(num_partitions),
      partitioner_(std::move(partitioner)),
      boundary_tracker_(std::make_unique<BoundaryTracker>()),
      late_handler_(std::make_unique<LateArrivalHandler>(allowed_lateness, watermark_delay)),
      partition_counts_(num_partitions) {
    // 初始化所有分区计数为0
    for (auto& count : partition_counts_) {
        count.store(0, std::memory_order_relaxed);
    }
}

auto PartitionCoordinator::routeQuery(const VectorRecord& query, size_t num_probes) -> std::vector<size_t> {
    // 1. 使用分区器获取候选分区
    std::vector<size_t> candidate_partitions = partitioner_->getCandidatePartitions(query, num_partitions_, num_probes);

    // 使用 unordered_set 去重
    std::unordered_set<size_t> result_set(candidate_partitions.begin(), candidate_partitions.end());

    // 2. 获取候选分区中的边界向量，并添加它们所属的其他分区
    for (size_t partition_id : candidate_partitions) {
        auto boundary_uids = boundary_tracker_->getBoundaryVectorsForPartition(partition_id);
        // 边界向量本身就在该分区，无需额外添加其他分区
        // 但可以考虑边界向量可能需要与其他分区的向量进行比较
        // 这里保持简单实现，后续可以根据需要扩展
    }

    // 3. 返回去重后的分区列表
    return std::vector<size_t>(result_set.begin(), result_set.end());
}

auto PartitionCoordinator::processRecord(const VectorRecord& record) -> ProcessResult {
    ProcessResult result;

    // 1. 检查到达状态
    result.status = late_handler_->processRecord(record);

    // 2. 确定分区
    result.partition_id = partitioner_->partition(record, num_partitions_);

    // 3. 检查是否为边界向量
    result.is_boundary = partitioner_->isBoundaryVector(record, num_partitions_);

    // 4. 如果是边界向量且不是太迟到达，标记
    if (result.is_boundary && result.status != ArrivalStatus::TOO_LATE) {
        markBoundary(record.uid_, result.partition_id);
    }

    return result;
}

void PartitionCoordinator::markBoundary(uint64_t uid, size_t partition_id) {
    boundary_tracker_->markAsBoundary(uid, partition_id);
}

void PartitionCoordinator::unmarkBoundary(uint64_t uid) {
    boundary_tracker_->unmark(uid);
}

auto PartitionCoordinator::getBoundaryVectors(size_t partition_id) const -> std::vector<uint64_t> {
    return boundary_tracker_->getBoundaryVectorsForPartition(partition_id);
}

void PartitionCoordinator::bufferLateRecord(std::unique_ptr<VectorRecord> record) {
    late_handler_->bufferLateRecord(std::move(record));
}

auto PartitionCoordinator::flushLateBuffer() -> std::vector<std::unique_ptr<VectorRecord>> {
    return late_handler_->flushLateBuffer();
}

auto PartitionCoordinator::getLateBufferSize() const -> size_t {
    return late_handler_->getLateBufferSize();
}

void PartitionCoordinator::updatePartitionCount(size_t partition_id, int64_t delta) {
    if (partition_id >= num_partitions_) {
        return;  // 忽略无效的分区ID
    }

    if (delta >= 0) {
        partition_counts_[partition_id].fetch_add(static_cast<size_t>(delta), std::memory_order_relaxed);
    } else {
        // 处理减少的情况，确保不会下溢
        auto current = partition_counts_[partition_id].load(std::memory_order_relaxed);
        auto decrease = static_cast<size_t>(-delta);
        if (current >= decrease) {
            partition_counts_[partition_id].fetch_sub(decrease, std::memory_order_relaxed);
        } else {
            partition_counts_[partition_id].store(0, std::memory_order_relaxed);
        }
    }
}

auto PartitionCoordinator::getPartitionStats() const -> std::vector<PartitionStats> {
    std::vector<PartitionStats> stats;
    stats.reserve(num_partitions_);

    auto boundary_stats = boundary_tracker_->getPartitionStats();

    for (size_t i = 0; i < num_partitions_; ++i) {
        PartitionStats ps;
        ps.partition_id = i;
        ps.record_count = partition_counts_[i].load(std::memory_order_relaxed);

        auto it = boundary_stats.find(i);
        ps.boundary_count = (it != boundary_stats.end()) ? it->second : 0;

        stats.push_back(ps);
    }

    return stats;
}

auto PartitionCoordinator::needsRebalance(double imbalance_threshold) const -> bool {
    if (num_partitions_ == 0) {
        return false;
    }

    size_t total = 0;
    size_t max_count = 0;

    for (size_t i = 0; i < num_partitions_; ++i) {
        size_t count = partition_counts_[i].load(std::memory_order_relaxed);
        total += count;
        if (count > max_count) {
            max_count = count;
        }
    }

    // 如果没有记录，不需要重平衡
    if (total == 0) {
        return false;
    }

    double avg = static_cast<double>(total) / static_cast<double>(num_partitions_);

    // 避免除零
    if (avg < 1e-9) {
        return false;
    }

    double ratio = static_cast<double>(max_count) / avg;
    return ratio > imbalance_threshold;
}

auto PartitionCoordinator::getLateArrivalStats() const -> const LateArrivalStats& {
    return late_handler_->getStats();
}

auto PartitionCoordinator::getWatermark() const -> int64_t {
    return late_handler_->getWatermark();
}

}  // namespace sageFlow

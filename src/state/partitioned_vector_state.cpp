//
// Created for sageFlow architecture refactoring - Phase 2
// Task B-02: PartitionedVectorState 分区向量状态
//

#include <atomic>
#include "state/partitioned_vector_state.h"
#include "utils/logger.h"
#include "compute_engine/simd_distance.h" //  使用项目的高性能 SIMD 库

#include <cmath>
#include <limits>
#include <algorithm>
#include <unordered_set>

namespace sageFlow {

PartitionedVectorState::PartitionedVectorState(
    size_t num_partitions,
    std::shared_ptr<VectorSpacePartitioner> partitioner,
    size_t compact_threshold,
    bool enable_boundary_tracking)
    : num_partitions_(num_partitions),
      partitioner_(std::move(partitioner)),
      enable_boundary_tracking_(enable_boundary_tracking),
      compact_threshold_(compact_threshold) {

    if (num_partitions_ == 0) {
        throw std::invalid_argument("PartitionedVectorState: num_partitions must be positive");
    }

    if (!partitioner_) {
        throw std::invalid_argument("PartitionedVectorState: partitioner cannot be null");
    }

    // 创建 num_partitions 个 TwoTierWindowState 实例
    // 每个分区的 parallelism 设为 1（分区内不再细分）
    partitions_.reserve(num_partitions_);
    for (size_t i = 0; i < num_partitions_; ++i) {
        partitions_.push_back(std::make_unique<TwoTierWindowState>(
            1,  // parallelism = 1，每个分区只有一个子任务
            compact_threshold_,
            compact_threshold_ / 2  // merge_batch_size 设为阈值的一半
        ));
    }

    // 如果启用边界追踪，创建 BoundaryTracker
    if (enable_boundary_tracking_) {
        boundary_tracker_ = std::make_unique<BoundaryTracker>();
    }

    SAGEFLOW_LOG_DEBUG("PartitionedVectorState",
        "Created PartitionedVectorState with num_partitions={}, "
        "compact_threshold={}, boundary_tracking={}",
        num_partitions_, compact_threshold_, enable_boundary_tracking_);
}

void PartitionedVectorState::addRecord(std::unique_ptr<VectorRecord> record,
                                       size_t subtask_index) {
    if (!record) {
        return;
    }

    // [DEBUG LOGGING]
    static std::atomic<uint64_t> p_stats[32] = {0}; // 假设最大并行度32
    size_t p_id = getPartitionId(*record);
    
    uint64_t count = p_stats[p_id].fetch_add(1, std::memory_order_relaxed);
    // 每处理 500 条数据打印一次分布，避免刷屏
    if (count > 0 && count % 500 == 0) {
        SAGEFLOW_LOG_INFO("SkewDebug", "Partition [{}] received total {} records", p_id, count);
    }

    // [S3J] 检查是否开启了 S3J 动态构建模式
    // 如果设置了阈值，且 record 有效，则走 S3J 逻辑 (Layer 2)
    if (s3j_threshold_ > 0.0f) {
        addRecordS3J(std::move(record));
        return; 
    }

    // 确定向量所属分区
    size_t partition_id = getPartitionId(*record);
    uint64_t uid = record->uid_;

    // 更新 uid_partition_map_
    {
        std::unique_lock lock(uid_map_mutex_);
        uid_partition_map_[uid] = partition_id;
    }

    // 获取记录指针用于后续操作
    const VectorRecord* record_ptr = record.get();

    // 将记录添加到对应分区的 TwoTierWindowState
    // 分区内 subtask_index 固定为 0
    partitions_[partition_id]->addRecord(std::move(record), 0);

    // 更新 uid -> record 映射
    {
        std::unique_lock lock(record_map_mutex_);
        // 由于记录已被移动，我们需要从分区中获取记录指针
        auto records = partitions_[partition_id]->getAllRecords(0);
        for (const auto* rec : records) {
            if (rec->uid_ == uid) {
                uid_record_map_[uid] = rec;
                record_ptr = rec;
                break;
            }
        }
    }

    // 如果启用边界追踪，更新边界向量
    if (enable_boundary_tracking_ && record_ptr != nullptr) {
        updateBoundaryTracking(*record_ptr, partition_id);
    }

    // 标记视图需要更新
    {
        std::unique_lock lock(merge_mutex_);
        view_dirty_ = true;
    }
}

// 2. 新增 addRecordS3J 实现 (Paper Section 7.1 - 7.5)
void PartitionedVectorState::addRecordS3J(std::unique_ptr<VectorRecord> record) {
    if (!record) return;

    // 准备参数
    float t = s3j_threshold_;
    float t_half = t / 2.0f;
    float t_double = t * 2.0f;
    
    // 我们需要保留 record 的 raw 指针用于多次计算，但所有权要在最后移交
    // 技巧：先持有 unique_ptr，如果需要存入多个集合（Outer），则深拷贝
    VectorRecord* raw_rec = record.get();
    size_t dim = raw_rec->data_.dim_;
    const float* rec_ptr = reinterpret_cast<const float*>(raw_rec->data_.data_.get());

    // Step 1: 寻找最近的 Workset (Paper Section 7.2)
    auto [nearest_workset, min_dist] = findNearestWorkset(*raw_rec);

    bool assigned_to_inner = false;

    // Step 2 & 3: 判定归属 (Inner vs New Workset vs Outlier)
    
    // Case A: 加入 Inner Set (dist <= t/2) [cite: 62-65, 82]
    if (nearest_workset && min_dist <= t_half) {
        nearest_workset->inner_set->addRecord(std::move(record), 0);
        assigned_to_inner = true;
        // 增加负载计数 (Approximate)
        nearest_workset->computation_cost.fetch_add(1, std::memory_order_relaxed);
    }
    // Case B: 创建新 Workset (dist > t) [cite: 66, 298-302]
    // 论文 Criterion 2: 如果距离所有现有质心 > t，则选为新质心
    else if (!nearest_workset || min_dist > t) {
        // 生成新 ID
        uint64_t new_id = next_workset_id_.fetch_add(1);
        
        // 当前记录作为质心 (深拷贝)
        auto centroid_copy = std::make_unique<VectorRecord>(*raw_rec);
        createWorkset(new_id, std::move(centroid_copy));
        
        // 重新获取新创建的 Workset (createWorkset 内部加了锁)
        S3JWorkset* new_ws = getWorkset(new_id);
        if (new_ws) {
            new_ws->inner_set->addRecord(std::move(record), 0);
            assigned_to_inner = true;
        }
    }
    // Case C: 成为 Outlier (t/2 < dist <= t) [cite: 304-307]
    else {
        // 加入到最近 Workset 的 Outliers 集合
        nearest_workset->outliers->addRecord(std::move(record), 0);
        // 此处不置 assigned_to_inner，因为 Outlier 需要参与更多比较
        nearest_workset->computation_cost.fetch_add(1, std::memory_order_relaxed);
    }

    // 论文 Definition 10: dist <= 2t (且 > t/2，因为 <=t/2 是 Inner)
    
    auto snapshots = getWorksetsSnapshot();
    for (auto* ws : snapshots) {
        // 跳过它刚刚加入 Inner Set 的那个 Workset 
        if (assigned_to_inner && ws == nearest_workset) continue;
        
        // 计算距离
        const float* cen_ptr = reinterpret_cast<const float*>(ws->centroid->data_.data_.get());
        float dist = SIMDDistance::l2Distance(rec_ptr, cen_ptr, dim);
        
        // 路由准则: t/2 < dist <= 2t
        if (dist <= t_double && dist > t_half) {
            // 深拷贝一份放入 Outer Set
            auto record_copy = std::make_unique<VectorRecord>(
                raw_rec->uid_, raw_rec->timestamp_, raw_rec->data_ 
            );
            // 手动复制数据，如果 VectorData 拷贝不完整
            if (record_copy->data_.dim_ == 0) {
   
            }
            
            ws->outer_set->addRecord(std::move(record_copy), 0);
            ws->migration_cost.fetch_add(1, std::memory_order_relaxed); // 增加存储/迁移成本计数
        }
    }
}

// [S3J] 释放(迁出) Workset
std::unique_ptr<S3JWorkset> PartitionedVectorState::releaseWorkset(uint64_t workset_id) {
    // 获取写锁 (Unique Lock)，因为我们要修改 map 结构
    std::unique_lock lock(workset_map_mutex_);

    auto it = s3j_worksets_.find(workset_id);
    if (it == s3j_worksets_.end()) {
        // ID 不存在，返回空指针
        return nullptr;
    }

    // 移动语义：将指针的所有权提取出来
    std::unique_ptr<S3JWorkset> workset_ptr = std::move(it->second);

    // 从 Map 中移除该条目
    s3j_worksets_.erase(it);

    // 返回提取出的 Workset 对象
    return workset_ptr;
}

// [S3J] 注入(迁入) Workset
void PartitionedVectorState::injectWorkset(std::unique_ptr<S3JWorkset> workset) {
    if (!workset) return;

    uint64_t id = workset->workset_id;

    // 获取写锁 (Unique Lock)
    std::unique_lock lock(workset_map_mutex_);

    // 插入 Map
    // 如果 ID 已存在（极罕见情况），这里会直接覆盖旧的 Workset
    s3j_worksets_[id] = std::move(workset);
    
    // 注意：如果 S3JWorkset 内部维护了更复杂的全局索引引用，
    // 在这里可能需要额外的 hook（例如更新全局路由表），
    // 但对于目前基于 "findNearestWorkset" 的动态路由机制，
    // 只要 Workset 进入了 s3j_worksets_ 容器，它就会立即被查询逻辑发现。
}


const std::deque<std::unique_ptr<VectorRecord>>&
PartitionedVectorState::getRecords(size_t /*subtask_index*/) const {
    std::shared_lock lock(merge_mutex_);

    if (view_dirty_) {
        lock.unlock();
        updateMergedView();
        lock.lock();
    }

    return merged_view_;
}

std::vector<std::shared_ptr<const VectorRecord>> 
PartitionedVectorState::getRecordsSnapshot(size_t /*subtask_index*/) const {
    std::shared_lock lock(merge_mutex_);
    
    std::vector<std::shared_ptr<const VectorRecord>> snapshot;
    
    // 计算总大小
    size_t total_size = 0;
    for (size_t i = 0; i < num_partitions_; ++i) {
        total_size += partitions_[i]->size(0);
    }
    snapshot.reserve(total_size);
    
    // 收集所有分区的记录
    for (size_t i = 0; i < num_partitions_; ++i) {
        auto all_records = partitions_[i]->getAllRecords(0);
        for (const auto* record : all_records) {
            if (record) {
                snapshot.push_back(std::make_shared<const VectorRecord>(*record));
            }
        }
    }
    
    return snapshot;
}

bool PartitionedVectorState::containsUid(uint64_t uid, size_t /*subtask_index*/) const {
    std::shared_lock lock(uid_map_mutex_);
    return uid_partition_map_.find(uid) != uid_partition_map_.end();
}

std::unordered_set<uint64_t> PartitionedVectorState::getUidSet(size_t /*subtask_index*/) const {
    std::shared_lock lock(uid_map_mutex_);
    std::unordered_set<uint64_t> uid_set;
    uid_set.reserve(uid_partition_map_.size());
    for (const auto& [uid, _] : uid_partition_map_) {
        uid_set.insert(uid);
    }
    return uid_set;
}

void PartitionedVectorState::evictExpired(int64_t current_timestamp,
                                          int64_t window_size,
                                          size_t /*subtask_index*/) {
    std::vector<uint64_t> all_evicted_uids;

    // 遍历所有分区进行过期清理
    for (size_t partition_id = 0; partition_id < num_partitions_; ++partition_id) {
        // 获取驱逐前的记录
        auto records_before = partitions_[partition_id]->getAllRecords(0);
        std::unordered_set<uint64_t> uids_before;
        for (const auto* rec : records_before) {
            uids_before.insert(rec->uid_);
        }

        // 执行过期清理（TwoTierWindowState 会记录过期 UID）
        partitions_[partition_id]->evictExpired(current_timestamp, window_size, 0);

        // 获取驱逐后的记录
        auto records_after = partitions_[partition_id]->getAllRecords(0);
        std::unordered_set<uint64_t> uids_after;
        for (const auto* rec : records_after) {
            uids_after.insert(rec->uid_);
        }

        // 找出被驱逐的 UID
        for (uint64_t uid : uids_before) {
            if (uids_after.find(uid) == uids_after.end()) {
                all_evicted_uids.push_back(uid);
            }
        }
    }

    if (all_evicted_uids.empty()) {
        return;
    }

    // 将过期 UID 添加到全局 expired_uids_ 集合
    {
        std::unique_lock lock(expired_mutex_);
        for (uint64_t uid : all_evicted_uids) {
            expired_uids_.insert(uid);
        }
    }

    // 更新 uid_partition_map_
    {
        std::unique_lock lock(uid_map_mutex_);
        for (uint64_t uid : all_evicted_uids) {
            uid_partition_map_.erase(uid);
        }
    }

    // 更新 uid_record_map_
    {
        std::unique_lock lock(record_map_mutex_);
        for (uint64_t uid : all_evicted_uids) {
            uid_record_map_.erase(uid);
        }
    }

    // 如果启用边界追踪，从 boundary_tracker_ 中移除
    if (enable_boundary_tracking_ && boundary_tracker_) {
        boundary_tracker_->unmarkBatch(all_evicted_uids);
    }

    // 标记视图需要更新
    {
        std::unique_lock lock(merge_mutex_);
        view_dirty_ = true;
    }

    SAGEFLOW_LOG_DEBUG("PartitionedVectorState",
        "Evicted {} records across all partitions", all_evicted_uids.size());
}

bool PartitionedVectorState::isExpired(uint64_t uid, size_t /*subtask_index*/) const {
    std::shared_lock lock(expired_mutex_);
    return expired_uids_.count(uid) > 0;
}

size_t PartitionedVectorState::getExpiredCount(size_t /*subtask_index*/) const {
    std::shared_lock lock(expired_mutex_);
    return expired_uids_.size();
}

std::vector<uint64_t> PartitionedVectorState::flushExpiredUids(size_t /*subtask_index*/) {
    std::unique_lock lock(expired_mutex_);
    std::vector<uint64_t> result(expired_uids_.begin(), expired_uids_.end());
    expired_uids_.clear();
    return result;
}

size_t PartitionedVectorState::size(size_t /*subtask_index*/) const {
    return totalSize();
}

std::vector<const VectorRecord*> PartitionedVectorState::getRecordsForQuery(
    const VectorRecord& query, size_t num_probes) const {

    // 使用分区器获取候选分区
    std::vector<size_t> candidate_partitions =
        partitioner_->getCandidatePartitions(query, num_partitions_, num_probes);

    std::vector<const VectorRecord*> result;
    std::unordered_set<uint64_t> included_uids;

    // 收集所有候选分区的记录
    for (size_t partition_id : candidate_partitions) {
        if (partition_id >= num_partitions_) {
            continue;
        }

        auto records = partitions_[partition_id]->getAllRecords(0);
        for (const auto* rec : records) {
            if (included_uids.find(rec->uid_) == included_uids.end()) {
                result.push_back(rec);
                included_uids.insert(rec->uid_);
            }
        }
    }

    // 如果启用边界追踪，额外包含相邻分区的边界向量
    if (enable_boundary_tracking_ && boundary_tracker_) {
        // 获取查询所属分区的邻近分区
        for (size_t partition_id : candidate_partitions) {
            if (partition_id >= num_partitions_) {
                continue;
            }

            // 获取该分区的边界向量
            auto boundary_uids = boundary_tracker_->getBoundaryVectorsForPartition(partition_id);

            // 还需要检查其他分区的边界向量是否相邻
            for (size_t other_pid = 0; other_pid < num_partitions_; ++other_pid) {
                if (std::find(candidate_partitions.begin(), candidate_partitions.end(),
                              other_pid) != candidate_partitions.end()) {
                    continue;  // 已经包含的分区跳过
                }

                auto other_boundary_uids =
                    boundary_tracker_->getBoundaryVectorsForPartition(other_pid);

                for (uint64_t uid : other_boundary_uids) {
                    if (included_uids.find(uid) == included_uids.end()) {
                        // 查找记录
                        const VectorRecord* rec = findRecordByUid(uid);
                        if (rec != nullptr) {
                            result.push_back(rec);
                            included_uids.insert(uid);
                        }
                    }
                }
            }
        }
    }

    return result;
}

std::vector<const VectorRecord*> PartitionedVectorState::getRecordsForPartition(
    size_t partition_id) const {

    if (partition_id >= num_partitions_) {
        SAGEFLOW_LOG_WARN("PartitionedVectorState",
            "getRecordsForPartition: invalid partition_id={}, num_partitions={}",
            partition_id, num_partitions_);
        return {};
    }

    return partitions_[partition_id]->getAllRecords(0);
}

std::vector<uint64_t> PartitionedVectorState::getBoundaryVectors(size_t partition_id) const {
    if (!enable_boundary_tracking_ || !boundary_tracker_) {
        return {};
    }

    if (partition_id >= num_partitions_) {
        return {};
    }

    return boundary_tracker_->getBoundaryVectorsForPartition(partition_id);
}

std::vector<size_t> PartitionedVectorState::getPartitionSizes() const {
    std::vector<size_t> sizes;
    sizes.reserve(num_partitions_);

    for (size_t i = 0; i < num_partitions_; ++i) {
        sizes.push_back(partitions_[i]->size(0));
    }

    return sizes;
}

size_t PartitionedVectorState::totalSize() const {
    size_t total = 0;
    for (size_t i = 0; i < num_partitions_; ++i) {
        total += partitions_[i]->size(0);
    }
    return total;
}

void PartitionedVectorState::compactAllPartitions() {
    for (size_t i = 0; i < num_partitions_; ++i) {
        partitions_[i]->compactTiers(0);
    }

    SAGEFLOW_LOG_DEBUG("PartitionedVectorState",
        "Compacted all {} partitions", num_partitions_);
}

int64_t PartitionedVectorState::getPartitionForUid(uint64_t uid) const {
    std::shared_lock lock(uid_map_mutex_);

    auto it = uid_partition_map_.find(uid);
    if (it == uid_partition_map_.end()) {
        return -1;
    }

    return static_cast<int64_t>(it->second);
}

const VectorRecord* PartitionedVectorState::findRecordByUid(uint64_t uid) const {
    // 首先尝试从缓存中查找
    {
        std::shared_lock lock(record_map_mutex_);
        auto it = uid_record_map_.find(uid);
        if (it != uid_record_map_.end()) {
            return it->second;
        }
    }

    // 如果缓存中没有，尝试从分区中查找
    int64_t partition_id = getPartitionForUid(uid);
    if (partition_id < 0) {
        return nullptr;
    }

    auto records = partitions_[static_cast<size_t>(partition_id)]->getAllRecords(0);
    for (const auto* rec : records) {
        if (rec->uid_ == uid) {
            // 更新缓存 - 使用 insert_or_assign 替代 operator[] 以支持 const 方法
            {
                std::unique_lock lock(record_map_mutex_);
                uid_record_map_.insert_or_assign(uid, rec);
            }
            return rec;
        }
    }

    return nullptr;
}

size_t PartitionedVectorState::getPartitionId(const VectorRecord& record) const {
    return partitioner_->partition(record, num_partitions_);
}

void PartitionedVectorState::updateBoundaryTracking(const VectorRecord& record,
                                                     size_t partition_id) {
    if (!enable_boundary_tracking_ || !boundary_tracker_) {
        return;
    }

    // 使用分区器判断是否为边界向量
    if (partitioner_->isBoundaryVector(record, num_partitions_)) {
        boundary_tracker_->markAsBoundary(record.uid_, partition_id);

        SAGEFLOW_LOG_DEBUG("PartitionedVectorState",
            "Marked vector uid={} as boundary in partition {}",
            record.uid_, partition_id);
    }
}

void PartitionedVectorState::updateMergedView() const {
    std::unique_lock lock(merge_mutex_);

    // 双重检查
    if (!view_dirty_) {
        return;
    }

    // 清空旧视图
    merged_view_.clear();

    // 合并所有分区的记录
    for (size_t i = 0; i < num_partitions_; ++i) {
        auto records = partitions_[i]->getAllRecords(0);
        for (const auto* rec : records) {
            // 创建记录的深拷贝
            auto copy = std::make_unique<VectorRecord>(
                rec->uid_,
                rec->timestamp_,
                rec->data_
            );
            merged_view_.push_back(std::move(copy));
        }
    }

    // 按时间戳排序
    std::sort(merged_view_.begin(), merged_view_.end(),
              [](const std::unique_ptr<VectorRecord>& a,
                 const std::unique_ptr<VectorRecord>& b) {
                  return a->timestamp_ < b->timestamp_;
              });

    view_dirty_ = false;

    SAGEFLOW_LOG_DEBUG("PartitionedVectorState",
        "Updated merged view with {} records", merged_view_.size());
}

std::vector<uint64_t> PartitionedVectorState::collectEvictedUids(
    size_t /*partition_id*/,
    size_t /*before_size*/,
    size_t /*after_size*/) const {
    // 该方法已被弃用，保留以备将来使用
    return {};
}

// S3J Adaptive Components Implementation

void PartitionedVectorState::createWorkset(uint64_t workset_id, std::unique_ptr<VectorRecord> centroid) {
    std::unique_lock lock(workset_map_mutex_);
    
    if (s3j_worksets_.find(workset_id) != s3j_worksets_.end()) {
        return; 
    }

    auto workset = std::make_unique<S3JWorkset>(workset_id, std::move(centroid), compact_threshold_);
    
    // 存入 Map
    s3j_worksets_[workset_id] = std::move(workset);

    SAGEFLOW_LOG_DEBUG("S3J", "Created new workset ID={} at centroid", workset_id);
}

S3JWorkset* PartitionedVectorState::getWorkset(uint64_t workset_id) {
    std::shared_lock lock(workset_map_mutex_);
    
    auto it = s3j_worksets_.find(workset_id);
    if (it != s3j_worksets_.end()) {
        return it->second.get();
    }
    return nullptr;
}

std::pair<S3JWorkset*, float> PartitionedVectorState::findNearestWorkset(const VectorRecord& record) {
    // [Optimization] Snapshot Read: 持锁仅用于复制指针，最小化临界区
    std::vector<S3JWorkset*> snapshot;
    {
        std::shared_lock lock(workset_map_mutex_);
        snapshot.reserve(s3j_worksets_.size());
        for (const auto& [id, workset] : s3j_worksets_) {
            if (workset && workset->centroid) {
                snapshot.push_back(workset.get());
            }
        }
    } // 锁在此处释放

    S3JWorkset* nearest = nullptr;
    float min_dist = std::numeric_limits<float>::max();
    
    // 准备查询向量的原始指针
    const float* rec_ptr = reinterpret_cast<const float*>(record.data_.data_.get());
    size_t dim = record.data_.dim_;
    
    if (!rec_ptr || dim == 0) {
        return {nullptr, min_dist};
    }

    // 无锁遍历快照进行计算
    for (S3JWorkset* workset : snapshot) {
        //  使用高性能 SIMD 库计算距离
        const float* cen_ptr = reinterpret_cast<const float*>(workset->centroid->data_.data_.get());
        if (!cen_ptr) continue;
        
        // 调用 SIMDDistance::l2Distance
        float dist = SIMDDistance::l2Distance(rec_ptr, cen_ptr, dim);
        
        if (dist < min_dist) {
            min_dist = dist;
            nearest = workset;
        }
    }
    
    return {nearest, min_dist};
}

std::vector<S3JWorkset*> PartitionedVectorState::getWorksetsSnapshot() const {
    std::shared_lock lock(workset_map_mutex_);
    
    std::vector<S3JWorkset*> snapshot;
    snapshot.reserve(s3j_worksets_.size());
    
    for (const auto& [id, workset_ptr] : s3j_worksets_) {
        if (workset_ptr) {
            snapshot.push_back(workset_ptr.get());
        }
    }
    
    return snapshot;
}

// ==================== 时间戳追踪接口实现 ====================

void PartitionedVectorState::updateMaxSeenTimestamp(int64_t timestamp, size_t /*subtask_index*/) {
    // PartitionedVectorState 使用全局时间戳（跨所有分区）
    int64_t current_max = max_seen_timestamp_.load(std::memory_order_relaxed);
    while (timestamp > current_max && 
           !max_seen_timestamp_.compare_exchange_weak(
               current_max, timestamp,
               std::memory_order_release,
               std::memory_order_relaxed)) {
        // 重试直到成功或发现更大的值
    }
}

int64_t PartitionedVectorState::getMaxSeenTimestamp(size_t /*subtask_index*/) const {
    return max_seen_timestamp_.load(std::memory_order_acquire);
}

int64_t PartitionedVectorState::getSafeEvictTimestamp(size_t /*subtask_index*/, 
                                                       const WindowState* other_state) const {
    // PartitionedVectorState 作为整体使用，取 min(this_max, other_max)
    constexpr int64_t kMinTimestamp = std::numeric_limits<int64_t>::min();
    
    int64_t this_max = max_seen_timestamp_.load(std::memory_order_acquire);
    
    if (!other_state) {
        return this_max;
    }
    
    int64_t other_max = other_state->getMaxSeenTimestamp(0);
    
    // 处理初始状态
    if (this_max == kMinTimestamp && other_max == kMinTimestamp) {
        return kMinTimestamp;
    } else if (this_max == kMinTimestamp) {
        return other_max;
    } else if (other_max == kMinTimestamp) {
        return this_max;
    } else {
        return std::min(this_max, other_max);
    }
}

} // namespace sageFlow

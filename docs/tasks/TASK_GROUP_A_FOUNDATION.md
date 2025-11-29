# Group A: 独立基础任务

本文档包含所有无依赖的基础组件任务，可完全并行开发。

---

## A-01: TwoTierWindowState 双层窗口状态

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: 无  
**输出文件**:
- `include/state/two_tier_window_state.h`
- `src/state/two_tier_window_state.cpp`
- `test/UnitTest/test_two_tier_window_state.cpp`

### 任务描述

实现双层窗口数据结构，将窗口分为写友好层（Write-Friendly Tier）和紧凑层（Compact Tier），优化高频插入和相似性查询的性能。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 TwoTierWindowState 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase (如 TwoTierWindowState)
- 方法名: camelBack (如 addRecord, compactTiers)
- 成员变量: lower_case_ 带尾部下划线 (如 write_tier_, compact_threshold_)
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
当前 WindowState 使用单层 deque 存储窗口记录，在高频插入场景下存在以下问题：
1. 插入和查询共享同一数据结构，存在锁竞争
2. 无法针对插入和查询分别优化数据布局

## 任务目标
实现双层窗口结构：
- **Write-Friendly Tier (write_tier_)**: 使用 deque，快速吸收新插入
- **Compact Tier (compact_tier_)**: 使用 vector，按时间戳排序，优化查询

## 文件位置
- 头文件: include/state/two_tier_window_state.h
- 实现文件: src/state/two_tier_window_state.cpp

## 接口要求
继承现有 WindowState 接口：

```cpp
#pragma once

#include "state/window_state.h"
#include <deque>
#include <vector>
#include <shared_mutex>

namespace sageFlow {

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

    void addRecord(std::unique_ptr<VectorRecord> record, size_t subtask_index) override;
    const std::deque<std::unique_ptr<VectorRecord>>& getRecords(size_t subtask_index) const override;
    void evictExpired(int64_t current_timestamp, int64_t window_size, size_t subtask_index) override;
    size_t size(size_t subtask_index) const override;
    bool isShared() const override { return false; }

    // 新增方法
    
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
    const std::vector<std::unique_ptr<VectorRecord>>& getCompactRecords(size_t subtask_index) const;
    
    /**
     * @brief 获取所有记录（写层+紧凑层合并视图）
     * @param subtask_index 子任务索引
     * @return 所有记录的向量
     */
    std::vector<const VectorRecord*> getAllRecords(size_t subtask_index) const;

private:
    struct TierPair {
        std::deque<std::unique_ptr<VectorRecord>> write_tier_;
        std::vector<std::unique_ptr<VectorRecord>> compact_tier_;
        mutable std::shared_mutex mutex_;
        
        // 用于 getRecords() 返回的临时合并视图
        mutable std::deque<std::unique_ptr<VectorRecord>> merged_view_;
        mutable bool view_dirty_ = true;
    };
    
    std::vector<TierPair> partitions_;
    size_t compact_threshold_;
    size_t merge_batch_size_;
    
    // 检查是否需要压缩
    bool needsCompaction(size_t subtask_index) const;
    
    // 更新合并视图
    void updateMergedView(size_t subtask_index) const;
};

} // namespace sageFlow
```

## 实现要点

1. **addRecord()**: 
   - 插入 write_tier_
   - 标记 view_dirty_ = true
   - 检查是否触发 compactTiers()

2. **compactTiers()**: 
   - 将 write_tier_ 中时间戳较早的记录（前 merge_batch_size_ 个）迁移到 compact_tier_
   - 保持 compact_tier_ 按时间戳排序
   - 使用 std::move 避免拷贝

3. **evictExpired()**: 
   - 同时清理两层的过期记录
   - compact_tier_ 从尾部（旧记录端）删除
   - write_tier_ 从头部删除

4. **getRecords()**: 
   - 由于接口要求返回 deque 引用，需要维护 merged_view_
   - 仅在 view_dirty_ 时更新

5. **线程安全**:
   - 使用 shared_mutex 实现读写分离
   - 写操作（addRecord, evictExpired, compactTiers）使用 unique_lock
   - 读操作（getRecords, size）使用 shared_lock

## 参考文件
- include/state/window_state.h (接口定义)
- include/state/partitioned_window_state.h (类似实现)
- src/state/partitioned_window_state.cpp

## 测试要求
在 test/UnitTest/test_two_tier_window_state.cpp 中添加测试：

```cpp
#include <gtest/gtest.h>
#include "state/two_tier_window_state.h"

class TwoTierWindowStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        state_ = std::make_unique<TwoTierWindowState>(4, 10, 5);
    }
    std::unique_ptr<TwoTierWindowState> state_;
};

// 基础功能测试
TEST_F(TwoTierWindowStateTest, AddRecordToWriteTier) { ... }
TEST_F(TwoTierWindowStateTest, GetRecordsReturnsAllRecords) { ... }
TEST_F(TwoTierWindowStateTest, EvictExpiredFromBothTiers) { ... }
TEST_F(TwoTierWindowStateTest, SizeReturnsTotal) { ... }

// 压缩触发测试
TEST_F(TwoTierWindowStateTest, CompactTriggeredWhenThresholdReached) { ... }
TEST_F(TwoTierWindowStateTest, CompactMovesOldRecordsToCompactTier) { ... }
TEST_F(TwoTierWindowStateTest, CompactMaintainsTimestampOrder) { ... }

// 并发测试
TEST_F(TwoTierWindowStateTest, ConcurrentAddRecords) { ... }
TEST_F(TwoTierWindowStateTest, ConcurrentReadAndWrite) { ... }

// 边界条件测试
TEST_F(TwoTierWindowStateTest, EmptyState) { ... }
TEST_F(TwoTierWindowStateTest, AllRecordsExpired) { ... }
```

## 验收标准
1. 所有单元测试通过
2. 代码通过 clang-tidy 检查
3. 性能测试显示高频插入场景下优于单层结构
```

---

## A-02: LSHPartitioner 局部敏感哈希分区器

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: 无  
**输出文件**:
- `include/execution/vector_space_partitioner.h`
- `src/execution/vector_space_partitioner.cpp`
- `test/UnitTest/test_vector_space_partitioner.cpp`

### 任务描述

实现基于局部敏感哈希（LSH）的向量空间分区器，确保相似向量大概率被分配到同一分区。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 LSHPartitioner 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护

## 背景
当前 VectorHashPartitioner 仅使用向量前8维的简单哈希，无法保证相似向量的局部性。
VSJoin 需要基于向量空间的分区策略，使相似向量大概率分配到同一分区。

## 任务目标
实现基于随机投影的 LSH 分区器：
1. 使用多个随机超平面将向量空间划分
2. 相似向量具有高概率获得相同的哈希码
3. 支持查询时返回候选分区列表

## 文件位置
- 头文件: include/execution/vector_space_partitioner.h
- 实现文件: src/execution/vector_space_partitioner.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <vector>
#include <random>
#include <cstdint>

namespace sageFlow {

/**
 * @brief 向量空间分区器基类
 */
class VectorSpacePartitioner {
public:
    virtual ~VectorSpacePartitioner() = default;
    
    /**
     * @brief 计算向量所属分区
     * @param record 向量记录
     * @param num_partitions 分区总数
     * @return 分区ID
     */
    virtual size_t partition(const VectorRecord& record, size_t num_partitions) = 0;
    
    /**
     * @brief 获取查询时需要检查的候选分区（包含邻近分区）
     * @param query 查询向量
     * @param num_partitions 分区总数
     * @param num_probes 探测数量（1=仅主分区）
     * @return 候选分区列表
     */
    virtual std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) = 0;
    
    /**
     * @brief 判断向量是否靠近分区边界
     * @param record 向量记录
     * @param num_partitions 分区总数
     * @return 是否为边界向量
     */
    virtual bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) = 0;
};

/**
 * @brief 基于局部敏感哈希的分区器
 * 
 * 使用随机超平面将向量空间划分，相似向量有高概率获得相同哈希码。
 * 适用于欧氏距离和角距离场景。
 */
class LSHPartitioner : public VectorSpacePartitioner {
public:
    /**
     * @brief 构造函数
     * @param dimension 向量维度
     * @param num_hash_functions 哈希函数数量（影响分区粒度）
     * @param seed 随机种子
     * @param boundary_threshold 边界判定阈值（与超平面距离的比例）
     */
    LSHPartitioner(int dimension, int num_hash_functions = 8, 
                   int seed = 42, double boundary_threshold = 0.1);
    
    size_t partition(const VectorRecord& record, size_t num_partitions) override;
    
    std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) override;
    
    bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;
    
    /**
     * @brief 获取向量的原始 LSH 哈希码（用于调试）
     */
    uint64_t getHashCode(const VectorRecord& record) const;

private:
    int dimension_;
    int num_hash_functions_;
    double boundary_threshold_;
    
    // 随机投影向量 (num_hash_functions x dimension)
    std::vector<std::vector<float>> random_projections_;
    
    /**
     * @brief 计算 LSH 哈希码
     * @param record 向量记录
     * @return 二进制哈希码
     */
    uint64_t computeHashCode(const VectorRecord& record) const;
    
    /**
     * @brief 计算向量到各超平面的有符号距离
     * @param record 向量记录
     * @return 各超平面的距离（正=超平面一侧，负=另一侧）
     */
    std::vector<float> computeDistancesToHyperplanes(const VectorRecord& record) const;
    
    /**
     * @brief 初始化随机投影向量
     * @param seed 随机种子
     */
    void initRandomProjections(int seed);
};

/**
 * @brief 基于 K-Means 的分区器（备选方案）
 */
class KMeansPartitioner : public VectorSpacePartitioner {
public:
    KMeansPartitioner(int dimension, int num_clusters, int seed = 42);
    
    /**
     * @brief 使用样本数据初始化质心
     * @param samples 样本向量
     * @param max_iterations 最大迭代次数
     */
    void initCentroids(const std::vector<const VectorRecord*>& samples, 
                       int max_iterations = 100);
    
    /**
     * @brief 在线更新质心（增量 K-Means）
     * @param record 新向量
     * @param learning_rate 学习率
     */
    void updateCentroids(const VectorRecord& record, double learning_rate = 0.01);
    
    size_t partition(const VectorRecord& record, size_t num_partitions) override;
    std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) override;
    bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;

private:
    int dimension_;
    int num_clusters_;
    std::vector<std::vector<float>> centroids_;
    
    size_t findNearestCentroid(const VectorRecord& record) const;
};

} // namespace sageFlow
```

## 实现要点

1. **initRandomProjections()**:
   - 使用标准正态分布初始化 num_hash_functions 个随机向量
   - 每个向量维度为 dimension
   - 归一化为单位向量

2. **computeHashCode()**:
   - 对每个投影向量计算与输入向量的点积
   - 点积 > 0 则对应位为 1，否则为 0
   - 组合成 uint64_t 哈希码

3. **partition()**:
   - hashCode % num_partitions

4. **getCandidatePartitions()**:
   - 返回主分区
   - 如果 num_probes > 1，翻转距离超平面最近的 bit 位，获取邻近分区
   - 使用 computeDistancesToHyperplanes() 确定哪些 bit 最容易翻转

5. **isBoundaryVector()**:
   - 检查是否有任何超平面距离小于 boundary_threshold * 向量模长
   - 距离小说明向量靠近分区边界

## 参考资料
- 现有分区器: include/execution/partitioner.h
- LSH 理论: Locality-Sensitive Hashing Scheme Based on p-Stable Distributions

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "execution/vector_space_partitioner.h"

class LSHPartitionerTest : public ::testing::Test {
protected:
    void SetUp() override {
        partitioner_ = std::make_unique<LSHPartitioner>(128, 8, 42);
    }
    std::unique_ptr<LSHPartitioner> partitioner_;
};

// 一致性测试
TEST_F(LSHPartitionerTest, SameVectorSamePartition) {
    // 相同向量应该分配到相同分区
}

// 局部性测试
TEST_F(LSHPartitionerTest, SimilarVectorsSamePartitionHighProbability) {
    // 相似向量有高概率分配到同一分区（统计测试）
    // 生成100对相似向量，检查同分区比例 > 70%
}

// 候选分区测试
TEST_F(LSHPartitionerTest, CandidatePartitionsIncludesMainPartition) {
    // getCandidatePartitions 结果应包含主分区
}

TEST_F(LSHPartitionerTest, MoreProbesMeansMoreCandidates) {
    // num_probes 增加时，候选分区数应增加
}

// 边界向量测试
TEST_F(LSHPartitionerTest, BoundaryVectorDetection) {
    // 构造靠近超平面的向量，验证被标记为边界向量
}
```

## 验收标准
1. 所有单元测试通过
2. 相似向量同分区率 > 70%（在测试数据集上）
3. 代码通过 clang-tidy 检查
```

---

## A-03: BoundaryTracker 边界向量追踪器

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/coordination/boundary_tracker.h`
- `src/coordination/boundary_tracker.cpp`
- `test/UnitTest/test_boundary_tracker.cpp`

### 任务描述

实现边界向量追踪机制，标记和管理靠近分区边界的向量，用于跨分区查询时的额外检查。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 BoundaryTracker 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎。

## 背景
向量空间分区后，靠近分区边界的向量可能与其他分区的向量相似。
需要追踪这些边界向量，在跨分区 join 时进行额外检查，避免召回率损失。

## 任务目标
实现轻量级的边界向量追踪器：
1. 高效标记/取消标记边界向量
2. 快速查询向量是否为边界向量
3. 获取特定分区的所有边界向量

## 文件位置
- 头文件: include/coordination/boundary_tracker.h
- 实现文件: src/coordination/boundary_tracker.cpp

## 接口要求

```cpp
#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <shared_mutex>
#include <cstdint>

namespace sageFlow {

/**
 * @brief 边界向量追踪器
 * 
 * 追踪靠近分区边界的向量，用于跨分区查询时的额外检查。
 * 线程安全，支持高并发读取。
 */
class BoundaryTracker {
public:
    BoundaryTracker() = default;
    
    /**
     * @brief 标记向量为边界向量
     * @param vector_uid 向量唯一ID
     * @param partition_id 所属分区ID
     */
    void markAsBoundary(uint64_t vector_uid, size_t partition_id);
    
    /**
     * @brief 取消边界标记
     * @param vector_uid 向量唯一ID
     */
    void unmark(uint64_t vector_uid);
    
    /**
     * @brief 批量取消边界标记
     * @param vector_uids 向量ID列表
     */
    void unmarkBatch(const std::vector<uint64_t>& vector_uids);
    
    /**
     * @brief 检查是否为边界向量
     * @param vector_uid 向量唯一ID
     * @return 是否为边界向量
     */
    bool isBoundaryVector(uint64_t vector_uid) const;
    
    /**
     * @brief 获取特定分区的所有边界向量 UID
     * @param partition_id 分区ID
     * @return 边界向量UID列表
     */
    std::vector<uint64_t> getBoundaryVectorsForPartition(size_t partition_id) const;
    
    /**
     * @brief 获取向量所属分区（仅对边界向量有效）
     * @param vector_uid 向量唯一ID
     * @return 分区ID，如果不是边界向量返回 -1
     */
    int64_t getPartition(uint64_t vector_uid) const;
    
    /**
     * @brief 获取边界向量总数
     */
    size_t size() const;
    
    /**
     * @brief 获取各分区边界向量数量
     */
    std::unordered_map<size_t, size_t> getPartitionStats() const;
    
    /**
     * @brief 清空所有记录
     */
    void clear();

private:
    // uid -> partition_id
    std::unordered_map<uint64_t, size_t> boundary_vectors_;
    
    // partition_id -> set of uids (用于快速获取分区边界向量)
    std::unordered_map<size_t, std::unordered_set<uint64_t>> partition_boundaries_;
    
    mutable std::shared_mutex mutex_;
};

} // namespace sageFlow
```

## 实现要点

1. **markAsBoundary()**:
   - 获取 unique_lock
   - 更新 boundary_vectors_[uid] = partition_id
   - 更新 partition_boundaries_[partition_id].insert(uid)

2. **unmark()**:
   - 获取 unique_lock
   - 查找 uid 对应的 partition_id
   - 从两个索引中移除

3. **getBoundaryVectorsForPartition()**:
   - 获取 shared_lock
   - 返回副本以避免锁持有时间过长

4. **线程安全**:
   - 使用 shared_mutex 支持读多写少场景
   - 所有读操作使用 shared_lock
   - 所有写操作使用 unique_lock

## 测试要求

```cpp
TEST(BoundaryTrackerTest, MarkAndCheck) {
    BoundaryTracker tracker;
    tracker.markAsBoundary(100, 0);
    EXPECT_TRUE(tracker.isBoundaryVector(100));
    EXPECT_FALSE(tracker.isBoundaryVector(200));
}

TEST(BoundaryTrackerTest, UnmarkRemovesBoundary) {
    BoundaryTracker tracker;
    tracker.markAsBoundary(100, 0);
    tracker.unmark(100);
    EXPECT_FALSE(tracker.isBoundaryVector(100));
}

TEST(BoundaryTrackerTest, GetBoundaryVectorsForPartition) {
    BoundaryTracker tracker;
    tracker.markAsBoundary(100, 0);
    tracker.markAsBoundary(101, 0);
    tracker.markAsBoundary(200, 1);
    
    auto partition0 = tracker.getBoundaryVectorsForPartition(0);
    EXPECT_EQ(partition0.size(), 2);
}

TEST(BoundaryTrackerTest, ConcurrentAccess) {
    // 多线程并发读写测试
}
```

## 验收标准
1. 所有单元测试通过
2. 并发测试无死锁和数据竞争
3. 代码通过 clang-tidy 检查
```

---

## A-04: LateArrivalHandler 延迟到达处理器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: 无  
**输出文件**:
- `include/coordination/late_arrival_handler.h`
- `src/coordination/late_arrival_handler.cpp`
- `test/UnitTest/test_late_arrival_handler.cpp`

### 任务描述

实现延迟到达向量的处理机制，支持乱序数据流和 watermark 语义。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 LateArrivalHandler 类。

## 背景
流式系统中，数据可能乱序到达。当前系统假设数据按时间戳顺序到达，
无法正确处理延迟到达的向量，可能导致 join 结果不完整。

## 任务目标
实现延迟到达处理器：
1. 维护 watermark（水位线），追踪已处理数据的时间进度
2. 识别延迟到达的记录
3. 缓冲延迟记录，定期与主窗口进行补充 join

## 文件位置
- 头文件: include/coordination/late_arrival_handler.h
- 实现文件: src/coordination/late_arrival_handler.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <deque>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <cstdint>

namespace sageFlow {

/**
 * @brief 记录到达状态
 */
enum class ArrivalStatus {
    ON_TIME,      ///< 正常到达（时间戳 >= watermark）
    LATE,         ///< 延迟但可处理（时间戳在允许延迟范围内）
    TOO_LATE      ///< 超出允许延迟，应丢弃
};

/**
 * @brief 延迟到达处理器
 * 
 * 实现 watermark 机制，处理乱序数据流。
 * 参考 Apache Flink 的 watermark 语义。
 */
class LateArrivalHandler {
public:
    /**
     * @brief 构造函数
     * @param allowed_lateness 允许的最大延迟时间（毫秒）
     * @param watermark_delay watermark 滞后于最新记录的时间（毫秒）
     */
    explicit LateArrivalHandler(int64_t allowed_lateness = 5000,
                                int64_t watermark_delay = 1000);
    
    /**
     * @brief 处理到达的记录，返回状态
     * @param record 到达的记录
     * @return 到达状态
     */
    ArrivalStatus processRecord(const VectorRecord& record);
    
    /**
     * @brief 更新 watermark
     * @param event_time 事件时间戳
     */
    void updateWatermark(int64_t event_time);
    
    /**
     * @brief 获取当前 watermark
     */
    int64_t getWatermark() const;
    
    /**
     * @brief 添加延迟记录到缓冲区
     * @param record 延迟记录
     */
    void bufferLateRecord(std::unique_ptr<VectorRecord> record);
    
    /**
     * @brief 获取并清空延迟缓冲区
     * @return 缓冲的延迟记录
     */
    std::vector<std::unique_ptr<VectorRecord>> flushLateBuffer();
    
    /**
     * @brief 获取延迟缓冲区大小
     */
    size_t getLateBufferSize() const;
    
    /**
     * @brief 统计信息
     */
    struct Stats {
        std::atomic<uint64_t> on_time_count{0};
        std::atomic<uint64_t> late_count{0};
        std::atomic<uint64_t> too_late_count{0};
    };
    
    /**
     * @brief 获取统计信息
     */
    const Stats& getStats() const { return stats_; }
    
    /**
     * @brief 重置统计信息
     */
    void resetStats();

private:
    std::atomic<int64_t> watermark_{0};
    int64_t allowed_lateness_;
    int64_t watermark_delay_;
    std::atomic<int64_t> max_seen_timestamp_{0};
    
    std::deque<std::unique_ptr<VectorRecord>> late_buffer_;
    mutable std::shared_mutex buffer_mutex_;
    
    Stats stats_;
};

} // namespace sageFlow
```

## 实现要点

1. **processRecord()**:
   ```cpp
   ArrivalStatus processRecord(const VectorRecord& record) {
       int64_t event_time = record.getTimestamp();
       
       // 更新最大观察时间戳
       int64_t expected = max_seen_timestamp_.load();
       while (event_time > expected && 
              !max_seen_timestamp_.compare_exchange_weak(expected, event_time)) {}
       
       // 更新 watermark
       updateWatermark(max_seen_timestamp_.load());
       
       int64_t current_watermark = watermark_.load();
       
       if (event_time >= current_watermark) {
           stats_.on_time_count++;
           return ArrivalStatus::ON_TIME;
       } else if (event_time >= current_watermark - allowed_lateness_) {
           stats_.late_count++;
           return ArrivalStatus::LATE;
       } else {
           stats_.too_late_count++;
           return ArrivalStatus::TOO_LATE;
       }
   }
   ```

2. **updateWatermark()**:
   ```cpp
   void updateWatermark(int64_t event_time) {
       int64_t new_watermark = event_time - watermark_delay_;
       int64_t expected = watermark_.load();
       while (new_watermark > expected && 
              !watermark_.compare_exchange_weak(expected, new_watermark)) {}
   }
   ```

3. **flushLateBuffer()**:
   - 获取 unique_lock
   - 返回所有缓冲记录，使用 std::move
   - 清空缓冲区

## 测试要求

```cpp
TEST(LateArrivalHandlerTest, OnTimeRecord) {
    LateArrivalHandler handler(5000, 1000);
    // 模拟正常到达的记录
}

TEST(LateArrivalHandlerTest, LateRecord) {
    LateArrivalHandler handler(5000, 1000);
    // 模拟延迟但在允许范围内的记录
}

TEST(LateArrivalHandlerTest, TooLateRecord) {
    LateArrivalHandler handler(5000, 1000);
    // 模拟超出允许延迟的记录
}

TEST(LateArrivalHandlerTest, WatermarkProgression) {
    // 测试 watermark 正确递增
}

TEST(LateArrivalHandlerTest, FlushLateBuffer) {
    // 测试缓冲区 flush 正确性
}
```

## 验收标准
1. 所有单元测试通过
2. watermark 语义正确
3. 线程安全
```

---

## A-05: DistanceVerifier 距离验证器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: 无  
**输出文件**:
- `include/operator/distance_verifier.h`
- `src/operator/distance_verifier.cpp`
- `test/UnitTest/test_distance_verifier.cpp`

### 任务描述

实现高效的距离验证器，用于验证候选向量是否满足相似度阈值，支持 SIMD 加速和早期终止优化。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 DistanceVerifier 类。

## 背景
当前 JoinOperator 中的候选验证与候选生成耦合在一起。
将验证逻辑独立出来，可以：
1. 支持 SIMD 批量验证
2. 实现早期终止优化
3. 方便并行验证

## 任务目标
实现距离验证器：
1. 批量验证候选向量
2. 使用 SIMD 加速距离计算（可选）
3. 支持早期终止（部分维度快速筛选）

## 文件位置
- 头文件: include/operator/distance_verifier.h
- 实现文件: src/operator/distance_verifier.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <vector>
#include <memory>

namespace sageFlow {

/**
 * @brief 验证结果
 */
struct VerificationResult {
    uint64_t candidate_uid;
    double distance;
    double similarity;
    bool passed;
};

/**
 * @brief 距离验证器
 * 
 * 验证候选向量是否满足相似度阈值。
 * 支持批量验证和早期终止优化。
 */
class DistanceVerifier {
public:
    /**
     * @brief 构造函数
     * @param similarity_threshold 相似度阈值 (similarity >= threshold 才通过)
     * @param alpha 距离到相似度的转换系数 (similarity = exp(-alpha * distance))
     */
    explicit DistanceVerifier(double similarity_threshold, double alpha = 0.1);
    
    /**
     * @brief 验证单个候选
     * @param query 查询向量
     * @param candidate 候选向量
     * @return 验证结果
     */
    VerificationResult verify(const VectorRecord& query, const VectorRecord& candidate);
    
    /**
     * @brief 批量验证
     * @param query 查询向量
     * @param candidates 候选向量列表
     * @return 所有验证结果
     */
    std::vector<VerificationResult> verifyBatch(
        const VectorRecord& query,
        const std::vector<std::unique_ptr<VectorRecord>>& candidates);
    
    /**
     * @brief 批量验证（只返回通过的）
     * @param query 查询向量
     * @param candidates 候选向量列表（会被移动）
     * @return 通过验证的候选
     */
    std::vector<std::unique_ptr<VectorRecord>> filterCandidates(
        const VectorRecord& query,
        std::vector<std::unique_ptr<VectorRecord>>&& candidates);
    
    /**
     * @brief 设置早期终止的维度检查数
     * @param dims 0 表示不使用早期终止
     */
    void setEarlyTerminationDims(int dims) { early_termination_dims_ = dims; }
    
    /**
     * @brief 获取相似度阈值
     */
    double getThreshold() const { return similarity_threshold_; }
    
    /**
     * @brief 将距离转换为相似度
     */
    double distanceToSimilarity(double distance) const {
        return std::exp(-alpha_ * distance);
    }
    
    /**
     * @brief 将相似度转换为距离阈值
     */
    double similarityToDistance(double similarity) const {
        return -std::log(similarity) / alpha_;
    }

private:
    double similarity_threshold_;
    double alpha_;
    int early_termination_dims_ = 0;  // 0 表示不使用早期终止
    double distance_threshold_;  // 预计算的距离阈值
    
    /**
     * @brief 计算 L2 距离
     */
    double computeL2Distance(const VectorRecord& a, const VectorRecord& b) const;
    
    /**
     * @brief 早期终止检查：使用前 N 维估计距离下界
     * @return true 表示可以安全拒绝
     */
    bool earlyReject(const VectorRecord& query, const VectorRecord& candidate) const;
};

} // namespace sageFlow
```

## 实现要点

1. **computeL2Distance()**:
   ```cpp
   double computeL2Distance(const VectorRecord& a, const VectorRecord& b) const {
       const auto& vec_a = a.getVector();
       const auto& vec_b = b.getVector();
       
       double sum = 0.0;
       for (size_t i = 0; i < vec_a.size(); ++i) {
           double diff = vec_a[i] - vec_b[i];
           sum += diff * diff;
       }
       return std::sqrt(sum);
   }
   ```

2. **earlyReject()**:
   - 只用前 early_termination_dims_ 维计算部分距离
   - 如果部分距离已超过 distance_threshold_，直接拒绝
   - 利用 L2 距离的性质：部分维度距离 <= 完整距离

3. **filterCandidates()**:
   - 先进行早期终止筛选（如果启用）
   - 对剩余候选进行完整验证
   - 返回通过验证的候选（使用 std::move）

## 测试要求

```cpp
TEST(DistanceVerifierTest, VerifySingleCandidate) {
    DistanceVerifier verifier(0.8, 0.1);
    // 测试单个候选验证
}

TEST(DistanceVerifierTest, BatchVerification) {
    // 测试批量验证正确性
}

TEST(DistanceVerifierTest, EarlyTermination) {
    // 测试早期终止不影响正确性
    // 确保不会错误拒绝满足条件的候选
}

TEST(DistanceVerifierTest, FilterCandidates) {
    // 测试过滤后只保留通过的候选
}
```

## 验收标准
1. 所有单元测试通过
2. 早期终止正确性验证
3. 批量验证结果与单个验证一致
```

---

## A-06: PCA 工具类

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/compute_engine/pca.h`
- `src/compute_engine/pca.cpp`
- `test/UnitTest/test_pca.cpp`

### 任务描述

实现 PCA（主成分分析）工具类，用于 HDR-Tree baseline 的降维操作。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 PCA 类。

## 背景
HDR-Tree baseline 需要使用 PCA 将高维向量投影到低维空间，
利用 PCA 距离下界性质进行候选过滤。

## 任务目标
实现 PCA 工具类：
1. 使用样本数据拟合 PCA
2. 投影向量到低维空间
3. 支持批量投影

## 文件位置
- 头文件: include/compute_engine/pca.h
- 实现文件: src/compute_engine/pca.cpp

## 接口要求

```cpp
#pragma once

#include <vector>
#include <cstddef>

namespace sageFlow {

/**
 * @brief 主成分分析 (PCA) 工具类
 * 
 * 使用幂迭代法计算主成分，适用于中等规模数据。
 * 对于大规模数据建议使用增量 PCA 或随机化 PCA。
 */
class PCA {
public:
    /**
     * @brief 构造函数
     * @param original_dim 原始维度
     * @param target_dim 目标维度（主成分数量）
     */
    PCA(int original_dim, int target_dim);
    
    /**
     * @brief 使用样本数据拟合 PCA
     * @param samples 样本数据 (n_samples x original_dim)
     * @param max_iterations 最大迭代次数
     * @param tolerance 收敛阈值
     */
    void fit(const std::vector<std::vector<float>>& samples,
             int max_iterations = 100, double tolerance = 1e-6);
    
    /**
     * @brief 投影单个向量到低维空间
     * @param vector 原始向量
     * @return 低维向量
     */
    std::vector<float> transform(const std::vector<float>& vector) const;
    
    /**
     * @brief 批量投影
     * @param vectors 原始向量列表
     * @return 低维向量列表
     */
    std::vector<std::vector<float>> transformBatch(
        const std::vector<std::vector<float>>& vectors) const;
    
    /**
     * @brief 检查是否已拟合
     */
    bool isFitted() const { return fitted_; }
    
    /**
     * @brief 获取解释方差比例
     */
    const std::vector<float>& getExplainedVarianceRatio() const;
    
    /**
     * @brief 获取主成分矩阵 (target_dim x original_dim)
     */
    const std::vector<std::vector<float>>& getComponents() const { return components_; }
    
    /**
     * @brief 获取数据均值
     */
    const std::vector<float>& getMean() const { return mean_; }

private:
    int original_dim_;
    int target_dim_;
    bool fitted_ = false;
    
    std::vector<float> mean_;
    std::vector<std::vector<float>> components_;  // target_dim x original_dim
    std::vector<float> explained_variance_;
    std::vector<float> explained_variance_ratio_;
    
    /**
     * @brief 计算数据均值
     */
    std::vector<float> computeMean(const std::vector<std::vector<float>>& data) const;
    
    /**
     * @brief 中心化数据
     */
    std::vector<std::vector<float>> centerData(
        const std::vector<std::vector<float>>& data,
        const std::vector<float>& mean) const;
    
    /**
     * @brief 使用幂迭代法计算主成分
     */
    void powerIteration(const std::vector<std::vector<float>>& centered_data,
                        int max_iterations, double tolerance);
};

} // namespace sageFlow
```

## 实现要点

1. **fit()**:
   - 计算数据均值
   - 中心化数据
   - 使用幂迭代法或协方差矩阵特征分解计算主成分

2. **transform()**:
   - 减去均值
   - 与主成分矩阵相乘

3. **powerIteration()**:
   - 迭代计算每个主成分
   - 每次计算后需要去除已有主成分的影响（deflation）

## 测试要求

```cpp
TEST(PCATest, FitAndTransform) {
    PCA pca(128, 32);
    // 生成测试数据并拟合
    // 验证 transform 输出维度正确
}

TEST(PCATest, DistanceLowerBound) {
    // 验证 PCA 距离下界性质
    // ||P*x - P*y|| <= ||x - y||
}

TEST(PCATest, ExplainedVariance) {
    // 验证解释方差比例合理
}
```

## 验收标准
1. 所有单元测试通过
2. PCA 距离下界性质验证通过
3. 性能可接受（1000 样本 128 维 < 1s）
```

---

## A-07: ComputeEngine SIMD 优化

**优先级**: 🟢 低  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/compute_engine/simd_distance.h`
- `src/compute_engine/simd_distance.cpp`
- `test/UnitTest/test_simd_distance.cpp`

### 任务描述

为现有 ComputeEngine 添加 SIMD 优化的距离计算函数。

### 提示词

```
你是 sageFlow 项目的开发者，需要为 ComputeEngine 添加 SIMD 优化。

## 背景
距离计算是 Join 操作的性能热点，使用 SIMD 指令可以显著提升性能。

## 任务目标
实现 SIMD 加速的距离计算：
1. L2 距离 (SSE/AVX)
2. 余弦相似度 (SSE/AVX)
3. 自动检测 CPU 支持的指令集

## 文件位置
- 头文件: include/compute_engine/simd_distance.h
- 实现文件: src/compute_engine/simd_distance.cpp

## 接口要求

```cpp
#pragma once

#include <vector>
#include <cstddef>

namespace sageFlow {

/**
 * @brief SIMD 加速的距离计算
 */
class SIMDDistance {
public:
    /**
     * @brief 检测支持的 SIMD 指令集
     */
    enum class SIMDLevel {
        NONE,   ///< 无 SIMD 支持
        SSE,    ///< SSE 支持
        AVX,    ///< AVX 支持
        AVX2,   ///< AVX2 支持
        AVX512  ///< AVX-512 支持
    };
    
    /**
     * @brief 获取当前 CPU 支持的 SIMD 级别
     */
    static SIMDLevel detectSIMDLevel();
    
    /**
     * @brief 计算 L2 距离（自动选择最优实现）
     */
    static float l2Distance(const float* a, const float* b, size_t dim);
    
    /**
     * @brief 计算 L2 距离平方（避免 sqrt）
     */
    static float l2DistanceSquared(const float* a, const float* b, size_t dim);
    
    /**
     * @brief 计算余弦相似度
     */
    static float cosineSimilarity(const float* a, const float* b, size_t dim);
    
    /**
     * @brief 批量计算 L2 距离
     * @param query 查询向量
     * @param candidates 候选向量数组
     * @param num_candidates 候选数量
     * @param dim 向量维度
     * @param results 输出距离数组
     */
    static void l2DistanceBatch(const float* query, 
                                const float* const* candidates,
                                size_t num_candidates, size_t dim,
                                float* results);

private:
    // 标量实现
    static float l2DistanceScalar(const float* a, const float* b, size_t dim);
    
    // SSE 实现
    static float l2DistanceSSE(const float* a, const float* b, size_t dim);
    
    // AVX 实现
    static float l2DistanceAVX(const float* a, const float* b, size_t dim);
};

} // namespace sageFlow
```

## 实现要点

1. **detectSIMDLevel()**:
   - 使用 __cpuid 检测 CPU 特性
   - 返回最高支持的 SIMD 级别

2. **l2DistanceAVX()**:
   ```cpp
   static float l2DistanceAVX(const float* a, const float* b, size_t dim) {
       __m256 sum = _mm256_setzero_ps();
       size_t i = 0;
       
       // 每次处理 8 个 float
       for (; i + 8 <= dim; i += 8) {
           __m256 va = _mm256_loadu_ps(a + i);
           __m256 vb = _mm256_loadu_ps(b + i);
           __m256 diff = _mm256_sub_ps(va, vb);
           sum = _mm256_fmadd_ps(diff, diff, sum);  // FMA
       }
       
       // 水平求和
       __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum, 0),
                                   _mm256_extractf128_ps(sum, 1));
       sum128 = _mm_hadd_ps(sum128, sum128);
       sum128 = _mm_hadd_ps(sum128, sum128);
       float result = _mm_cvtss_f32(sum128);
       
       // 处理剩余元素
       for (; i < dim; ++i) {
           float diff = a[i] - b[i];
           result += diff * diff;
       }
       
       return std::sqrt(result);
   }
   ```

## 测试要求

```cpp
TEST(SIMDDistanceTest, L2DistanceCorrectness) {
    // 验证 SIMD 结果与标量结果一致
}

TEST(SIMDDistanceTest, CosineSimilarityCorrectness) {
    // 验证余弦相似度计算正确
}

TEST(SIMDDistanceTest, BatchDistance) {
    // 验证批量计算正确
}

TEST(SIMDDistanceTest, Performance) {
    // 性能对比测试
}
```

## 验收标准
1. 所有单元测试通过
2. SIMD 结果与标量结果误差 < 1e-5
3. 性能提升 > 2x（在支持 AVX 的 CPU 上）
```

---

## 任务检查清单

| 任务ID | 状态 | 负责人 | 开始日期 | 完成日期 |
|--------|------|--------|----------|----------|
| A-01 | ⬜ | - | - | - |
| A-02 | ⬜ | - | - | - |
| A-03 | ⬜ | - | - | - |
| A-04 | ⬜ | - | - | - |
| A-05 | ⬜ | - | - | - |
| A-06 | ⬜ | - | - | - |
| A-07 | ⬜ | - | - | - |

# Task A-03: BoundaryTracker 边界向量追踪器

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/coordination/boundary_tracker.h`
- `src/coordination/boundary_tracker.cpp`
- `test/UnitTest/test_boundary_tracker.cpp`

---

## 任务描述

实现边界向量追踪机制，标记和管理靠近分区边界的向量，用于跨分区查询时的额外检查。

---

## 提示词

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

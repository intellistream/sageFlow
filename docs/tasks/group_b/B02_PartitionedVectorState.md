# Task B-02: PartitionedVectorState 分区向量状态

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: A-01 (TwoTierWindowState) ✅, A-02 (LSHPartitioner) ✅  
**输出文件**:
- `include/state/partitioned_vector_state.h`
- `src/state/partitioned_vector_state.cpp`
- `test/UnitTest/test_partitioned_vector_state.cpp`

---

## 任务描述

实现分区向量状态，结合双层窗口和向量空间分区，是 VSJoin 的核心状态管理类。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionedVectorState 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase (如 PartitionedVectorState)
- 方法名: camelBack (如 getRecordsForQuery, getPartitionSizes)
- 成员变量: lower_case_ 带尾部下划线 (如 partitions_, boundary_tracker_)
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
这是 VSJoin 的核心状态管理类，结合：
1. TwoTierWindowState（双层窗口）- 已实现于 include/state/two_tier_window_state.h
2. LSHPartitioner（向量空间分区）- 已实现于 include/execution/vector_space_partitioner.h
3. BoundaryTracker（边界追踪）- 已实现于 include/coordination/boundary_tracker.h

## 任务目标
实现分区向量状态：
1. 每个向量空间分区拥有独立的 TwoTierWindowState
2. 自动路由记录到正确分区
3. 支持跨分区查询
4. 边界向量追踪

## 文件位置
- 头文件: include/state/partitioned_vector_state.h
- 实现文件: src/state/partitioned_vector_state.cpp

## 接口要求

```cpp
#pragma once

#include "state/window_state.h"
#include "state/two_tier_window_state.h"
#include "execution/vector_space_partitioner.h"
#include "coordination/boundary_tracker.h"
#include <vector>
#include <memory>

namespace sageFlow {

/**
 * @brief 分区向量状态
 * 
 * 结合双层窗口和向量空间分区的状态管理。
 * 每个向量空间分区拥有独立的 TwoTierWindowState。
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
    
    // WindowState 接口实现
    void addRecord(std::unique_ptr<VectorRecord> record, size_t subtask_index) override;
    const std::deque<std::unique_ptr<VectorRecord>>& getRecords(size_t subtask_index) const override;
    void evictExpired(int64_t current_timestamp, int64_t window_size, size_t subtask_index) override;
    size_t size(size_t subtask_index) const override;
    bool isShared() const override { return false; }
    
    // 分区特定操作
    
    /**
     * @brief 获取查询相关的记录
     * @param query 查询向量
     * @param num_probes 探测分区数
     * @return 相关分区的所有记录
     */
    std::vector<const VectorRecord*> getRecordsForQuery(
        const VectorRecord& query, size_t num_probes = 2) const;
    
    /**
     * @brief 获取特定分区的记录
     * @param partition_id 分区ID
     * @return 该分区的所有记录
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
     */
    size_t getNumPartitions() const { return num_partitions_; }
    
    /**
     * @brief 获取各分区大小
     */
    std::vector<size_t> getPartitionSizes() const;
    
    /**
     * @brief 获取总记录数
     */
    size_t totalSize() const;
    
    /**
     * @brief 触发所有分区的层压缩
     */
    void compactAllPartitions();

private:
    size_t num_partitions_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    bool enable_boundary_tracking_;
    size_t compact_threshold_;
    
    // 每个向量空间分区的状态
    std::vector<std::unique_ptr<TwoTierWindowState>> partitions_;
    
    // 边界向量追踪器
    std::unique_ptr<BoundaryTracker> boundary_tracker_;
    
    // uid -> partition_id 映射
    std::unordered_map<uint64_t, size_t> uid_partition_map_;
    mutable std::shared_mutex uid_map_mutex_;
    
    // 用于 getRecords() 的合并视图
    mutable std::deque<std::unique_ptr<VectorRecord>> merged_view_;
    mutable std::shared_mutex merge_mutex_;
    mutable bool view_dirty_ = true;
    
    /**
     * @brief 确定向量所属分区
     */
    size_t getPartitionId(const VectorRecord& record) const;
    
    /**
     * @brief 更新边界向量追踪
     */
    void updateBoundaryTracking(const VectorRecord& record, size_t partition_id);
    
    /**
     * @brief 更新合并视图
     */
    void updateMergedView() const;
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 创建 num_partitions 个 TwoTierWindowState 实例
   - 每个分区的 parallelism 设为 1（分区内不再细分）
   - 如果启用边界追踪，创建 BoundaryTracker

2. **addRecord()**:
   - 使用 partitioner_->partition() 确定分区
   - 更新 uid_partition_map_
   - 将记录添加到对应分区的 TwoTierWindowState
   - 如果启用边界追踪，调用 updateBoundaryTracking()
   - 标记 view_dirty_ = true

3. **getRecordsForQuery()**:
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 收集所有候选分区的记录（使用 getAllRecords()）
   - 如果启用边界追踪，额外包含边界向量

4. **evictExpired()**:
   - 遍历所有分区进行过期清理
   - 收集被清理的 uid
   - 更新 uid_partition_map_
   - 如果启用边界追踪，从 boundary_tracker_ 中移除

5. **getRecords()**:
   - 由于接口要求返回 deque 引用，需要维护 merged_view_
   - 仅在 view_dirty_ 时调用 updateMergedView()

## 参考文件
- include/state/window_state.h (接口定义)
- include/state/two_tier_window_state.h (双层窗口)
- include/execution/vector_space_partitioner.h (分区器)
- include/coordination/boundary_tracker.h (边界追踪)

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "state/partitioned_vector_state.h"

class PartitionedVectorStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        partitioner_ = std::make_shared<LSHPartitioner>(128, 8, 42);
        state_ = std::make_unique<PartitionedVectorState>(
            4, partitioner_, 10, true);
    }
    
    std::shared_ptr<LSHPartitioner> partitioner_;
    std::unique_ptr<PartitionedVectorState> state_;
    
    std::unique_ptr<VectorRecord> createRandomRecord(uint64_t uid, int64_t ts);
};

// 基础功能测试
TEST_F(PartitionedVectorStateTest, RecordRouting) {
    // 测试记录路由到正确分区
}

TEST_F(PartitionedVectorStateTest, AddAndRetrieve) {
    // 测试添加后能正确获取
}

TEST_F(PartitionedVectorStateTest, GetRecordsForQuery) {
    // 测试查询相关记录的覆盖率
}

TEST_F(PartitionedVectorStateTest, GetRecordsForPartition) {
    // 测试获取指定分区记录
}

// 边界向量测试
TEST_F(PartitionedVectorStateTest, BoundaryVectorTracking) {
    // 测试边界向量正确追踪
}

TEST_F(PartitionedVectorStateTest, BoundaryVectorInQueryResults) {
    // 测试边界向量包含在查询结果中
}

// 过期清理测试
TEST_F(PartitionedVectorStateTest, EvictExpiredAcrossPartitions) {
    // 测试过期清理跨分区一致性
}

TEST_F(PartitionedVectorStateTest, EvictUpdatesUidMap) {
    // 测试过期清理更新 uid 映射
}

TEST_F(PartitionedVectorStateTest, EvictUpdatesBoundaryTracker) {
    // 测试过期清理更新边界追踪
}

// 压缩测试
TEST_F(PartitionedVectorStateTest, CompactAllPartitions) {
    // 测试触发所有分区压缩
}

// 统计测试
TEST_F(PartitionedVectorStateTest, PartitionSizes) {
    // 测试分区大小统计
}

TEST_F(PartitionedVectorStateTest, TotalSize) {
    // 测试总大小统计
}

// 并发测试
TEST_F(PartitionedVectorStateTest, ConcurrentAddAndQuery) {
    // 测试并发添加和查询
}
```

## 验收标准
1. 所有单元测试通过
2. 查询覆盖率 > 95%
3. 边界向量追踪正确
4. 代码通过 clang-tidy 检查
```

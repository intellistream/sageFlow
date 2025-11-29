# Task A-01: TwoTierWindowState 双层窗口状态

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: 无  
**输出文件**:
- `include/state/two_tier_window_state.h`
- `src/state/two_tier_window_state.cpp`
- `test/UnitTest/test_two_tier_window_state.cpp`

---

## 任务描述

实现双层窗口数据结构，将窗口分为写友好层（Write-Friendly Tier）和紧凑层（Compact Tier），优化高频插入和相似性查询的性能。

---

## 提示词

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

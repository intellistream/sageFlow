# Task B-03: PartitionCoordinator 分区协调器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: A-03 (BoundaryTracker) ✅, A-04 (LateArrivalHandler) ✅  
**输出文件**:
- `include/coordination/partition_coordinator.h`
- `src/coordination/partition_coordinator.cpp`
- `test/UnitTest/test_partition_coordinator.cpp`

---

## 任务描述

实现分区协调层，管理跨分区查询和延迟到达处理。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionCoordinator 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase (如 PartitionCoordinator)
- 方法名: camelBack (如 routeQuery, processRecord)
- 成员变量: lower_case_ 带尾部下划线 (如 boundary_tracker_, late_handler_)
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
分区后需要一个协调层来：
1. 管理跨分区查询
2. 处理延迟到达的记录
3. 监控分区负载均衡

## 依赖
- BoundaryTracker (A-03): 已实现于 include/coordination/boundary_tracker.h
- LateArrivalHandler (A-04): 已实现于 include/coordination/late_arrival_handler.h
- VectorSpacePartitioner (A-02): 已实现于 include/execution/vector_space_partitioner.h

## 文件位置
- 头文件: include/coordination/partition_coordinator.h
- 实现文件: src/coordination/partition_coordinator.cpp

## 接口要求

```cpp
#pragma once

#include "coordination/boundary_tracker.h"
#include "coordination/late_arrival_handler.h"
#include "execution/vector_space_partitioner.h"
#include "common/vector_record.h"
#include <vector>
#include <memory>
#include <atomic>

namespace sageFlow {

/**
 * @brief 记录处理结果
 */
struct ProcessResult {
    ArrivalStatus status;      ///< 到达状态
    size_t partition_id;       ///< 目标分区
    bool is_boundary;          ///< 是否为边界向量
};

/**
 * @brief 分区统计信息
 */
struct PartitionStats {
    size_t partition_id;       ///< 分区ID
    size_t record_count;       ///< 记录数量
    size_t boundary_count;     ///< 边界向量数量
};

/**
 * @brief 分区协调器
 * 
 * 协调跨分区查询和延迟到达处理。
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
     * @brief 路由查询到相关分区
     * @param query 查询向量
     * @param num_probes 探测分区数
     * @return 需要查询的分区ID列表
     */
    std::vector<size_t> routeQuery(const VectorRecord& query, size_t num_probes = 2);
    
    /**
     * @brief 处理到达的记录
     * @param record 到达的记录
     * @return 记录的到达状态、目标分区和边界标记
     */
    ProcessResult processRecord(const VectorRecord& record);
    
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
    std::vector<uint64_t> getBoundaryVectors(size_t partition_id) const;
    
    /**
     * @brief 缓冲延迟记录
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
     * @brief 更新分区记录计数
     * @param partition_id 分区ID
     * @param delta 变化量（正数增加，负数减少）
     */
    void updatePartitionCount(size_t partition_id, int64_t delta);
    
    /**
     * @brief 获取分区统计信息
     * @return 各分区的统计信息
     */
    std::vector<PartitionStats> getPartitionStats() const;
    
    /**
     * @brief 检测是否需要重平衡
     * @param imbalance_threshold 不平衡阈值 (max/avg)
     * @return 是否需要重平衡
     */
    bool needsRebalance(double imbalance_threshold = 2.0) const;
    
    /**
     * @brief 获取延迟处理统计
     */
    const LateArrivalHandler::Stats& getLateArrivalStats() const;
    
    /**
     * @brief 获取当前 watermark
     */
    int64_t getWatermark() const;
    
    /**
     * @brief 获取分区数量
     */
    size_t getNumPartitions() const { return num_partitions_; }

private:
    size_t num_partitions_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    std::unique_ptr<BoundaryTracker> boundary_tracker_;
    std::unique_ptr<LateArrivalHandler> late_handler_;
    
    // 分区记录计数
    std::vector<std::atomic<size_t>> partition_counts_;
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 创建 BoundaryTracker 实例
   - 创建 LateArrivalHandler 实例（使用提供的 allowed_lateness 和 watermark_delay）
   - 初始化分区计数器

2. **processRecord()**:
   ```cpp
   ProcessResult processRecord(const VectorRecord& record) {
       ProcessResult result;
       
       // 1. 检查到达状态
       result.status = late_handler_->processRecord(record);
       
       // 2. 确定分区
       result.partition_id = partitioner_->partition(record, num_partitions_);
       
       // 3. 检查是否为边界向量
       result.is_boundary = partitioner_->isBoundaryVector(record, num_partitions_);
       
       // 4. 如果是边界向量，标记
       if (result.is_boundary && result.status != ArrivalStatus::TOO_LATE) {
           markBoundary(record.getUid(), result.partition_id);
       }
       
       return result;
   }
   ```

3. **routeQuery()**:
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 获取候选分区中的边界向量
   - 添加边界向量所属的其他分区
   - 返回去重后的分区列表

4. **needsRebalance()**:
   - 计算分区负载的 max 和 avg
   - 如果 max/avg > imbalance_threshold，返回 true

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "coordination/partition_coordinator.h"

class PartitionCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        partitioner_ = std::make_shared<LSHPartitioner>(128, 8, 42);
        coordinator_ = std::make_unique<PartitionCoordinator>(
            4, partitioner_, 5000, 1000);
    }
    
    std::shared_ptr<LSHPartitioner> partitioner_;
    std::unique_ptr<PartitionCoordinator> coordinator_;
    
    std::unique_ptr<VectorRecord> createRecord(uint64_t uid, int64_t ts);
};

// 路由测试
TEST_F(PartitionCoordinatorTest, RouteQueryBasic) {
    // 测试基本查询路由
}

TEST_F(PartitionCoordinatorTest, RouteQueryWithProbes) {
    // 测试多分区探测
}

TEST_F(PartitionCoordinatorTest, RouteQueryIncludesBoundaryPartitions) {
    // 测试边界分区包含
}

// 记录处理测试
TEST_F(PartitionCoordinatorTest, ProcessRecordOnTime) {
    // 测试正常到达记录
}

TEST_F(PartitionCoordinatorTest, ProcessRecordLate) {
    // 测试延迟记录
}

TEST_F(PartitionCoordinatorTest, ProcessRecordTooLate) {
    // 测试过期记录
}

TEST_F(PartitionCoordinatorTest, ProcessRecordBoundary) {
    // 测试边界向量标记
}

// 延迟处理测试
TEST_F(PartitionCoordinatorTest, BufferAndFlushLateRecords) {
    // 测试延迟记录缓冲和刷新
}

// 统计测试
TEST_F(PartitionCoordinatorTest, PartitionStats) {
    // 测试分区统计
}

TEST_F(PartitionCoordinatorTest, RebalanceDetection) {
    // 测试负载不平衡检测
}

TEST_F(PartitionCoordinatorTest, RebalanceNotNeeded) {
    // 测试负载平衡时不触发
}

// 边界向量测试
TEST_F(PartitionCoordinatorTest, MarkAndUnmarkBoundary) {
    // 测试边界标记和取消
}

TEST_F(PartitionCoordinatorTest, GetBoundaryVectors) {
    // 测试获取边界向量列表
}
```

## 验收标准
1. 所有单元测试通过
2. 协调逻辑正确
3. 线程安全
4. 代码通过 clang-tidy 检查
```

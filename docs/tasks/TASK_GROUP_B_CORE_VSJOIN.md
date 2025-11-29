# Group B: VSJoin 核心组件

本文档包含 VSJoin 核心功能的组合组件任务，依赖 Group A 的基础任务。

---

## B-01: PartitionedIndex 分区索引

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: A-02 (LSHPartitioner)  
**输出文件**:
- `include/index/partitioned_index.h`
- `src/index/partitioned_index.cpp`
- `test/UnitTest/test_partitioned_index.cpp`

### 任务描述

实现分区索引结构，每个向量空间分区维护独立的 IVF 索引，支持分区级别的插入/删除/查询。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionedIndex 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线

## 背景
当前索引是全局共享的，所有线程竞争同一个索引。
分区索引让每个分区拥有独立的索引，减少锁竞争，提高并行效率。

## 任务目标
实现分区索引：
1. 每个分区维护独立的 IVF 索引
2. 支持分区级别的插入/删除/查询
3. 支持跨分区查询

## 依赖
- LSHPartitioner (A-02): 用于确定向量所属分区

## 文件位置
- 头文件: include/index/partitioned_index.h
- 实现文件: src/index/partitioned_index.cpp

## 接口要求

```cpp
#pragma once

#include "index/index.h"
#include "index/ivf.h"
#include "execution/vector_space_partitioner.h"
#include <vector>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace sageFlow {

/**
 * @brief 分区索引
 * 
 * 将向量空间分区，每个分区维护独立的 IVF 索引。
 * 支持分区级别的并发操作，减少全局锁竞争。
 */
class PartitionedIndex : public Index {
public:
    /**
     * @brief 构造函数
     * @param num_partitions 分区数量
     * @param dimension 向量维度
     * @param partitioner 向量空间分区器
     * @param nlist 每个分区 IVF 的聚类数
     * @param nprobes 查询时探测的聚类数
     */
    PartitionedIndex(size_t num_partitions, int dimension,
                     std::shared_ptr<VectorSpacePartitioner> partitioner,
                     int nlist = 100, int nprobes = 10);
    
    // Index 接口实现
    int insert(std::unique_ptr<VectorRecord> record) override;
    bool erase(uint64_t uid) override;
    
    std::vector<std::shared_ptr<const VectorRecord>> 
        query(const VectorRecord& query, int k) override;
    
    std::vector<std::shared_ptr<const VectorRecord>>
        queryForJoin(const VectorRecord& query, double threshold) override;
    
    size_t size() const override;
    
    // 分区特定操作
    
    /**
     * @brief 插入到指定分区
     * @param partition_id 分区ID
     * @param record 向量记录
     * @return 成功返回1，失败返回-1
     */
    int insertToPartition(size_t partition_id, std::unique_ptr<VectorRecord> record);
    
    /**
     * @brief 查询指定分区
     * @param partition_id 分区ID
     * @param query 查询向量
     * @param k 返回数量
     * @return 查询结果
     */
    std::vector<std::shared_ptr<const VectorRecord>>
        queryPartition(size_t partition_id, const VectorRecord& query, int k);
    
    /**
     * @brief 跨分区查询
     * @param query 查询向量
     * @param k 返回数量
     * @param num_probes 探测分区数
     * @return 合并去重的结果
     */
    std::vector<std::shared_ptr<const VectorRecord>>
        queryMultiPartition(const VectorRecord& query, int k, size_t num_probes = 2);
    
    /**
     * @brief 获取分区数量
     */
    size_t getNumPartitions() const { return num_partitions_; }
    
    /**
     * @brief 获取分区大小
     */
    size_t getPartitionSize(size_t partition_id) const;
    
    /**
     * @brief 获取分区负载统计
     */
    std::vector<size_t> getPartitionSizes() const;

private:
    size_t num_partitions_;
    int dimension_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    
    // 每个分区的索引
    std::vector<std::unique_ptr<Ivf>> partition_indexes_;
    
    // uid -> partition_id 映射，用于删除时定位分区
    std::unordered_map<uint64_t, size_t> uid_partition_map_;
    mutable std::shared_mutex map_mutex_;
    
    // 分区级别的锁
    std::vector<std::unique_ptr<std::shared_mutex>> partition_mutexes_;
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 创建 num_partitions 个独立的 Ivf 实例
   - 每个分区使用相同的 nlist 和 nprobes

2. **insert()**:
   - 使用 partitioner_ 确定分区
   - 更新 uid_partition_map_
   - 调用对应分区的 insert

3. **erase()**:
   - 从 uid_partition_map_ 查找分区
   - 调用对应分区的 erase
   - 移除映射

4. **queryMultiPartition()**:
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 并行查询多个分区
   - 合并去重结果，按距离排序

## 测试要求

```cpp
TEST(PartitionedIndexTest, InsertRouting) {
    // 测试插入路由到正确分区
}

TEST(PartitionedIndexTest, EraseCorrectness) {
    // 测试删除操作正确性
}

TEST(PartitionedIndexTest, SinglePartitionQuery) {
    // 测试单分区查询
}

TEST(PartitionedIndexTest, MultiPartitionQuery) {
    // 测试跨分区查询召回率
}

TEST(PartitionedIndexTest, ConcurrentAccess) {
    // 测试并发安全性
}

TEST(PartitionedIndexTest, LoadBalance) {
    // 测试分区负载均衡
}
```

## 验收标准
1. 所有单元测试通过
2. 跨分区查询召回率 > 95%
3. 并发测试无数据竞争
```

---

## B-02: PartitionedVectorState 分区向量状态

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: A-01 (TwoTierWindowState), A-02 (LSHPartitioner)  
**输出文件**:
- `include/state/partitioned_vector_state.h`
- `src/state/partitioned_vector_state.cpp`
- `test/UnitTest/test_partitioned_vector_state.cpp`

### 任务描述

实现分区向量状态，结合双层窗口和向量空间分区，是 VSJoin 的核心状态管理类。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionedVectorState 类。

## 背景
这是 VSJoin 的核心状态管理类，结合：
1. TwoTierWindowState（双层窗口）
2. LSHPartitioner（向量空间分区）

## 任务目标
实现分区向量状态：
1. 每个向量空间分区拥有独立的 TwoTierWindowState
2. 自动路由记录到正确分区
3. 支持跨分区查询

## 依赖
- TwoTierWindowState (A-01)
- LSHPartitioner (A-02)

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
     * @brief 触发所有分区的层压缩
     */
    void compactAllPartitions();

private:
    size_t num_partitions_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    bool enable_boundary_tracking_;
    
    // 每个向量空间分区的状态
    std::vector<std::unique_ptr<TwoTierWindowState>> partitions_;
    
    // 边界向量追踪器
    std::unique_ptr<BoundaryTracker> boundary_tracker_;
    
    // 用于 getRecords() 的合并视图
    mutable std::deque<std::unique_ptr<VectorRecord>> merged_view_;
    mutable std::shared_mutex merge_mutex_;
    
    /**
     * @brief 确定向量所属分区
     */
    size_t getPartitionId(const VectorRecord& record) const;
    
    /**
     * @brief 更新边界向量追踪
     */
    void updateBoundaryTracking(const VectorRecord& record, size_t partition_id);
};

} // namespace sageFlow
```

## 实现要点

1. **addRecord()**:
   - 使用 partitioner_ 确定分区
   - 将记录添加到对应分区的 TwoTierWindowState
   - 如果启用边界追踪，检查并标记边界向量

2. **getRecordsForQuery()**:
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 收集所有候选分区的记录
   - 包含边界向量

3. **evictExpired()**:
   - 遍历所有分区进行过期清理
   - 更新 boundary_tracker_（移除已删除向量的边界标记）

## 测试要求

```cpp
TEST(PartitionedVectorStateTest, RecordRouting) {
    // 测试记录路由到正确分区
}

TEST(PartitionedVectorStateTest, GetRecordsForQuery) {
    // 测试查询相关记录的覆盖率
}

TEST(PartitionedVectorStateTest, BoundaryVectorTracking) {
    // 测试边界向量正确追踪
}

TEST(PartitionedVectorStateTest, EvictExpiredAcrossPartitions) {
    // 测试过期清理跨分区一致性
}
```

## 验收标准
1. 所有单元测试通过
2. 查询覆盖率 > 95%
3. 边界向量追踪正确
```

---

## B-03: CoordinationLayer 协调层

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: A-03 (BoundaryTracker), A-04 (LateArrivalHandler)  
**输出文件**:
- `include/coordination/partition_coordinator.h`
- `src/coordination/partition_coordinator.cpp`
- `test/UnitTest/test_partition_coordinator.cpp`

### 任务描述

实现分区协调层，管理跨分区查询和延迟到达处理。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionCoordinator 类。

## 背景
分区后需要一个协调层来：
1. 管理跨分区查询
2. 处理延迟到达的记录
3. 监控分区负载均衡

## 依赖
- BoundaryTracker (A-03)
- LateArrivalHandler (A-04)

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

namespace sageFlow {

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
     * @param allowed_lateness 允许的延迟时间
     */
    PartitionCoordinator(size_t num_partitions,
                         std::shared_ptr<VectorSpacePartitioner> partitioner,
                         int64_t allowed_lateness = 5000);
    
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
     * @return 记录的到达状态和目标分区
     */
    struct ProcessResult {
        ArrivalStatus status;
        size_t partition_id;
        bool is_boundary;
    };
    ProcessResult processRecord(const VectorRecord& record);
    
    /**
     * @brief 标记边界向量
     */
    void markBoundary(uint64_t uid, size_t partition_id);
    
    /**
     * @brief 获取分区的边界向量
     */
    std::vector<uint64_t> getBoundaryVectors(size_t partition_id) const;
    
    /**
     * @brief 缓冲延迟记录
     */
    void bufferLateRecord(std::unique_ptr<VectorRecord> record);
    
    /**
     * @brief 获取并清空延迟缓冲区
     */
    std::vector<std::unique_ptr<VectorRecord>> flushLateBuffer();
    
    /**
     * @brief 获取分区负载统计
     */
    struct PartitionStats {
        size_t partition_id;
        size_t record_count;
        size_t boundary_count;
    };
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

1. **processRecord()**:
   - 调用 late_handler_->processRecord() 判断到达状态
   - 使用 partitioner_ 确定分区
   - 检查是否为边界向量

2. **routeQuery()**:
   - 获取主分区
   - 如果 num_probes > 1，添加邻近分区
   - 添加相关边界向量的分区

3. **needsRebalance()**:
   - 计算分区负载的 max/avg 比率
   - 超过阈值返回 true

## 测试要求

```cpp
TEST(PartitionCoordinatorTest, RouteQuery) {
    // 测试查询路由正确性
}

TEST(PartitionCoordinatorTest, ProcessRecord) {
    // 测试记录处理流程
}

TEST(PartitionCoordinatorTest, LateArrivalHandling) {
    // 测试延迟到达处理
}

TEST(PartitionCoordinatorTest, RebalanceDetection) {
    // 测试负载不平衡检测
}
```

## 验收标准
1. 所有单元测试通过
2. 协调逻辑正确
3. 线程安全
```

---

## B-04: AsyncCandidateGenerator 异步候选生成器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: A-05 (DistanceVerifier)  
**输出文件**:
- `include/operator/async_candidate_generator.h`
- `src/operator/async_candidate_generator.cpp`
- `test/UnitTest/test_async_candidate_generator.cpp`

### 任务描述

实现异步候选生成器，解耦候选生成和距离验证，实现流水线化处理。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 AsyncCandidateGenerator 类。

## 背景
当前候选生成是同步的，阻塞处理流程。
异步候选生成可以实现：
1. 候选生成与验证的流水线化
2. 批量查询优化
3. 提高 CPU 利用率

## 依赖
- DistanceVerifier (A-05): 用于验证候选

## 文件位置
- 头文件: include/operator/async_candidate_generator.h
- 实现文件: src/operator/async_candidate_generator.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include "index/partitioned_index.h"
#include "operator/distance_verifier.h"
#include <vector>
#include <memory>
#include <future>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace sageFlow {

/**
 * @brief 候选查询请求
 */
struct CandidateQuery {
    const VectorRecord* query;
    int k;
    size_t request_id;
};

/**
 * @brief 候选查询结果
 */
struct CandidateResult {
    size_t request_id;
    std::vector<std::unique_ptr<VectorRecord>> candidates;
};

/**
 * @brief 异步候选生成器
 * 
 * 使用线程池异步执行索引查询，支持批量查询和流水线处理。
 */
class AsyncCandidateGenerator {
public:
    /**
     * @brief 构造函数
     * @param index 分区索引
     * @param num_threads 工作线程数
     */
    explicit AsyncCandidateGenerator(
        std::shared_ptr<PartitionedIndex> index,
        size_t num_threads = 4);
    
    /**
     * @brief 析构函数
     */
    ~AsyncCandidateGenerator();
    
    /**
     * @brief 提交查询请求
     * @param query 查询向量
     * @param k 返回数量
     * @return 异步结果的 future
     */
    std::future<std::vector<std::unique_ptr<VectorRecord>>> 
        submitQuery(const VectorRecord& query, int k);
    
    /**
     * @brief 批量提交查询
     * @param queries 查询向量列表
     * @param k 每个查询的返回数量
     * @return 异步结果的 future 列表
     */
    std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>>
        submitBatch(const std::vector<const VectorRecord*>& queries, int k);
    
    /**
     * @brief 获取待处理查询数量
     */
    size_t getPendingCount() const;
    
    /**
     * @brief 关闭生成器
     */
    void shutdown();
    
    /**
     * @brief 是否正在运行
     */
    bool isRunning() const { return running_; }

private:
    std::shared_ptr<PartitionedIndex> index_;
    size_t num_threads_;
    
    // 任务队列
    struct Task {
        CandidateQuery query;
        std::promise<std::vector<std::unique_ptr<VectorRecord>>> promise;
    };
    std::queue<std::unique_ptr<Task>> task_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    
    // 工作线程
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{true};
    
    /**
     * @brief 工作线程循环
     */
    void workerLoop();
    
    /**
     * @brief 执行单个查询
     */
    std::vector<std::unique_ptr<VectorRecord>> executeQuery(const CandidateQuery& query);
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 启动 num_threads 个工作线程
   - 每个线程运行 workerLoop()

2. **submitQuery()**:
   - 创建 promise/future 对
   - 将任务加入队列
   - 通知工作线程

3. **workerLoop()**:
   ```cpp
   void workerLoop() {
       while (running_) {
           std::unique_ptr<Task> task;
           {
               std::unique_lock<std::mutex> lock(queue_mutex_);
               queue_cv_.wait(lock, [this] { 
                   return !task_queue_.empty() || !running_; 
               });
               
               if (!running_ && task_queue_.empty()) break;
               
               task = std::move(task_queue_.front());
               task_queue_.pop();
           }
           
           auto result = executeQuery(task->query);
           task->promise.set_value(std::move(result));
       }
   }
   ```

4. **shutdown()**:
   - 设置 running_ = false
   - 通知所有工作线程
   - 等待线程结束

## 测试要求

```cpp
TEST(AsyncCandidateGeneratorTest, SingleQuery) {
    // 测试单个异步查询
}

TEST(AsyncCandidateGeneratorTest, BatchQuery) {
    // 测试批量查询
}

TEST(AsyncCandidateGeneratorTest, ConcurrentSubmit) {
    // 测试并发提交
}

TEST(AsyncCandidateGeneratorTest, GracefulShutdown) {
    // 测试优雅关闭
}
```

## 验收标准
1. 所有单元测试通过
2. 异步结果正确
3. 无内存泄漏
```

---

## C-01: JoinOperator VSJoin 集成

**优先级**: 🔴 高  
**预估工时**: 4-5 天  
**依赖**: B-01, B-02, B-03, B-04  
**输出文件**:
- 修改 `include/operator/join_operator.h`
- 修改 `src/operator/join_operator.cpp`
- `test/IntegrationTest/test_vsjoin_integration.cpp`

### 任务描述

将 VSJoin 组件集成到 JoinOperator，实现完整的 VSJoin 流式向量连接算法。

### 提示词

```
你是 sageFlow 项目的开发者，需要将 VSJoin 组件集成到 JoinOperator。

## 背景
前面的任务实现了 VSJoin 的各个组件：
- TwoTierWindowState (A-01)
- LSHPartitioner / PartitionedIndex (A-02, B-01)
- BoundaryTracker / LateArrivalHandler (A-03, A-04)
- CoordinationLayer (B-03)
- PartitionedVectorState (B-02)
- DistanceVerifier / AsyncCandidateGenerator (A-05, B-04)

现在需要将它们集成到 JoinOperator 中。

## 任务目标
扩展 JoinOperator，支持 VSJoin 模式。

## 修改文件
- include/operator/join_operator.h
- src/operator/join_operator.cpp

## 新增配置

```cpp
// 在 JoinConfig 或构造函数中添加
struct VSJoinConfig {
    bool enabled = false;                    // 是否启用 VSJoin 模式
    int num_partitions = 8;                  // 向量空间分区数
    size_t compact_threshold = 100;          // 双层窗口压缩阈值
    bool enable_boundary_tracking = true;    // 启用边界向量追踪
    int64_t allowed_lateness = 0;            // 允许的延迟（0=不处理）
    size_t async_generator_threads = 4;      // 异步候选生成线程数
};
```

## 修改点清单

### 1. 添加成员变量

```cpp
// JoinOperator.h 中添加
private:
    // VSJoin 配置
    VSJoinConfig vsjoin_config_;
    
    // VSJoin 组件
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    std::unique_ptr<PartitionedVectorState> left_vsjoin_state_;
    std::unique_ptr<PartitionedVectorState> right_vsjoin_state_;
    std::unique_ptr<PartitionedIndex> left_vsjoin_index_;
    std::unique_ptr<PartitionedIndex> right_vsjoin_index_;
    std::unique_ptr<PartitionCoordinator> coordinator_;
    std::unique_ptr<AsyncCandidateGenerator> async_generator_;
    std::shared_ptr<DistanceVerifier> verifier_;
```

### 2. 修改 open()

```cpp
void JoinOperator::open(const RuntimeContext& context) {
    // 现有逻辑...
    
    if (vsjoin_config_.enabled) {
        // 初始化 VSJoin 组件
        partitioner_ = std::make_shared<LSHPartitioner>(
            dimension_, /*num_hash_functions=*/8);
        
        left_vsjoin_state_ = std::make_unique<PartitionedVectorState>(
            vsjoin_config_.num_partitions,
            partitioner_,
            vsjoin_config_.compact_threshold,
            vsjoin_config_.enable_boundary_tracking);
        
        // 类似初始化其他组件...
        
        coordinator_ = std::make_unique<PartitionCoordinator>(
            vsjoin_config_.num_partitions,
            partitioner_,
            vsjoin_config_.allowed_lateness);
    }
}
```

### 3. 修改 apply()

```cpp
void JoinOperator::apply(Response&& record, int slot, 
                         Collector& collector, 
                         const RuntimeContext& context) {
    if (vsjoin_config_.enabled) {
        applyVSJoin(std::move(record), slot, collector, context);
    } else {
        // 现有逻辑
        applyLegacy(std::move(record), slot, collector, context);
    }
}

void JoinOperator::applyVSJoin(Response&& record, int slot,
                                Collector& collector,
                                const RuntimeContext& context) {
    auto vec_record = extractVectorRecord(record);
    
    // 1. 处理延迟到达
    auto process_result = coordinator_->processRecord(*vec_record);
    if (process_result.status == ArrivalStatus::TOO_LATE) {
        // 记录统计，丢弃
        return;
    }
    if (process_result.status == ArrivalStatus::LATE) {
        coordinator_->bufferLateRecord(std::move(vec_record));
        return;
    }
    
    // 2. 更新状态
    if (slot == 0) {
        left_vsjoin_state_->addRecord(std::move(vec_record), 
                                       context.getSubtaskIndex());
    } else {
        right_vsjoin_state_->addRecord(std::move(vec_record),
                                        context.getSubtaskIndex());
    }
    
    // 3. 执行 join
    if (is_eager_) {
        executeVSJoinEager(*vec_record, slot, collector);
    }
}
```

### 4. 添加新的 join 方法名

```cpp
// 在 parseMethodType() 中添加
if (method_name == "vsjoin_eager") {
    return JoinMethodType::VSJOIN_EAGER;
} else if (method_name == "vsjoin_lazy") {
    return JoinMethodType::VSJOIN_LAZY;
}
```

## 向后兼容

- 保留所有现有接口和行为
- 只有显式配置 vsjoin_config_.enabled = true 时才启用新模式
- 使用 "vsjoin_eager" 或 "vsjoin_lazy" 方法名时自动启用

## 测试要求

```cpp
// test/IntegrationTest/test_vsjoin_integration.cpp

TEST(VSJoinIntegration, BasicFunctionality) {
    // 测试 VSJoin 模式基本功能
}

TEST(VSJoinIntegration, CompareWithLegacy) {
    // VSJoin 与现有模式结果对比（应该相同或更好）
}

TEST(VSJoinIntegration, LateArrivalHandling) {
    // 测试延迟到达处理
}

TEST(VSJoinIntegration, CrossPartitionJoin) {
    // 测试跨分区 join 正确性
}

TEST(VSJoinIntegration, Scalability) {
    // 测试不同并行度下的可扩展性
}
```

## 验收标准
1. 所有现有测试继续通过
2. VSJoin 模式功能正确
3. 与 legacy 模式结果一致性 > 99%
4. 性能不低于 legacy 模式
```

---

## C-02: AdaptiveIVF 自适应召回控制

**优先级**: 🟢 低  
**预估工时**: 2-3 天  
**依赖**: C-01  
**输出文件**:
- `include/index/adaptive_ivf.h`
- `src/index/adaptive_ivf.cpp`
- `test/UnitTest/test_adaptive_ivf.cpp`

### 任务描述

实现自适应 nprobes 调整机制，在运行时平衡召回率和性能。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 AdaptiveIVF 类。

## 背景
固定的 nprobes 可能导致：
- 太小：召回率不足
- 太大：性能下降

自适应调整可以在运行时平衡召回率和性能。

## 任务目标
实现 AdaptiveIVF：
1. 在线召回率估计
2. 自适应 nprobes 调整
3. 召回率目标配置

## 文件位置
- 头文件: include/index/adaptive_ivf.h
- 实现文件: src/index/adaptive_ivf.cpp

## 接口要求

```cpp
#pragma once

#include "index/ivf.h"
#include <atomic>
#include <deque>

namespace sageFlow {

/**
 * @brief 自适应 IVF 索引
 * 
 * 通过采样估计召回率，自动调整 nprobes 以达到目标召回率。
 */
class AdaptiveIVF : public Ivf {
public:
    /**
     * @brief 构造函数
     * @param nlist 聚类数量
     * @param rebuild_threshold 重建阈值
     * @param initial_nprobes 初始 nprobes
     * @param target_recall 目标召回率 (0.0-1.0)
     * @param sample_rate 采样率 (用于召回率估计)
     */
    AdaptiveIVF(int nlist, double rebuild_threshold, int initial_nprobes,
                double target_recall = 0.95, double sample_rate = 0.01);
    
    // 覆盖查询方法
    std::vector<std::shared_ptr<const VectorRecord>> 
        query(const VectorRecord& query, int k) override;
    
    std::vector<std::shared_ptr<const VectorRecord>>
        queryForJoin(const VectorRecord& query, double threshold) override;
    
    /**
     * @brief 获取当前 nprobes
     */
    int getCurrentNprobes() const { return current_nprobes_.load(); }
    
    /**
     * @brief 获取估计的召回率
     */
    double getEstimatedRecall() const { return estimated_recall_.load(); }
    
    /**
     * @brief 设置目标召回率
     */
    void setTargetRecall(double target) { target_recall_ = target; }
    
    /**
     * @brief 设置 nprobes 范围
     */
    void setNprobesRange(int min_probes, int max_probes);
    
    /**
     * @brief 获取统计信息
     */
    struct Stats {
        int current_nprobes;
        double estimated_recall;
        uint64_t sample_count;
        uint64_t adjustment_count;
    };
    Stats getStats() const;

private:
    double target_recall_;
    double sample_rate_;
    int min_nprobes_;
    int max_nprobes_;
    
    std::atomic<int> current_nprobes_;
    std::atomic<double> estimated_recall_{1.0};
    std::atomic<uint64_t> query_count_{0};
    std::atomic<uint64_t> sample_count_{0};
    std::atomic<uint64_t> adjustment_count_{0};
    
    // 召回率估计的滑动窗口
    std::deque<double> recall_samples_;
    mutable std::mutex samples_mutex_;
    static constexpr size_t MAX_SAMPLES = 100;
    
    /**
     * @brief 判断是否需要采样
     */
    bool shouldSample();
    
    /**
     * @brief 更新召回率估计
     * @param sample_recall 本次采样的召回率
     */
    void updateRecallEstimate(double sample_recall);
    
    /**
     * @brief 调整 nprobes
     */
    void adjustNprobes();
    
    /**
     * @brief 执行精确查询（用于采样验证）
     */
    std::vector<std::shared_ptr<const VectorRecord>>
        queryExact(const VectorRecord& query, int k);
};

} // namespace sageFlow
```

## 实现要点

1. **shouldSample()**:
   ```cpp
   bool shouldSample() {
       query_count_++;
       // 使用随机数决定是否采样
       static thread_local std::mt19937 gen(std::random_device{}());
       std::uniform_real_distribution<> dis(0.0, 1.0);
       return dis(gen) < sample_rate_;
   }
   ```

2. **updateRecallEstimate()**:
   - 对采样查询执行精确查询（nprobes = nlist）
   - 计算近似结果与精确结果的交集比例
   - 使用指数移动平均更新估计值

3. **adjustNprobes()**:
   - 召回率低于目标：增加 nprobes
   - 召回率高于目标 + 容差：减少 nprobes
   - 使用渐进式调整避免震荡

## 测试要求

```cpp
TEST(AdaptiveIVFTest, NprobesAutoIncrease) {
    // 测试召回率不足时 nprobes 自动增加
}

TEST(AdaptiveIVFTest, NprobesAutoDecrease) {
    // 测试召回率充足时 nprobes 自动减少
}

TEST(AdaptiveIVFTest, RecallEstimateAccuracy) {
    // 测试召回率估计准确性
}

TEST(AdaptiveIVFTest, SteadyState) {
    // 测试稳态行为（不频繁调整）
}
```

## 验收标准
1. 所有单元测试通过
2. 自适应调整正确
3. 召回率估计误差 < 5%
```

---

## 任务检查清单

| 任务ID | 状态 | 负责人 | 开始日期 | 完成日期 | 依赖完成 |
|--------|------|--------|----------|----------|----------|
| B-01 | ⬜ | - | - | - | A-02 |
| B-02 | ⬜ | - | - | - | A-01, A-02 |
| B-03 | ⬜ | - | - | - | A-03, A-04 |
| B-04 | ⬜ | - | - | - | A-05 |
| C-01 | ⬜ | - | - | - | B-01~B-04 |
| C-02 | ⬜ | - | - | - | C-01 |

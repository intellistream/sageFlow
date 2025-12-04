# SageFlow 统一连接策略详解

## 概述

SageFlow 采用**统一的 SPSC 队列矩阵**连接策略，在上下游算子之间建立 a×b 个点对点队列：

| 属性 | 值 |
|------|------|
| **队列数量** | upstream_parallelism × downstream_parallelism |
| **队列类型** | RingBufferQueue (SPSC, Lock-Free) |
| **生产者:消费者** | 1:1 (Single Producer Single Consumer) |

这种设计最大化吞吐量，同时通过 `WindowState` 抽象层处理状态共享需求。

---

## 1. 架构

### 1.1 SPSC 队列矩阵

```
                    上游算子 (parallelism=2)
                ┌─────────────┬─────────────┐
                │  Vertex[0]  │  Vertex[1]  │
                │   Thread-0  │   Thread-1  │
                └──────┬──────┴──────┬──────┘
                       │             │
          Partitioner  │             │  Partitioner
          选择目标队列  │             │  选择目标队列
                       │             │
          ┌────────────┼─────────────┼────────────┐
          │            │             │            │
          ▼            ▼             ▼            ▼
       ┌─────┐      ┌─────┐      ┌─────┐      ┌─────┐
       │Q[0] │      │Q[1] │      │Q[2] │      │Q[3] │  ← 4个 SPSC 队列
       │ 0→0 │      │ 0→1 │      │ 1→0 │      │ 1→1 │    (2×2 矩阵)
       └──┬──┘      └──┬──┘      └──┬──┘      └──┬──┘
          │            │             │            │
          └────────────┼─────────────┼────────────┘
                       │             │
                ┌──────┴──────┬──────┴──────┐
                │ InputGate   │ InputGate   │
                │ 轮询 Q[0,2] │ 轮询 Q[1,3] │
                └──────┬──────┴──────┬──────┘
                       │             │
                ┌──────┴──────┬──────┴──────┐
                │  Vertex[0]  │  Vertex[1]  │
                │   Thread-2  │   Thread-3  │
                └─────────────┴─────────────┘
                    下游算子 (parallelism=2)
```

### 1.2 队列索引计算

```cpp
// 队列索引公式
queue_index(upstream_i, downstream_j) = upstream_i × downstream_parallelism + downstream_j

// 示例：upstream=3, downstream=4
// upstream_0: [0, 1, 2, 3]
// upstream_1: [4, 5, 6, 7]
// upstream_2: [8, 9, 10, 11]
```

### 1.3 上下游配置

**上游 (ResultPartition)**:
- 上游实例 i 可以写入队列 `[i×D, i×D+1, ..., i×D+D-1]`
- 通过 `Partitioner` 选择具体目标队列

**下游 (InputGate)**:
- 下游实例 j 从队列 `[0×D+j, 1×D+j, 2×D+j, ...]` 轮询读取
- 无锁轮询，非阻塞

---

## 2. 代码实现

### 2.1 连接策略类

```cpp
// include/execution/connection_strategy.h

class ConnectionStrategy {
public:
    /**
     * @brief 创建 SPSC 队列矩阵
     * @param upstream_parallelism 上游并行度
     * @param downstream_parallelism 下游并行度  
     * @param use_blocking 是否使用阻塞队列（用于特殊场景）
     * @return queue_count = upstream × downstream 个队列
     */
    std::vector<QueuePtr> createQueues(
        size_t upstream_parallelism,
        size_t downstream_parallelism);

    /**
     * @brief 配置上游输出通道
     * 上游 i 连接到队列 [i*D, i*D+1, ..., i*D+D-1]
     * 使用下游算子指定的分区器，或默认 RoundRobin
     */
    void setupResultPartition(
        ResultPartition& partition,
        const std::vector<QueuePtr>& queues,
        size_t upstream_index,
        size_t upstream_parallelism,
        size_t downstream_parallelism,
        int slot,
        std::unique_ptr<IPartitioner> partitioner = nullptr);

    /**
     * @brief 配置下游输入网关
     * 下游 j 读取队列 [0*D+j, 1*D+j, 2*D+j, ...]
     */
    void setupInputGate(
        InputGate& gate,
        const std::vector<QueuePtr>& queues,
        size_t downstream_index,
        size_t upstream_parallelism,
        size_t downstream_parallelism);
};
```

### 2.2 分区器选择

下游算子可以通过 `getPreferredPartitioner()` 方法指定期望的分区器：

```cpp
// include/operator/operator.h

class Operator {
    // ...
    
    /**
     * @brief 获取算子期望的输入分区器
     * 默认返回 nullptr（使用 RoundRobin）
     * JoinOperator 可重写以支持不同策略
     */
    virtual std::unique_ptr<IPartitioner> getPreferredPartitioner(
        int dimension = 0, int num_partitions = 0) const;
};

// JoinOperator 根据配置返回适当的分区器：
// - 共享索引 Join (bruteforce/ivf): RoundRobin（负载均衡）
// - VSJoin: LSH 分区器（向量空间分区）
```

### 2.3 队列创建

```cpp
// src/execution/connection_strategy.cpp

std::vector<QueuePtr> ConnectionStrategy::createQueues(
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    bool use_blocking) {
    
    size_t queue_count = upstream_parallelism * downstream_parallelism;
    std::vector<QueuePtr> queues;
    queues.reserve(queue_count);
    
    for (size_t i = 0; i < queue_count; ++i) {
        if (use_blocking) {
            queues.push_back(std::make_shared<BlockingQueue>(capacity_));
        } else {
            queues.push_back(std::make_shared<RingBufferQueue>(capacity_));
        }
    }
    return queues;
}
```

---

## 3. 数据流与状态管理

### 3.1 数据分发模式

通过 `Partitioner` 控制数据路由：

| Partitioner | 路由逻辑 | 适用场景 |
|-------------|----------|----------|
| `RoundRobinPartitioner` | 轮询分发，负载均衡 | 共享索引 Join |
| `KeyPartitioner` | 按 key hash 到固定分区 | 分区 Join |
| `VectorHashPartitioner` | 按向量 hash 分区 | 向量分区 |
| `BroadcastPartitioner` | 广播到所有下游 | 全局聚合 |

### 3.2 与 WindowState 配合

| Partitioner | 推荐 WindowState | 说明 |
|-------------|------------------|------|
| `RoundRobinPartitioner` | `SharedWindowState` | 数据可能到达任意下游，需共享状态 |
| `KeyPartitioner` | `PartitionedWindowState` | 同 key 数据到达同一下游 |
| `VectorHashPartitioner` | `PartitionedWindowState` | 相似向量到达同一下游 |

### 3.3 锁分析

**SPSC 队列 + SharedWindowState (推荐的共享索引场景)**:
```
每条数据的锁开销：
  队列操作：0 锁 (SPSC 无锁)
  状态操作：1 锁 (SharedWindowState 全局锁)
  总计：1 锁/tuple
```

**SPSC 队列 + PartitionedWindowState (分区场景)**:
```
每条数据的锁开销：
  队列操作：0 锁 (SPSC 无锁)
  状态操作：0 锁 (分区隔离，无竞争)
  总计：0 锁/tuple
```

---

## 4. Join 算子的多上游处理

Join 算子有两个输入流（left/right），每个上游都建立独立的队列矩阵：

```
                Left Stream                    Right Stream
                (parallelism=2)                (parallelism=2)
                ┌────┬────┐                    ┌────┬────┐
                │ L0 │ L1 │                    │ R0 │ R1 │
                └─┬──┴──┬─┘                    └─┬──┴──┬─┘
                  │     │                        │     │
            ┌─────┴─────┴─────┐            ┌─────┴─────┴─────┐
            │  slot=0 队列    │            │  slot=1 队列    │
            │  [Q0,Q1,Q2,Q3]  │            │  [Q4,Q5,Q6,Q7]  │
            └────────┬────────┘            └────────┬────────┘
                     │                              │
                     └──────────────┬───────────────┘
                                    │
                              ┌─────┴─────┐
                              │ InputGate │  轮询两组队列
                              │ 合并读取  │
                              └─────┬─────┘
                                    │
                              ┌─────┴─────┐
                              │  Join[j]  │
                              └───────────┘
```

**InputGate 轮询策略**:
- 依次从 slot=0 和 slot=1 的队列集合中轮询
- 非阻塞读取，避免单流饥饿
- 通过 `TaggedResponse.slot` 标识数据来源

---

## 5. 使用方式

### 5.1 API

```cpp
// 添加算子（使用统一连接策略）
graph.addOperator(filter_op);
graph.addOperator(join_op);

// 连接算子
graph.connectOperators(source, filter);
graph.connectOperators(filter, join, /*slot=*/0);  // left stream
graph.connectOperators(other, join, /*slot=*/1);   // right stream
```

### 5.2 完整示例

```cpp
// 创建共享索引 Join
auto join_op = std::make_shared<JoinOperator>(
    join_func,
    concurrency_manager,
    "bruteforce_lazy",
    0.8,                    // similarity_threshold
    false,                  // enable_profiling
    "",                     // profile_output_path
    true);                  // use_shared_state = true → SharedWindowState

graph.addOperator(join_op);
graph.connectOperators(left_source, join_op, 0);
graph.connectOperators(right_source, join_op, 1);

// RoundRobinPartitioner 会自动用于负载均衡
```

---

## 6. 性能特点

| 维度 | SPSC 矩阵设计 |
|------|--------------|
| **队列吞吐** | 极高 (无锁 SPSC) |
| **延迟** | 低 (无锁操作) |
| **CPU 缓存** | 友好 (每队列独立内存) |
| **内存开销** | 中等 (a×b 个队列) |
| **负载均衡** | 通过 Partitioner 控制 |
| **状态同步** | 通过 WindowState 抽象 |

---

## 7. 实现文件清单

| 文件 | 说明 |
|------|------|
| `include/execution/connection_strategy.h` | 统一连接策略接口 |
| `src/execution/connection_strategy.cpp` | 策略实现 |
| `src/execution/execution_graph.cpp` | 策略应用 |
| `include/execution/ring_buffer_queue.h` | SPSC 无锁队列 |
| `include/state/window_state.h` | 状态抽象接口 |
| `include/state/partitioned_window_state.h` | 分区状态实现 |
| `include/state/shared_window_state.h` | 共享状态实现 |

---

## 更新日志

- **2025-01-XX**：统一为单一 SPSC 矩阵策略，移除 SHARED_QUEUE 模式
- **2025-11-26**：初始版本，支持分区和共享队列两种模式


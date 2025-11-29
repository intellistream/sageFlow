# SageFlow 连接策略详解：分区模式 vs 共享队列模式

## 概述

SageFlow 支持两种算子间的连接策略，用于控制数据在上下游算子之间的流动方式：

| 策略 | 类名 | 适用场景 |
|------|------|----------|
| **分区模式** | `PartitionedConnectionStrategy` | 基于分区的 Join、常规流处理 |
| **共享队列模式** | `SharedQueueConnectionStrategy` | 共享索引的 Join、负载均衡场景 |

---

## 1. 架构对比

### 1.1 分区模式 (Partitioned)

```
                    上游算子 (parallelism=2)
                ┌─────────────┬─────────────┐
                │  Vertex[0]  │  Vertex[1]  │
                │   Thread-0  │   Thread-1  │
                └──────┬──────┴──────┬──────┘
                       │             │
          ResultPartition    ResultPartition
          output=[Q0]        output=[Q1]
                       │             │
                       ▼             ▼
                    ┌─────┐       ┌─────┐
                    │ Q0  │       │ Q1  │     ← 队列数 = 上游并行度
                    └─────┘       └─────┘
                       │             │
                       └──────┬──────┘
                              │
                    ┌─────────▼─────────┐
                    │     InputGate     │
                    │ input=[Q0, Q1]    │  ← 每个下游读取所有队列
                    └─────────┬─────────┘
                              │
                ┌─────────────┴─────────────┐
                │  Vertex[0]  │  Vertex[1]  │
                │   Thread-2  │   Thread-3  │
                └─────────────┴─────────────┘
                    下游算子 (parallelism=2)
```

**特点**：
- 队列数量 = **上游并行度**
- 每个上游实例有**独立的输出队列**
- 每个下游实例从**所有队列**轮询读取
- 数据通过 Partitioner 决定发往哪个队列

### 1.2 共享队列模式 (Shared Queue)

```
                    上游算子 (parallelism=2)
                ┌─────────────┬─────────────┐
                │  Vertex[0]  │  Vertex[1]  │
                │   Thread-0  │   Thread-1  │
                └──────┬──────┴──────┬──────┘
                       │             │
          ResultPartition    ResultPartition
          output=[Q0,Q1]     output=[Q0,Q1]
                       │             │
                       └──────┬──────┘
                              │ (都写入共享队列池)
                              ▼
                    ┌─────┐ ┌─────┐
                    │ Q0  │ │ Q1  │     ← 队列数 = 下游并行度
                    └─────┘ └─────┘
                       │       │
                       ▼       ▼
                    ┌─────┐ ┌─────┐
                    │Input│ │Input│     ← 每个下游只读一个队列
                    │Gate │ │Gate │
                    │[Q0] │ │[Q1] │
                    └──┬──┘ └──┬──┘
                       │       │
                ┌──────┴───────┴──────┐
                │  Vertex[0]  Vertex[1]│
                │   Thread-2  Thread-3 │
                └─────────────────────┘
                    下游算子 (parallelism=2)
```

**特点**：
- 队列数量 = **下游并行度**
- 所有上游实例**共享同一组队列**
- 每个下游实例只从**对应的一个队列**读取
- 数据通过轮询分发到各共享队列

---

## 2. 代码实现对比

### 2.1 队列创建

| 维度 | 分区模式 | 共享队列模式 |
|------|----------|--------------|
| 队列数量 | `upstream_parallelism` | `downstream_parallelism` |
| 队列类型 | Join用BlockingQueue，其他用RingBufferQueue | 始终用BlockingQueue |
| 生产者:消费者 | 1:N (单生产者多消费者) | M:1 (多生产者单消费者) |

```cpp
// PartitionedConnectionStrategy::createQueues
size_t queue_count = upstream_parallelism;  // ← 与上游一致

// SharedQueueConnectionStrategy::createQueues
size_t queue_count = downstream_parallelism;  // ← 与下游一致
```

### 2.2 上游配置 (ResultPartition)

| 维度 | 分区模式 | 共享队列模式 |
|------|----------|--------------|
| 输出通道 | 自己对应的队列（或全部队列） | 所有共享队列 |
| 分区策略 | RoundRobin 分发 | RoundRobin 分发 |

```cpp
// PartitionedConnectionStrategy::setupResultPartition
if (downstream_parallelism == 1) {
    output_channels.push_back(queues[upstream_index % queues.size()]);
} else {
    for (size_t j = 0; j < downstream_parallelism; ++j) {
        output_channels.push_back(queues[j % queues.size()]);
    }
}

// SharedQueueConnectionStrategy::setupResultPartition
for (const auto& queue : queues) {
    output_channels.push_back(queue);  // 所有上游都连接所有队列
}
```

### 2.3 下游配置 (InputGate)

| 维度 | 分区模式 | 共享队列模式 |
|------|----------|--------------|
| 输入队列 | 所有上游队列 | 仅自己对应的队列 |
| 竞争关系 | 无（各读各的） | 多上游竞争写入 |

```cpp
// PartitionedConnectionStrategy::setupInputGate
for (size_t j = 0; j < upstream_parallelism; ++j) {
    input_queues.push_back(queues[j]);  // 读取所有队列
}

// SharedQueueConnectionStrategy::setupInputGate
input_queues.push_back(queues[downstream_index]);  // 只读自己的队列
```

---

## 3. 数据流对比

### 3.1 分区模式数据流

```
时间线 ────────────────────────────────────────────────────────▶

上游[0] 产生数据 D1
    │
    ▼ emit(D1) → Partitioner选择 Q0
    │
    └──▶ Q0.push(D1)
              │
              ▼
         下游[0] 从 Q0 读取 D1
         下游[1] 从 Q0 读取 (轮询到时)

上游[1] 产生数据 D2
    │
    ▼ emit(D2) → Partitioner选择 Q1
    │
    └──▶ Q1.push(D2)
              │
              ▼
         下游[0] 从 Q1 读取 D2 (轮询到时)
         下游[1] 从 Q1 读取 D2

特点：
- D1 只在 Q0 中
- D2 只在 Q1 中
- 下游需要轮询多个队列才能获取所有数据
```

### 3.2 共享队列模式数据流

```
时间线 ────────────────────────────────────────────────────────▶

上游[0] 产生数据 D1
    │
    ▼ emit(D1) → RoundRobin选择 Q0
    │
    └──▶ Q0.push(D1)
              │
              ▼
         下游[0] 从 Q0 读取 D1  ← 只有下游[0]能读到

上游[1] 产生数据 D2
    │
    ▼ emit(D2) → RoundRobin选择 Q1
    │
    └──▶ Q1.push(D2)
              │
              ▼
         下游[1] 从 Q1 读取 D2  ← 只有下游[1]能读到

上游[0] 产生数据 D3
    │
    ▼ emit(D3) → RoundRobin选择 Q1 (轮转)
    │
    └──▶ Q1.push(D3)
              │
              ▼
         下游[1] 从 Q1 读取 D3  ← 只有下游[1]能读到

特点：
- 同一数据只会被一个下游实例处理
- 自动负载均衡（轮询分发）
- 不同数据可能被不同下游处理
```

---

## 4. 与状态管理的配合

### 4.1 分区模式 + PartitionedWindowState

```
推荐组合：PARTITIONED + PartitionedWindowState

                    数据流向
    ┌────────────────────────────────────────┐
    │                                        │
    │  上游[0] ───D1,D3───▶ Q0 ───▶ 下游[0]  │
    │                              │         │
    │                              ▼         │
    │                        WindowState     │
    │                        Partition[0]    │
    │                                        │
    │  上游[1] ───D2,D4───▶ Q1 ───▶ 下游[1]  │
    │                              │         │
    │                              ▼         │
    │                        WindowState     │
    │                        Partition[1]    │
    └────────────────────────────────────────┘

特点：
- 数据按分区隔离
- 每个下游只处理特定分区的数据
- 状态也按分区隔离，无锁竞争
- Recall 取决于数据分布（可能丢失跨分区匹配）
```

### 4.2 共享队列模式 + SharedWindowState

```
推荐组合：SHARED_QUEUE + SharedWindowState

                    数据流向
    ┌─────────────────────────────────────────────┐
    │                                             │
    │  上游[0] ──┬──D1───▶ Q0 ───▶ 下游[0] ──┐    │
    │            │                           │    │
    │            └──D3───▶ Q1 ───▶ 下游[1] ──┤    │
    │                                        │    │
    │  上游[1] ──┬──D2───▶ Q0 ───▶ 下游[0] ──┤    │
    │            │                           │    │
    │            └──D4───▶ Q1 ───▶ 下游[1] ──┤    │
    │                                        ▼    │
    │                                 ┌───────────┐│
    │                                 │  Shared   ││
    │                                 │ WindowState│
    │                                 │ (全局锁)  ││
    │                                 └───────────┘│
    └─────────────────────────────────────────────┘

特点：
- 数据按轮询分发，负载均衡
- 每个下游可能处理任意上游的数据
- 状态全局共享，需要读写锁
- Recall 可以达到 100%（所有数据都可见）
```

---

## 5. 使用方式

### 5.1 API 层面

```cpp
// 方式1：使用默认分区模式
env.addOperator(join_op);  // 默认 ConnectionType::PARTITIONED

// 方式2：显式指定连接类型
env.addOperator(join_op, ConnectionType::PARTITIONED);
env.addOperator(join_op, ConnectionType::SHARED_QUEUE);
```

### 5.2 完整配置示例

```cpp
// 分区模式配置
auto join_op = std::make_shared<JoinOperator>(
    join_func,
    concurrency_manager,
    "bruteforce_lazy",
    0.8,                    // similarity_threshold
    false,                  // enable_profiling
    "",                     // profile_output_path
    false);                 // use_shared_state = false → PartitionedWindowState
env.addOperator(join_op, ConnectionType::PARTITIONED);

// 共享队列模式配置
auto join_op = std::make_shared<JoinOperator>(
    join_func,
    concurrency_manager,
    "ivf_lazy",
    0.8,
    false,
    "",
    true);                  // use_shared_state = true → SharedWindowState
env.addOperator(join_op, ConnectionType::SHARED_QUEUE);
```

---

## 6. 性能对比

| 维度 | 分区模式 | 共享队列模式 |
|------|----------|--------------|
| **锁竞争** | 低（分区隔离） | 高（共享状态需要全局锁） |
| **负载均衡** | 取决于分区策略 | 自动均衡（轮询） |
| **Recall** | 可能丢失跨分区匹配 | 100%（所有数据可见） |
| **吞吐量** | 高（无锁或细粒度锁） | 中（全局锁开销） |
| **延迟** | 低 | 可能因锁等待增加 |
| **内存** | 分散在各分区 | 集中在共享状态 |

---

## 7. 选择指南

```
                        ┌─────────────────────────────┐
                        │      需要 100% Recall?       │
                        └─────────────┬───────────────┘
                                      │
                      ┌───────────────┴───────────────┐
                      │                               │
                      ▼ Yes                           ▼ No
            ┌─────────────────┐             ┌─────────────────┐
            │  SHARED_QUEUE   │             │   PARTITIONED   │
            │      +          │             │        +        │
            │ SharedWindowState│            │PartitionedWindow│
            └─────────────────┘             └─────────────────┘
                      │                               │
                      ▼                               ▼
            ┌─────────────────┐             ┌─────────────────┐
            │ 适用场景:        │             │ 适用场景:        │
            │ - 共享索引 Join  │             │ - 分区 Join     │
            │ - 小规模数据     │             │ - 大规模数据    │
            │ - 低并行度       │             │ - 高并行度      │
            │ - 准确性优先     │             │ - 吞吐量优先    │
            └─────────────────┘             └─────────────────┘
```

---

## 8. 实现文件清单

| 文件 | 说明 |
|------|------|
| `include/execution/connection_strategy.h` | `IConnectionStrategy` 接口定义 |
| `include/execution/partitioned_connection_strategy.h` | 分区策略头文件 |
| `include/execution/shared_queue_connection_strategy.h` | 共享队列策略头文件 |
| `src/execution/partitioned_connection_strategy.cpp` | 分区策略实现 |
| `src/execution/shared_queue_connection_strategy.cpp` | 共享队列策略实现 |
| `src/execution/execution_graph.cpp` | 策略选择与应用 |
| `include/state/window_state.h` | 状态抽象接口 |
| `include/state/partitioned_window_state.h` | 分区状态实现 |
| `include/state/shared_window_state.h` | 共享状态实现 |

---

## 9. 未来扩展

1. **动态策略切换**：运行时根据负载自动切换策略
2. **混合模式**：部分算子用分区，部分用共享
3. **自定义 Partitioner**：支持 Hash、Range 等分区策略
4. **背压机制**：队列满时的流控策略
5. **状态后端**：支持 RocksDB 等持久化状态后端

---

## 更新日志

- **2025-11-26**：初始版本，基于 refactor-sageflow-multithreading 分支


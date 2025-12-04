# SageFlow 系统架构详解

> **目标读者**：新加入项目的开发者，希望快速上手 Join 算子开发和 Baseline 复现  
> **文档版本**：v1.0 (2024-12)  
> **对应 PR**：feat: VSJoin 流式向量相似性连接引擎 - 多线程架构重构与 Baseline 实现

---

## 目录

1. [系统概述](#1-系统概述)
2. [整体架构图](#2-整体架构图)
3. [核心组件详解](#3-核心组件详解)
4. [Join 执行流程](#4-join-执行流程)
5. [数据流图解](#5-数据流图解)
6. [Join 方法实现指南](#6-join-方法实现指南)
7. [Baseline 复现指南](#7-baseline-复现指南)
8. [配置系统](#8-配置系统)
9. [测试与调试](#9-测试与调试)

---

## 1. 系统概述

### 1.1 什么是 SageFlow？

SageFlow 是一个**向量原生的流处理引擎**，专为实时 LLM 生成任务设计。它提供声明式 API 来组合有状态的向量操作，支持在时间窗口内对动态变化的数据集进行快速语义上下文更新。

### 1.2 核心能力

| 能力 | 描述 |
|------|------|
| **流式向量 Join** | 两个向量数据流之间的实时相似性连接 |
| **多 Join 算法** | BruteForce、IVF、HNSW、S3J、VSJoin 等 |
| **窗口管理** | 滑动窗口、会话窗口的状态维护 |
| **并行执行** | 多线程并行处理，支持分区和共享两种模式 |
| **索引加速** | 内置 HNSW、IVF、BruteForce 等索引实现 |

### 1.3 技术栈

- **语言**: C++20
- **构建**: CMake 3.20+
- **测试**: Google Test
- **日志**: spdlog
- **配置**: TOML (tomlplusplus)

---

## 2. 整体架构图

### 2.1 系统分层架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            用户 API 层 (User API Layer)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Stream    │  │ StreamEnv   │  │  Planner    │  │   Config    │        │
│  │   Builder   │  │ (执行环境)   │  │ (执行计划)   │  │   Loader    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└───────────────────────────────────────────────────────────────────────────┬┘
                                                                             │
┌───────────────────────────────────────────────────────────────────────────▼┘
│                           算子层 (Operator Layer)                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  Source  │  │  Filter  │  │   Map    │  │   Join   │  │   Sink   │      │
│  │ Operator │  │ Operator │  │ Operator │  │ Operator │  │ Operator │      │
│  └──────────┘  └──────────┘  └──────────┘  └────┬─────┘  └──────────┘      │
│                                                 │                          │
│                              ┌──────────────────┴──────────────────┐       │
│                              │        Join Method Layer            │       │
│                              │  ┌────────────┐ ┌────────────────┐  │       │
│                              │  │ BruteForce │ │ IVF/HNSW/S3J   │  │       │
│                              │  └────────────┘ └────────────────┘  │       │
│                              └─────────────────────────────────────┘       │
└───────────────────────────────────────────────────────────────────────────┬┘
                                                                             │
┌───────────────────────────────────────────────────────────────────────────▼┘
│                           执行层 (Execution Layer)                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐    │
│  │ ExecutionGraph │  │ExecutionVertex │  │   RuntimeContext           │    │
│  │  (执行图)       │  │  (执行顶点)    │  │  (运行时上下文)             │    │
│  └───────┬────────┘  └───────┬────────┘  └────────────────────────────┘    │
│          │                   │                                             │
│  ┌───────▼───────────────────▼────────┐  ┌─────────────────────────────┐   │
│  │         Queue System               │  │   Connection Strategy       │   │
│  │  ┌────────────┐  ┌──────────────┐  │  │  ┌──────────┐ ┌──────────┐  │   │
│  │  │ InputGate  │  │ResultPartition│ │  │  │Partitioned│ │SharedQueue│ │   │
│  │  └────────────┘  └──────────────┘  │  │  └──────────┘ └──────────┘  │   │
│  └────────────────────────────────────┘  └─────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┬┘
                                                                             │
┌───────────────────────────────────────────────────────────────────────────▼┘
│                           状态层 (State Layer)                              │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                        WindowState Interface                        │    │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────┐ │    │
│  │  │SharedWindow  │ │Partitioned   │ │TwoTierWindow │ │Partitioned │ │    │
│  │  │    State     │ │ WindowState  │ │    State     │ │VectorState │ │    │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └────────────┘ │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────────┬┘
                                                                             │
┌───────────────────────────────────────────────────────────────────────────▼┘
│                           索引层 (Index Layer)                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     ConcurrencyManager                               │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────┐   │   │
│  │  │BruteForce│  │   HNSW   │  │   IVF    │  │  PartitionedIndex  │   │   │
│  │  │  Index   │  │  Index   │  │  Index   │  │    (分区索引)       │   │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┬┘
                                                                             │
┌───────────────────────────────────────────────────────────────────────────▼┘
│                           存储层 (Storage Layer)                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                       StorageManager                                 │   │
│  │               (向量数据的统一存储与访问)                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 模块关系图

```
                                ┌─────────────────┐
                                │  用户代码        │
                                │  (Pipeline 定义) │
                                └────────┬────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │           StreamEnvironment             │
                    │  - addStream(source)                    │
                    │  - execute()                            │
                    │  - awaitTermination()                   │
                    └────────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │              Planner                     │
                    │  - 解析 Stream DAG                       │
                    │  - 创建 Operator Chain                   │
                    │  - 分配 Slot ID                          │
                    └────────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │           ExecutionGraph                 │
                    │  - 创建 ExecutionVertex (并行实例)       │
                    │  - 建立 Queue 连接                       │
                    │  - 管理线程生命周期                       │
                    └────────────────────┬────────────────────┘
                                         │
        ┌────────────────┬───────────────┼───────────────┬────────────────┐
        ▼                ▼               ▼               ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ExecutionVertex│ │ExecutionVertex│ │ExecutionVertex│ │ExecutionVertex│ │ExecutionVertex│
│   [Source]   │ │   [Filter]   │ │    [Join]    │ │    [Join]    │ │    [Sink]    │
│  Thread-0    │ │  Thread-1    │ │  Thread-2    │ │  Thread-3    │ │  Thread-4    │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │               │               │                │
       └────────────────┴───────────────┴───────────────┴────────────────┘
                                   │
                              Queue System
                         (数据在线程间流动)
```

---

## 3. 核心组件详解

### 3.1 RuntimeContext（运行时上下文）

**位置**: `include/execution/runtime_context.h`

RuntimeContext 为每个并行执行实例提供身份标识，是实现分区状态访问的关键。

```cpp
class RuntimeContext {
public:
    // 获取当前实例的索引（0-based）
    size_t getSubtaskIndex() const;
    
    // 获取总并行度
    size_t getParallelism() const;
    
    // 获取任务名称（用于日志）
    std::string getTaskName() const;  // 返回 "Task[2/8]" 格式
};
```

**使用场景**：
```cpp
void JoinOperator::open(const RuntimeContext& context) {
    // 根据 subtask_index 访问对应的分区状态
    size_t my_partition = context.getSubtaskIndex();
    auto& my_state = partitioned_state_->getRecords(my_partition);
}
```

### 3.2 WindowState（窗口状态接口）

**位置**: `include/state/window_state.h`

统一的窗口状态抽象，支持四种实现：

| 实现类 | 描述 | 适用场景 |
|-------|------|---------|
| `SharedWindowState` | 所有实例共享，需要锁同步 | RoundRobin 分区 |
| `PartitionedWindowState` | 每个 subtask 独立状态 | Key/Hash 分区 |
| `TwoTierWindowState` | 两层架构（写友好+紧凑层） | 高吞吐场景 |
| `PartitionedVectorState` | 向量空间分区 | VSJoin/S3J |

```cpp
class WindowState {
public:
    // 添加记录
    virtual void addRecord(std::unique_ptr<VectorRecord> record, 
                          size_t subtask_index) = 0;
    
    // 获取记录（只读引用）
    virtual const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const = 0;
    
    // 获取线程安全快照
    virtual std::vector<std::shared_ptr<const VectorRecord>> 
        getRecordsSnapshot(size_t subtask_index) const = 0;
    
    // 清理过期记录
    virtual void evictExpired(int64_t current_timestamp, 
                            int64_t window_size,
                            size_t subtask_index) = 0;
    
    // 检查是否为共享状态
    virtual bool isShared() const = 0;
};
```

### 3.3 ExecutionGraph & ExecutionVertex

**位置**: `include/execution/execution_graph.h`, `include/execution/execution_vertex.h`

ExecutionGraph 管理整个执行 DAG，ExecutionVertex 是单个并行执行实例。

```
ExecutionGraph
├── operators_: vector<Operator>     // 所有算子
├── operator_infos_: map             // 算子元信息（并行度等）
├── connections_: vector<tuple>      // 算子连接关系
└── all_queues_: vector<QueuePtr>    // 所有队列

ExecutionVertex
├── operator_: Operator              // 执行的算子
├── input_gate_: InputGate           // 输入端
├── result_partition_: ResultPartition // 输出端
├── context_: RuntimeContext         // 运行时上下文
└── thread_: std::thread             // 执行线程
```

### 3.4 Connection Strategy（连接策略）

**位置**: `include/execution/connection_strategy.h`

两种策略控制数据在算子间的流动方式：

#### Partitioned 模式

```
上游 [V0] ──→ Q0 ──→ ┐
                    ├──→ 下游 [V0] (轮询 Q0, Q1)
上游 [V1] ──→ Q1 ──→ ┘
                    ├──→ 下游 [V1] (轮询 Q0, Q1)
```

- 队列数 = 上游并行度
- 每个上游写自己的队列
- 每个下游读所有队列

#### SharedQueue 模式

```
上游 [V0] ──┐       ┌──→ 下游 [V0] (只读 Q0)
            ├──→ Q0 ┘
上游 [V1] ──┤
            ├──→ Q1 ───→ 下游 [V1] (只读 Q1)
```

- 队列数 = 下游并行度
- 所有上游共享队列池
- 每个下游只读自己的队列

### 3.5 ConcurrencyManager（并发管理器）

**位置**: `include/concurrency/concurrency_manager.h`

线程安全的索引管理器，提供统一的索引访问接口。

```cpp
class ConcurrencyManager {
public:
    // 创建索引
    int create_index(const std::string& name, IndexType type, 
                     int dimension, const IndexParameters& params = {});
    
    // 注册外部索引
    int register_index(const std::string& name, std::shared_ptr<Index> index);
    
    // 插入向量
    bool insert(int index_id, std::unique_ptr<VectorRecord> record);
    
    // 查询 TopK
    std::vector<std::shared_ptr<const VectorRecord>> 
        query(int index_id, const VectorRecord& query, int k);
    
    // 范围查询（用于 Join）
    std::vector<std::shared_ptr<const VectorRecord>> 
        query_for_join(int index_id, const VectorRecord& query, double threshold);
};
```

---

## 4. Join 执行流程

### 4.1 完整生命周期

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        Phase 1: 构建阶段                                    │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  用户代码:                                                                  │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │ auto left = make_shared<TestVectorStreamSource>("left", data);     │   │
│  │ auto right = make_shared<TestVectorStreamSource>("right", data);   │   │
│  │                                                                    │   │
│  │ left->join(right, join_func, "ivf_eager", 0.8, 4)                  │   │
│  │     ->writeSink(sink_func, 1);                                     │   │
│  │                                                                    │   │
│  │ env.addStream(left);                                               │   │
│  │ env.addStream(right);                                              │   │
│  │ env.execute();                                                     │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                    │                                       │
│                                    ▼                                       │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                    StreamEnvironment::execute()                    │   │
│  │                                                                    │   │
│  │  1. 为每个 Source 分配 slotId                                       │   │
│  │     left_source  → slot = 0                                        │   │
│  │     right_source → slot = 1                                        │   │
│  │                                                                    │   │
│  │  2. 创建 Planner                                                   │   │
│  │     planner = Planner(concurrency_manager)                         │   │
│  │                                                                    │   │
│  │  3. 构建执行图                                                      │   │
│  │     planner.planToExecutionGraph(stream, graph, parallelism)       │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                    │                                       │
│                                    ▼                                       │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                    Planner::buildOperatorChain()                   │   │
│  │                                                                    │   │
│  │  Stream DAG → Operator Chain:                                      │   │
│  │                                                                    │   │
│  │  DataStreamSource → OutputOperator                                 │   │
│  │  JoinStream       → JoinOperator                                   │   │
│  │  SinkStream       → SinkOperator                                   │   │
│  │                                                                    │   │
│  │  对于 JoinOperator:                                                │   │
│  │    - 解析 method: "ivf_eager" → algo=IVF, is_eager=true            │   │
│  │    - 创建索引: create_index("left_ivf"), create_index("right_ivf") │   │
│  │    - 创建 JoinMethod: IVFMethod(config)                            │   │
│  │    - 设置 slots: setSlots(0, 1)                                    │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                    │                                       │
│                                    ▼                                       │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                   ExecutionGraph::buildGraph()                     │   │
│  │                                                                    │   │
│  │  1. 为每个算子创建 ExecutionVertex (按并行度)                        │   │
│  │                                                                    │   │
│  │     JoinOperator (parallelism=4):                                  │   │
│  │       Vertex[0] with RuntimeContext(0, 4)                          │   │
│  │       Vertex[1] with RuntimeContext(1, 4)                          │   │
│  │       Vertex[2] with RuntimeContext(2, 4)                          │   │
│  │       Vertex[3] with RuntimeContext(3, 4)                          │   │
│  │                                                                    │   │
│  │  2. 创建队列连接                                                    │   │
│  │     根据 ConnectionStrategy 创建队列并配置 InputGate/ResultPartition│   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│                        Phase 2: 执行阶段                                    │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                   ExecutionGraph::start()                          │   │
│  │                                                                    │   │
│  │  为每个 ExecutionVertex 启动独立线程:                               │   │
│  │                                                                    │   │
│  │  Thread-0: Source[0].run()                                         │   │
│  │  Thread-1: Source[1].run()                                         │   │
│  │  Thread-2: Join[0].run()                                           │   │
│  │  Thread-3: Join[1].run()                                           │   │
│  │  Thread-4: Join[2].run()                                           │   │
│  │  Thread-5: Join[3].run()                                           │   │
│  │  Thread-6: Sink[0].run()                                           │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                   ExecutionVertex::run() 循环                      │   │
│  │                                                                    │   │
│  │  void ExecutionVertex::run() {                                     │   │
│  │      // 1. 初始化算子                                               │   │
│  │      operator_->open(context_);                                    │   │
│  │                                                                    │   │
│  │      // 2. 主循环                                                   │   │
│  │      while (!stopped_) {                                           │   │
│  │          // 从输入队列读取数据                                       │   │
│  │          auto data = input_gate_->read();                          │   │
│  │          if (!data) continue;                                      │   │
│  │                                                                    │   │
│  │          // 处理数据                                                │   │
│  │          operator_->apply(std::move(data->record),                 │   │
│  │                          data->slot, collector_, context_);        │   │
│  │                                                                    │   │
│  │          // 发送结果到下游                                          │   │
│  │          for (auto& result : collector_.results()) {               │   │
│  │              result_partition_->emit(std::move(result), slot);     │   │
│  │          }                                                         │   │
│  │      }                                                             │   │
│  │                                                                    │   │
│  │      // 3. 清理                                                     │   │
│  │      operator_->close();                                           │   │
│  │  }                                                                 │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 JoinOperator 数据处理流程

```
┌────────────────────────────────────────────────────────────────────────────┐
│                   JoinOperator::apply() 详细流程                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  输入: Response&& record, int slot, Collector& collector,                  │
│        RuntimeContext& context                                             │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ Step 1: 确定数据来源                                                 │  │
│  │                                                                     │  │
│  │   if (slot == left_slot_id_) {                                      │  │
│  │       // 来自左流                                                    │  │
│  │       current_side = LEFT;                                          │  │
│  │       opposite_side = RIGHT;                                        │  │
│  │   } else {                                                          │  │
│  │       // 来自右流                                                    │  │
│  │       current_side = RIGHT;                                         │  │
│  │       opposite_side = LEFT;                                         │  │
│  │   }                                                                 │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                       │
│                                    ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ Step 2: 更新窗口状态                                                 │  │
│  │                                                                     │  │
│  │   size_t my_partition = context.getSubtaskIndex();                  │  │
│  │                                                                     │  │
│  │   // 添加到当前侧窗口                                                 │  │
│  │   current_state_->addRecord(record.clone(), my_partition);          │  │
│  │                                                                     │  │
│  │   // 插入到索引                                                      │  │
│  │   concurrency_manager_->insert(current_index_id_, record.clone());  │  │
│  │                                                                     │  │
│  │   // 清理过期记录                                                    │  │
│  │   current_state_->evictExpired(now, window_size_, my_partition);    │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                       │
│                                    ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ Step 3: 执行 Join 查询                                               │  │
│  │                                                                     │  │
│  │   // 使用 JoinMethod 在对侧窗口中搜索候选                             │  │
│  │   auto candidates = join_method_->ExecuteEager(*record, slot);      │  │
│  │                                                                     │  │
│  │   // ExecuteEager 内部:                                             │  │
│  │   //   1. 查询对侧索引获取近似候选                                    │  │
│  │   //   2. 计算精确距离                                               │  │
│  │   //   3. 过滤不满足阈值的候选                                        │  │
│  │   //   4. 验证候选是否仍在窗口中                                      │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                       │
│                                    ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ Step 4: 生成 Join 结果                                               │  │
│  │                                                                     │  │
│  │   for (auto& candidate : candidates) {                              │  │
│  │       // 调用 JoinFunction 合并左右记录                              │  │
│  │       auto result = join_function_->join(record, candidate);        │  │
│  │                                                                     │  │
│  │       if (result != nullptr) {                                      │  │
│  │           // 收集结果，发送到下游                                     │  │
│  │           collector.collect(std::move(result));                     │  │
│  │       }                                                             │  │
│  │   }                                                                 │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. 数据流图解

### 5.1 双流 Join 数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          双流 Join 数据流                                    │
└─────────────────────────────────────────────────────────────────────────────┘

     Left Source                                        Right Source
         │                                                   │
         │ VectorRecord                                      │ VectorRecord
         │ {uid, timestamp, vector[D]}                       │ {uid, timestamp, vector[D]}
         ▼                                                   ▼
    ┌─────────┐                                         ┌─────────┐
    │Output   │                                         │Output   │
    │Operator │                                         │Operator │
    │ slot=0  │                                         │ slot=1  │
    └────┬────┘                                         └────┬────┘
         │                                                   │
         │ Response{slot=0, record}                          │ Response{slot=1, record}
         ▼                                                   ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                        Queue System                          │
    │  ┌─────────┐                               ┌─────────┐      │
    │  │  Q_L0   │  ← 左流数据                    │  Q_R0   │      │
    │  │  Q_L1   │                               │  Q_R1   │  ← 右流数据
    │  └─────────┘                               └─────────┘      │
    └────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                     JoinOperator[0..N]                       │
    │                                                              │
    │  ┌──────────────────────────────────────────────────────┐   │
    │  │              InputGate (轮询 Q_L*, Q_R*)              │   │
    │  └──────────────────────────┬───────────────────────────┘   │
    │                             │                                │
    │                             ▼                                │
    │  ┌──────────────────────────────────────────────────────┐   │
    │  │                 apply(record, slot)                   │   │
    │  │                                                       │   │
    │  │   if slot == 0:      ┌─────────────┐                  │   │
    │  │     左流记录到达 ──→  │ Left State  │ (添加到窗口)     │   │
    │  │     在右侧查询    ←── │ Left Index  │                  │   │
    │  │                      └─────────────┘                  │   │
    │  │                                                       │   │
    │  │   if slot == 1:      ┌─────────────┐                  │   │
    │  │     右流记录到达 ──→  │ Right State │ (添加到窗口)     │   │
    │  │     在左侧查询    ←── │ Right Index │                  │   │
    │  │                      └─────────────┘                  │   │
    │  └──────────────────────────┬───────────────────────────┘   │
    │                             │                                │
    │                             ▼                                │
    │  ┌──────────────────────────────────────────────────────┐   │
    │  │              JoinMethod::ExecuteEager()               │   │
    │  │                                                       │   │
    │  │   1. 索引查询: index.query(record, threshold)         │   │
    │  │   2. 距离验证: cosine_similarity(a, b) >= threshold   │   │
    │  │   3. 窗口验证: state.containsUid(candidate.uid)       │   │
    │  │   4. 返回候选: vector<VectorRecord>                   │   │
    │  └──────────────────────────┬───────────────────────────┘   │
    │                             │                                │
    │                             ▼                                │
    │  ┌──────────────────────────────────────────────────────┐   │
    │  │            JoinFunction::join(left, right)            │   │
    │  │                                                       │   │
    │  │   合并左右向量，生成输出记录                            │   │
    │  └──────────────────────────┬───────────────────────────┘   │
    │                             │                                │
    └─────────────────────────────┼────────────────────────────────┘
                                  │
                                  │ Response{joined_record}
                                  ▼
                         ┌──────────────┐
                         │ SinkOperator │
                         │  (输出结果)   │
                         └──────────────┘
```

### 5.2 窗口状态演进

```
时间轴: ─────────────────────────────────────────────────────────▶
        T1    T2    T3    T4    T5    T6    T7    T8    T9    T10

窗口大小: window_size = 5

Left Window State 演进:
┌─────────────────────────────────────────────────────────────────┐
│ T1: [L1]                                                        │
│ T2: [L1, L2]                                                    │
│ T3: [L1, L2, L3]                                                │
│ T4: [L1, L2, L3, L4]                                            │
│ T5: [L1, L2, L3, L4, L5]                                        │
│ T6: [L2, L3, L4, L5, L6]     ← L1 过期被移除                     │
│ T7: [L3, L4, L5, L6, L7]     ← L2 过期被移除                     │
│ ...                                                             │
└─────────────────────────────────────────────────────────────────┘

Right Window State 演进:
┌─────────────────────────────────────────────────────────────────┐
│ T1: [R1]                                                        │
│ T2: [R1, R2]                                                    │
│ ...                                                             │
└─────────────────────────────────────────────────────────────────┘

Join 发生时机:
┌─────────────────────────────────────────────────────────────────┐
│ T3: L3 到达                                                      │
│     → 在 Right Window [R1, R2] 中搜索相似向量                    │
│     → 找到 R2 满足 similarity(L3, R2) >= 0.8                     │
│     → 输出 Join(L3, R2)                                          │
│                                                                 │
│ T3: R3 到达                                                      │
│     → 在 Left Window [L1, L2, L3] 中搜索相似向量                 │
│     → 找到 L1, L3 满足阈值                                       │
│     → 输出 Join(R3, L1), Join(R3, L3)                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Join 方法实现指南

### 6.1 BaseMethod 接口

所有 Join 方法都需要继承 `BaseMethod` 并实现核心接口：

```cpp
class BaseMethod {
public:
    explicit BaseMethod(double join_similarity_threshold);
    virtual ~BaseMethod() = default;
    
    /**
     * @brief Eager 模式执行（必须实现）
     * 
     * 每条记录到达时立即执行查询，返回满足阈值的候选向量
     */
    virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot) = 0;
        
protected:
    double join_similarity_threshold_;
};
```

### 6.2 实现新 Join 方法的步骤

#### Step 1: 创建头文件

```cpp
// include/operator/join_operator_methods/my_method.h
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"
#include "execution/runtime_context.h"

namespace sageFlow {

class MyMethod final : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        // 添加你的配置参数
    };
    
    explicit MyMethod(const Config& config);
    explicit MyMethod(double threshold);
    
    ~MyMethod() override = default;
    
    std::string getName() const { return "MyMethod"; }
    
    // 初始化
    void open(const RuntimeContext& context,
              WindowState* left_state,
              WindowState* right_state);
    
    // 核心查询接口
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot) override;
    
    void close();
    
private:
    Config config_;
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    size_t subtask_index_ = 0;
};

}  // namespace sageFlow
```

#### Step 2: 实现源文件

```cpp
// src/operator/join_operator_methods/my_method.cpp
#include "operator/join_operator_methods/my_method.h"
#include "compute_engine/distance.h"

namespace sageFlow {

MyMethod::MyMethod(const Config& config)
    : BaseMethod(config.similarity_threshold)
    , config_(config) {}

MyMethod::MyMethod(double threshold)
    : MyMethod(Config{.similarity_threshold = threshold}) {}

void MyMethod::open(const RuntimeContext& context,
                    WindowState* left_state,
                    WindowState* right_state) {
    subtask_index_ = context.getSubtaskIndex();
    left_state_ = left_state;
    right_state_ = right_state;
    
    // 初始化你的数据结构
}

std::vector<std::unique_ptr<VectorRecord>> MyMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 确定对侧窗口
    WindowState* opposite_state = (query_slot == 0) ? right_state_ : left_state_;
    
    // 获取窗口快照（线程安全）
    auto candidates = opposite_state->getRecordsSnapshot(subtask_index_);
    
    // 遍历候选，计算相似度
    for (const auto& candidate : candidates) {
        double similarity = CosineSimilarity(
            query_record.getData().data(),
            candidate->getData().data(),
            query_record.getDimension()
        );
        
        if (similarity >= join_similarity_threshold_) {
            // 创建结果副本
            results.push_back(std::make_unique<VectorRecord>(*candidate));
        }
    }
    
    return results;
}

void MyMethod::close() {
    // 清理资源
}

}  // namespace sageFlow
```

#### Step 3: 注册到 JoinMethodRegistry

```cpp
// 在 my_method.cpp 末尾添加自动注册
#include "operator/join_method_registry.h"

namespace {
    static bool registered = []() {
        JoinMethodRegistry::instance().registerMethod(
            JoinAlgorithm::MY_METHOD,
            JoinMethodRegistry::MethodInfo{
                .name = "MyMethod",
                .description = "My custom join method",
                .algorithm = JoinAlgorithm::MY_METHOD,
                .supports_eager = true,
                .supports_lazy = false,
                .recommended_partition = PartitionStrategy::ROUND_ROBIN,
                .recommended_window_state = WindowStateType::SHARED,
                .paper_reference = "Your Paper (VLDB 2025)"
            },
            [](const JoinStrategyConfig& config,
               std::shared_ptr<ConcurrencyManager> cm,
               int dimension, int left_idx, int right_idx) {
                return std::make_unique<MyMethod>(config.similarity_threshold);
            }
        );
        return true;
    }();
}
```

#### Step 4: 添加单元测试

```cpp
// test/UnitTest/test_my_method.cpp
#include <gtest/gtest.h>
#include "operator/join_operator_methods/my_method.h"
#include "state/shared_window_state.h"

class MyMethodTest : public ::testing::Test {
protected:
    void SetUp() override {
        left_state_ = std::make_unique<SharedWindowState>();
        right_state_ = std::make_unique<SharedWindowState>();
        
        method_ = std::make_unique<MyMethod>(0.8);
        
        RuntimeContext ctx(0, 1);
        method_->open(ctx, left_state_.get(), right_state_.get());
    }
    
    std::unique_ptr<SharedWindowState> left_state_;
    std::unique_ptr<SharedWindowState> right_state_;
    std::unique_ptr<MyMethod> method_;
};

TEST_F(MyMethodTest, BasicJoin) {
    // 添加测试数据
    auto left_record = createTestVector(128, 1);
    auto right_record = createTestVector(128, 2);
    
    left_state_->addRecord(std::move(left_record), 0);
    right_state_->addRecord(std::move(right_record), 0);
    
    // 执行查询
    auto results = method_->ExecuteEager(*query, 0);
    
    // 验证结果
    EXPECT_GE(results.size(), 0);
}
```

---

## 7. Baseline 复现指南

### 7.1 已实现的 Baseline

| 算法 | 类名 | 论文 | 状态 |
|-----|------|------|------|
| BruteForce | `BruteForceBaseline` | Ground Truth | ✅ 完成 |
| IVF | `IVFMethod` | Faiss | ✅ 完成 |
| HNSW | `HnswMethod` | HNSW Paper | ✅ 完成 |
| S3J | `S3JMethod` | DEBS'23 | ✅ 完成 |
| HDR-Tree | `HdrTreeMethod` | HDR-Tree Paper | 🚧 部分完成 |
| ClusteredJoin | `ClusteredJoinMethod` | VectraFlow | 🚧 部分完成 |

### 7.2 复现 Baseline 的推荐步骤

#### 1. 理解论文算法

- 阅读原论文，理解核心算法
- 确定算法的关键参数
- 分析时间/空间复杂度

#### 2. 选择合适的策略组合

使用 `JoinStrategyConfig::inferDefaults()` 获取推荐配置：

```cpp
JoinStrategyConfig config;
config.algorithm = JoinAlgorithm::S3J;
config.inferDefaults();  // 自动设置推荐的分区和窗口策略

// 推荐结果:
// - partition_strategy: CENTROID
// - window_state_type: PARTITIONED_VECTOR
// - index_strategy: PARTITIONED
```

#### 3. 实现核心组件

参考现有实现的目录结构：

```
include/operator/join_operator_methods/
├── s3j_method.h                 # 主类头文件
└── s3j_components/              # 辅助组件
    ├── adaptive_partitioner.h   # 自适应分区器
    └── adaptive_index_selector.h # 索引选择器

src/operator/join_operator_methods/
├── s3j_method.cpp
└── s3j_components/
    ├── adaptive_partitioner.cpp
    └── adaptive_index_selector.cpp
```

#### 4. 验证正确性

使用 BruteForce 作为 Ground Truth 验证召回率：

```cpp
TEST(S3JMethod, RecallTest) {
    // 生成测试数据
    auto test_data = TestDataGenerator::generate(1000, 128);
    
    // 运行 BruteForce 获取 Ground Truth
    auto gt_results = runBruteForce(test_data, threshold);
    
    // 运行 S3J
    auto s3j_results = runS3J(test_data, threshold);
    
    // 计算召回率
    double recall = computeRecall(gt_results, s3j_results);
    EXPECT_GE(recall, 0.95);  // 期望召回率 >= 95%
}
```

### 7.3 策略兼容性速查表

| 分区策略 | 兼容的窗口状态 | 说明 |
|---------|---------------|------|
| ROUND_ROBIN | Shared | 随机分发需要共享状态 |
| KEY_HASH | Partitioned, Shared | 基于 key 分区 |
| VECTOR_HASH | Partitioned | 相似向量聚集到同一分区 |
| LSH | PartitionedVector | VSJoin 专用 |
| CENTROID | PartitionedVector | S3J 专用 |

**⚠️ 不兼容的组合会导致召回率下降：**
- ❌ ROUND_ROBIN + PartitionedWindowState → 跨分区匹配丢失
- ❌ VSJoin + SharedWindowState → 架构不支持

---

## 8. 配置系统

### 8.1 配置文件结构

**位置**: `config/join_strategies.toml`

```toml
# 全局默认配置
[defaults]
dimension = 128
window_size_ms = 10000
similarity_threshold = 0.8

# BruteForce 策略
[strategies.bruteforce]
algorithm = "bruteforce"
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "none"

# IVF 策略
[strategies.ivf]
algorithm = "ivf"
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "ivf"
ivf_nlist = 100
ivf_nprobes = 10

# S3J 策略
[strategies.s3j]
algorithm = "s3j"
partition_strategy = "centroid"
window_state_type = "partitioned_vector"
index_strategy = "partitioned"
s3j_num_centroids = 16
s3j_enable_adaptive = true

# VSJoin 策略
[strategies.vsjoin]
algorithm = "vsjoin"
partition_strategy = "lsh"
window_state_type = "partitioned_vector"
index_strategy = "partitioned"
vsjoin_num_hash_functions = 8
vsjoin_boundary_threshold = 0.1
```

### 8.2 加载配置

```cpp
// 加载默认策略
auto config = loadJoinStrategyConfig("config/join_strategies.toml");

// 加载特定策略
auto s3j_config = loadJoinStrategyConfig(
    "config/join_strategies.toml", 
    "s3j"
);

// 验证配置
JoinConfigValidator validator;
auto result = validator.validate(config);
if (!result.valid) {
    for (const auto& error : result.errors) {
        LOG_ERROR("Config error: {}", error);
    }
}
```

### 8.3 使用工厂创建组件

```cpp
// 使用 JoinStrategyFactory 创建完整策略
JoinStrategyFactory factory(concurrency_manager);
auto components = factory.create(config);

// components 包含:
// - join_method: 已初始化的 JoinMethod
// - left_state: 左流窗口状态
// - right_state: 右流窗口状态
// - partitioner: 数据分区器
// - left_index_id / right_index_id: 索引 ID
```

---

## 9. 测试与调试

### 9.1 运行测试

```bash
# 编译
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build -j$(nproc)

# 运行所有单元测试
ctest --test-dir build --output-on-failure -L UNIT

# 运行特定测试
./build/bin/test_join_strategy_factory

# 运行集成测试
ctest --test-dir build --output-on-failure -L INTEGRATION

# 运行性能测试
./build/bin/perf_join_with_datasource --config config/perf_join.toml
```

### 9.2 日志调试

```cpp
#include "utils/logging.h"

// 使用不同级别的日志
SAGEFLOW_LOG_DEBUG("JOIN", "Processing record uid={}", record.getUid());
SAGEFLOW_LOG_INFO("JOIN", "Window size: {}", window_state_->size());
SAGEFLOW_LOG_WARN("JOIN", "High latency detected: {}ms", latency);
SAGEFLOW_LOG_ERROR("JOIN", "Index query failed: {}", error_msg);
```

配置日志级别（在代码中或配置文件中）：

```cpp
// 设置全局日志级别
spdlog::set_level(spdlog::level::debug);
```

### 9.3 性能分析

启用 profiling 收集性能指标：

```cpp
auto join_op = std::make_shared<JoinOperator>(
    join_func,
    concurrency_manager,
    "ivf_eager",
    0.8,
    true,  // enable_profiling
    "profile_output.json"  // output path
);
```

输出的性能指标包括：
- 平均查询延迟
- 吞吐量 (QPS)
- 索引查询时间
- 窗口更新时间
- 召回率估计

### 9.4 常见问题排查

#### 问题 1: 召回率低于预期

```
检查清单:
□ 分区策略与窗口状态是否兼容？
□ 相似度阈值是否设置正确？
□ 窗口大小是否足够？
□ 索引参数是否合理（如 IVF 的 nprobes）？
```

#### 问题 2: 内存占用过高

```
检查清单:
□ 窗口大小是否过大？
□ 是否及时清理过期记录？
□ 索引是否需要重建？
□ 是否有内存泄漏（使用 Valgrind 检测）？
```

#### 问题 3: 死锁或线程阻塞

```
检查清单:
□ 锁的获取顺序是否一致？
□ 是否存在循环依赖？
□ 队列是否正确关闭？
□ 使用 TSAN 检测数据竞争
```

---

## 附录

### A. 关键文件路径速查

| 组件 | 头文件 | 源文件 |
|------|--------|--------|
| JoinOperator | `include/operator/join_operator.h` | `src/operator/join_operator.cpp` |
| BaseMethod | `include/operator/join_operator_methods/base_method.h` | - |
| BruteForce | `include/operator/join_operator_methods/bruteforce_baseline.h` | `src/operator/join_operator_methods/bruteforce_baseline.cpp` |
| IVFMethod | `include/operator/join_operator_methods/ivf_method.h` | `src/operator/join_operator_methods/ivf_method.cpp` |
| S3JMethod | `include/operator/join_operator_methods/s3j_method.h` | `src/operator/join_operator_methods/s3j_method.cpp` |
| WindowState | `include/state/window_state.h` | - |
| ExecutionGraph | `include/execution/execution_graph.h` | `src/execution/execution_graph.cpp` |
| RuntimeContext | `include/execution/runtime_context.h` | - |
| JoinStrategyConfig | `include/operator/join_strategy_config.h` | `src/operator/join_strategy_config.cpp` |
| JoinStrategyFactory | `include/operator/join_strategy_factory.h` | `src/operator/join_strategy_factory.cpp` |

### B. 术语表

| 术语 | 含义 |
|------|------|
| Slot | 数据流标识，区分左流(0)和右流(1) |
| Subtask | 并行实例，每个算子可以有多个 subtask |
| WindowState | 窗口内的记录状态，支持分区和共享两种模式 |
| Eager Mode | 每条记录到达时立即执行查询 |
| Lazy Mode | 批量记录累积后统一执行查询（已弃用） |
| Partitioner | 数据分区器，决定数据发往哪个下游实例 |
| Ground Truth | 精确结果，通常由 BruteForce 产生 |
| Recall | 召回率，近似方法找到的正确结果占全部正确结果的比例 |

### C. 参考文献

1. **Faiss**: Johnson, J., Douze, M., & Jégou, H. (2019). Billion-scale similarity search with GPUs.
2. **HNSW**: Malkov, Y. A., & Yashunin, D. A. (2018). Efficient and robust approximate nearest neighbor search using hierarchical navigable small world graphs.
3. **S3J**: DEBS'23 - Adaptive Distributed Streaming Similarity Join
4. **HDR-Tree**: High-Dimensional R-Tree for Streaming Data

---

> **文档维护者**: SageFlow Team  
> **最后更新**: 2024-12


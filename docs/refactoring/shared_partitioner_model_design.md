# 分区器共享模型重构方案（Shared Partitioner Model）

**作者**：Cascade

**日期**：2026-01-29

**状态**：提案（Proposed）

> 本文档用于指导一次“一步到位”的框架级重构：
> - 解决高并行度下 `CentroidPartitioner` 等**有状态分区器**多实例导致的**训练/广播状态不一致**问题；
> - 用 **RCU（Read-Copy-Update）快照**与**后台训练线程**实现“正确性一致 + 数据路径无锁/低锁”；
> - 统一 `ResultPartition` 的分发语义，避免冷启动阶段全广播导致的性能灾难；
> - 确保 RoundRobin/KeyHash/VectorHash/LSH 等分区器功能不受影响，并通过单元测试与集成测试。

---

## 1. 背景与问题定义

当前框架中，连接创建路径（`ExecutionGraph::createConnections()`）会为**每个 upstream ExecutionVertex**调用一次下游算子的 `getPreferredPartitioner()`，并将返回的 `std::unique_ptr<IPartitioner>` 移入对应的 `ResultPartition`。

对 **无状态分区器**（如 RoundRobin/KeyHash/VectorHash/LSH）这通常没问题。

但对 **有状态分区器**（如 `CentroidPartitioner`）会产生严重问题：

1. **状态不一致（正确性灾难）**
   - 每个 upstream vertex 都拥有一个独立的 `CentroidPartitioner` 实例
   - 其内部存在训练状态：`trained_`、`centroids_`、`training_buffer_`、`sample_count_`、`training_triggered_`
   - 因此同一条边上会出现“部分实例已训练进入单播分区、部分实例仍在冷启动广播”的混合态
   - 对于 VSJoin（“只查本地、不跨分区探测”）来说，会破坏“相似向量必须在同一分区相遇”的前提，导致 recall 随并行度断崖式下降。

2. **冷启动全广播（性能灾难）**
   - 当前 `ResultPartition::emit()` 在 `partitioner_->isBroadcast()` 为 true 时会把同一条数据复制到所有下游通道
   - 高并行度下复制与 `sink_dedup` 开销巨大，吞掉二级索引的性能收益。

3. **训练触发与更新不可控**
   - 训练由每个实例独立收样本并触发，无法保证全局一致
   - 训练计算发生在数据路径上会阻塞或导致竞争

---

## 2. 设计目标（验收标准）

### 2.1 正确性（Correctness）
- **同一条连接（edge）上**，所有 upstream vertex 的分区语义一致：
  - 训练状态一致
  - 质心/模型一致
  - 冷启动策略一致
- 不允许再出现“同一 edge 上部分广播、部分单播”的混合态。

### 2.2 性能（Performance）
- 数据路径上的 `partition()` / `partitionMulti()`：
  - 不引入全局大锁
  - 允许一次原子读（RCU）+ 纯计算
- 训练/模型更新：
  - 在后台线程完成
  - 更新以 RCU 快照切换实现（读不阻塞）

### 2.3 兼容性（Compatibility）
- RoundRobin/KeyHash/VectorHash/LSH 行为保持不变
- ClusteredJoin/S3J/VSJoin 等依赖 Centroid 分区的算法功能不回退

---

## 3. 核心思想：状态（Model）与逻辑（Partitioner）分离

将分区器拆为两部分：

- **IPartitionerModel（共享状态）**：
  - 训练样本缓冲
  - 训练状态（ready/trained）
  - 不可变模型快照（centroids snapshot）
  - 后台训练线程

- **IPartitioner（轻量代理）**：
  - 每个 upstream vertex 仍可拥有一个分区器对象（保持框架结构不变）
  - 但这些对象共享同一个 model
  - 数据路径只读 model 快照，无锁/低锁

### 3.1 RCU 快照（Read-Copy-Update）

- model 维护：
  - `std::atomic<std::shared_ptr<const Snapshot>> snapshot_`
- 读路径：
  - `auto snap = snapshot_.load(std::memory_order_acquire);`
  - 纯计算
- 写路径（训练完成/更新模型）：
  - 构建新 snapshot
  - `snapshot_.store(new_snap, std::memory_order_release);`

---

## 4. 新增组件：`CentroidPartitionerModel`

### 4.1 新文件
- `include/execution/partitioner_model.h`
- `src/execution/partitioner_model.cpp`

### 4.2 接口草案（中文注释版）

```cpp
class IPartitionerModel {
public:
    virtual ~IPartitionerModel() = default;
    virtual bool isReady() const = 0;
};

class CentroidPartitionerModel : public IPartitionerModel {
public:
    using Centroids = std::vector<std::vector<float>>;
    using CentroidsSnapshot = const Centroids;

    explicit CentroidPartitionerModel(const CentroidPartitioner::Config& cfg);
    ~CentroidPartitionerModel();

    // 数据路径：轻量采样写入（可降采样）
    void addTrainingSample(const VectorRecord& record);

    // 数据路径：只读快照（RCU）
    std::shared_ptr<CentroidsSnapshot> getCentroidsSnapshot() const;

    bool isReady() const override;

private:
    void trainingLoop();
    void triggerTrainingOnce();

    CentroidPartitioner::Config config_;

    std::atomic<bool> trained_{false};
    std::atomic<bool> training_triggered_{false};

    // RCU：质心快照
    std::atomic<std::shared_ptr<CentroidsSnapshot>> centroids_snapshot_;

    // 样本收集缓冲（可用 mutex 或 lock-free 队列；第一版可用 mutex + 降采样）
    std::mutex buffer_mutex_;
    std::vector<std::vector<float>> training_buffer_;
    std::atomic<size_t> sample_count_{0};

    // 后台训练线程
    std::unique_ptr<std::thread> training_thread_;
    std::atomic<bool> stop_{false};
};
```

### 4.3 训练策略
- **触发条件**（可配置）：
  - 样本数达到阈值（如 1000）
  - 或者时间窗口达到阈值（如 window_size 的 10%）
- **执行方式**：后台线程训练 KMeans（复用现有 CentroidPartitioner 的训练实现或抽出工具类）
- **切换方式**：训练结束后通过 `centroids_snapshot_.store(...)` 原子替换

---

## 5. 重构 `CentroidPartitioner`：从“持状态”改为“代理”

### 5.1 修改目标
`CentroidPartitioner` 不再维护 `trained_/centroids_/training_buffer_` 等状态，改为：
- 持有 `std::shared_ptr<CentroidPartitionerModel> model_`
- `partition/partitionMulti/isBroadcast` 都从 model 读取状态

### 5.2 关键行为变更
- `partition()`：
  - 调用 `model_->addTrainingSample()`（轻量）
  - 若 `model_->isReady()`：使用快照算主分区
  - 若未 ready：按冷启动策略返回目标（见第 8 节）
- `partitionMulti()`：
  - 若 ready：计算 kNN 质心 / overlap 逻辑
  - 若未 ready：按冷启动策略返回多个目标或单目标

---

## 6. ExecutionGraph 改造：按连接创建并缓存共享模型

### 6.1 问题点
当前在 `ExecutionGraph::createConnections()` 中，每个 upstream vertex 都调用一次 `downstream_op->getPreferredPartitioner()`，导致模型多实例。

### 6.2 新机制
- 引入 `ConnectionKey = (upstream_op*, downstream_op*, slot)`
- 维护 `connection_models_`：每条连接只创建一个 model

伪代码：
```cpp
if (strategy == CENTROID) {
  auto key = (upstream, downstream, slot);
  if (!connection_models_.contains(key)) {
    connection_models_[key] = std::make_shared<CentroidPartitionerModel>(cfg);
  }
}

for each upstream vertex i:
  auto p = createPartitionerProxy(cfg, connection_models_[key]);
  setupResultPartition(..., std::move(p));
```

> 注意：RoundRobin 仍可每 vertex 创建独立实例（其计数器语义合理）。

---

## 7. ResultPartition 改造：统一分发入口，移除隐式广播分支

### 7.1 现状
`ResultPartition::emit()` 目前会：
- 先 `partition()` 收样本
- 再根据 `isBroadcast()` 决定是否全广播
- 否则才走 multicast 或单播

这导致：
- 同一条 record 可能多次调用 `partition()`
- 广播/多播/单播逻辑分叉在 ResultPartition 层，语义混乱

### 7.2 新目标
- 如果 `supportsMulticast()`：**只调用一次 `partitionMulti()`** 来获取目标集合
- 否则：调用 `partition()` 获取单目标

冷启动期是否“广播/多播/发0”应由分区器（背后的共享 model）统一决定，而不是 ResultPartition 通过 `isBroadcast()` 隐式决定。

---

## 8. 冷启动策略（可配置且一致）

考虑你提出的偏好“冷启动阶段发到 0 号线程也可以”，我们将冷启动策略做成**显式配置**，并保证所有 upstream vertex 一致。

### 8.1 冷启动模式选项
- `SINGLE_0`：训练前全部发到 channel 0（正确性强、性能差）
- `CONTROLLED_MULTICAST(k)`：训练前发到固定 k 个 channel（推荐默认：k=2/4）
- `BROADCAST_ALL`：训练前全广播（正确性强、性能最差，通常不推荐作为默认）

> 说明：在现有 `CentroidPartitioner` 语义中，`multicast_k` 有两种工作模式：
> - `multicast_k >= 1`：固定多播到最近的 k 个质心（k=1 等价单播）
> - `multicast_k == 0`：进入 **overlap_ratio 阈值模式**（基于距离相对差 `ratio=(dist_i-min_dist)/min_dist`，若 `ratio < overlap_ratio` 则复制到分区 i）。
> 
> 重构后必须在文档/配置中明确该语义，并保证该逻辑在共享 model 快照上一致执行。

### 8.2 配置入口
- 在 `JoinStrategyConfig` 增加 centroid/clustered/vsjoin 相关冷启动策略字段
- 或者在 `CentroidPartitioner::Config` 中增加 `cold_start_mode` 与 `cold_start_multicast_k`

---

## 9. 与现有 PartitionerFactory 的关系

当前 `PartitionerFactory` 已能基于 `PartitionStrategy` 创建：
- RoundRobin
- KeyHash
- VectorHash
- LSH（`LSHIPartitioner`）
- Centroid（`CentroidPartitioner`）

重构后：
- `PartitionerFactory` 仍负责创建 `IPartitioner`（proxy）
- 但对 CENTROID 会额外注入共享 model
- 其余分区器保持原样，不受影响

---

## 10. 实施步骤（里程碑式）

### M1：基础设施落地（可编译）
1. 新增 `partitioner_model.h/.cpp`
2. 实现 `CentroidPartitionerModel`（最小可用：样本收集 + 后台训练 + RCU 快照）
3. `CentroidPartitioner` 改为 proxy（编译通过，功能暂时等价）

### M2：ExecutionGraph 引入共享 model
1. 给 `ExecutionGraph` 增加 `connection_models_`
2. 在 createConnections 中对 CENTROID 连接创建共享 model
3. 每个 upstream vertex 创建 proxy，但共享同一个 model

### M3：ResultPartition 统一分发入口
1. 移除 `isBroadcast()` 分支（或改成兼容模式开关）
2. 统一 `partitionMulti()`/`partition()` 调用路径

### M4：冷启动策略显式化
1. 增加 config 字段
2. 冷启动默认策略设为 `CONTROLLED_MULTICAST(2/4)` 或按你的偏好 `SINGLE_0`
3. 通过日志确认不会再出现全广播风暴

### M5：清理旧接口与兼容
1. 逐步废弃 `Operator::getPreferredPartitioner()` 或保持兼容但不再依赖
2. 保持 RoundRobin 等无状态分区器行为完全一致

---

## 11. 测试与验收

### 11.1 单元测试（必须通过）
- `test_clustered_partitioner`
- `test_multicast_partitioner`
- `test_centroid_cold_start`
- 新增（建议）：
  - `test_shared_partitioner_model_consistency`：多个 proxy 共享同一 model，训练切换一致
  - `test_result_partition_no_broadcast_branch`：冷启动策略由 partitioner 决定，ResultPartition 只按目标集合分发

### 11.2 集成测试（必须通过）
- `vsjoin_parallelism_scaling`：
  - `p=8/16/24/32` recall 不再断崖式下降（建议阈值：>=0.90，理想 >=0.95）
  - `sink_dedup` 不应随 p 指数爆炸（应明显低于全广播策略）
- ClusteredJoin/S3J 相关集成用例（若当前开启）

### 11.3 验收标准（明确可量化）
- **正确性**：
  - `vsjoin_parallelism_scaling` 在 `p=32` 不低于 `expected_min_recall`
  - 不再出现“同一条边上部分广播部分单播”的状态混合（通过日志/断言验证）
- **性能**：
  - 分区路径不引入 per-record 全局互斥锁
  - 冷启动不再全广播（除非显式配置 BROADCAST_ALL）

---

## 12. 风险与回滚策略

- 风险：接口改造影响面较大（ExecutionGraph/ResultPartition/partitioner_factory/operator）
- 回滚策略：
  - 保留旧逻辑开关（如环境变量 `SAGEFLOW_PARTITIONER_LEGACY=1`）在紧急情况下回退
  - 分阶段合并（M1~M5），每阶段保持编译与核心测试通过

---

## 13. 备注：当前冷启动行为的事实依据

当前实现中，`ResultPartition::emit()` 在 `partitioner_->isBroadcast()` 为 true 时会**向所有通道广播**，而不是发到 0。

该行为需要在重构后由“冷启动策略”显式控制，避免高并行度性能灾难。

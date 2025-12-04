# VSJoin 流式向量相似性连接引擎 - 多线程架构重构与 Baseline 实现

## 📋 概述

本 PR 实现了 SageFlow 的 **VSJoin 流式向量相似性连接引擎**，包含完整的多线程架构重构和多种 Baseline 算法实现。这是一个大规模重构，包含 **195 个文件变更**，新增约 **50,000+ 行代码**。

---

## 🏗️ 架构变更

### 1. RuntimeContext 注入与并行执行模型

- **RuntimeContext**: 提供任务标识 (`subtask_index`) 和并行度 (`parallelism`) 信息
- **ExecutionVertex**: 每个并行实例持有独立的 `RuntimeContext`
- **Operator 接口**: `open()`, `apply()`, `close()` 方法增加 `RuntimeContext` 参数

### 2. 窗口状态抽象层

新增 `WindowState` 接口，支持多种状态实现：

| 状态类型 | 描述 | 适用场景 |
|---------|------|---------|
| `SharedWindowState` | 所有实例共享同一状态 | RoundRobin 分区 |
| `PartitionedWindowState` | 每个 subtask 独立状态 | Key/VectorHash 分区 |
| `TwoTierWindowState` | 两层架构（写友好+紧凑层） | 高吞吐场景 |
| `PartitionedVectorState` | 向量空间分区状态 | VSJoin/S3J |

### 3. 连接策略

统一的 SPSC 队列矩阵连接策略：

- **ConnectionStrategy**: 统一的连接策略，使用 `upstream × downstream` 个 SPSC 队列
- 支持不同的分区器（RoundRobin、KeyHash、VectorHash、LSH、Centroid）

---

## 🔧 核心组件

### Join 策略工厂模式 (新增)

```
JoinStrategyConfig (配置) 
    ↓
JoinConfigValidator (验证)
    ↓
JoinStrategyFactory (创建)
    ↓
StrategyComponents {
    JoinMethod,
    WindowState (left/right),
    Partitioner,
    Index (shared/partitioned),
    VSJoin/S3J 专用组件
}
```

#### 新增文件：
- `include/operator/join_strategy_config.h` - 统一配置结构
- `include/operator/join_strategy_factory.h` - 策略工厂
- `include/operator/join_method_registry.h` - 方法注册中心
- `include/operator/join_config_validator.h` - 配置验证器
- `include/execution/partitioner_factory.h` - 分区器工厂
- `include/state/window_state_factory.h` - 状态工厂
- `config/join_strategies.toml` - 策略配置文件

### 向量空间分区器

- **VectorSpacePartitioner**: 抽象接口
  - `LSHPartitioner`: 局部敏感哈希分区 (VSJoin)
  - `KMeansPartitioner`: K-Means 聚类分区
  - `CentroidPartitioner`: 质心分区 (S3J)

### 并发与索引管理

- **ConcurrencyManager**: 线程安全的索引管理
- **ConcurrencyController**: 索引级别的并发控制
- **PartitionedIndex**: 向量空间分区索引

### VSJoin 专用组件

- `PartitionCoordinator`: 跨分区协调
- `BoundaryTracker`: 边界向量追踪
- `AsyncCandidateGenerator`: 异步候选生成
- `LateArrivalHandler`: 延迟数据处理
- `DistanceVerifier`: 距离验证

---

## 📊 支持的 Join 算法

| 算法 | 类型 | 分区策略 | 窗口状态 | 说明 |
|-----|------|---------|---------|------|
| **BruteForce** | Baseline | RoundRobin | Shared | Ground Truth |
| **IVF** | Approximate | RoundRobin/KeyHash | Shared/Partitioned | 倒排索引 |
| **HNSW** | Approximate | RoundRobin | Shared | 分层可导航图 |
| **S3J** | Baseline | Centroid | PartitionedVector | DEBS'23 |
| **ClusteredJoin** | Baseline | Centroid | PartitionedVector | VectraFlow |
| **HDRTree** | Baseline | VectorHash | Partitioned | HDR-Tree |
| **VSJoin** | Our Method | LSH | PartitionedVector | 本项目核心方法 |

---

## 🧪 测试覆盖

### 新增单元测试 (19 个测试文件)

- `test_join_strategy_factory.cpp` - 策略工厂测试
- `test_join_config_validator.cpp` - 配置验证测试
- `test_join_method_registry.cpp` - 注册中心测试
- `test_partitioner_factory.cpp` - 分区器工厂测试
- `test_window_state_factory.cpp` - 状态工厂测试
- `test_window_state.cpp` - 窗口状态基础测试
- `test_two_tier_window_state.cpp` - 两层状态测试
- `test_partitioned_vector_state.cpp` - 分区向量状态测试
- `test_vector_space_partitioner.cpp` - 空间分区器测试
- `test_partitioned_index.cpp` - 分区索引测试
- `test_partition_coordinator.cpp` - 协调器测试
- `test_late_arrival_handler.cpp` - 延迟处理测试
- `test_join_operator_state.cpp` - Join 状态测试
- `test_bruteforce_method.cpp` / `test_ivf_method.cpp` / `test_s3j_method.cpp` - 方法测试
- `test_pca.cpp` / `test_simd_distance.cpp` - 计算优化测试

### 集成测试

- `test_join_pipeline.cpp` - 端到端 Join 流水线测试
- `test_join_datasource_modes.cpp` - 多并行度召回率测试

### 性能测试

- `perf_join_with_datasource.cpp` - 数据源性能测试
- 支持不同数据集模式的性能评估

---

## 📚 文档更新

- `docs/ARCHITECTURE_REFACTORING.md` - 架构重构指南
- `docs/CONNECTION_STRATEGIES.md` - 连接策略详解
- `docs/JOIN_PIPELINE_GUIDE.md` - Join 流水线指南
- `docs/VSJOIN_IMPLEMENTATION_ROADMAP.md` - VSJoin 实现路线图
- `.github/copilot-instructions.md` - AI 辅助开发指南

---

## 🔧 最新修复 (Latest Fixes)

### 统一连接策略为 SPSC 矩阵模式

重构连接策略，使用统一的 SPSC (Single-Producer Single-Consumer) 队列矩阵：

- **统一 `ConnectionStrategy` 类**：合并 `PartitionedConnectionStrategy` 和 `SharedQueueConnectionStrategy`
- **队列矩阵模型**：`upstream_parallelism × downstream_parallelism` 个 SPSC 队列
- **队列索引公式**：`queue_index(i, j) = i × downstream_parallelism + j`
- **简化架构**：移除 `ConnectionType` 枚举，所有连接使用相同策略

### 修复高并行度 Join 召回率问题

解决了 BruteForce 和 IVF Join 方法在高并行度下召回率下降的问题：

- **根本原因**：高并行度路径缺少 Query2（第二次查询）
- **修复方案**：实现完整的 QIQ (Query-Insert-Query) 策略
  - `p=1`：无锁 QIQ（单线程不需要同步）
  - `p>1`：读写锁 QIQ（`shared_lock` 用于查询，`unique_lock` 用于插入）
- **测试验证**：所有并行度级别 (1,2,4,8,16) 召回率均达到 95%+

测试结果：

| 方法 | p=1 | p=2 | p=4 | p=8 | p=16 |
|------|-----|-----|-----|-----|------|
| BruteForce | 98.8% ✅ | 98.3% ✅ | 99.3% ✅ | 98.1% ✅ | 99.9% ✅ |
| IVF | 99.0% ✅ | 97.4% ✅ | 99.9% ✅ | 100% ✅ | 100% ✅ |

### IVF 方法优化

- 恢复使用真实 IVF 索引进行范围搜索
- 优化 `nprobes` 参数为 `nlist` 的 50%
- 设置 `rebuild_threshold = 2.0` 以减少重建频率

### Operator 分区器接口

新增 `getPreferredPartitioner()` 虚方法，支持算子指定首选分区策略：

- JoinOperator: 支持 LSH/RoundRobin 选择
- 默认实现返回 `std::nullopt`（使用系统默认）

---

## ⚠️ 已知问题

1. **性能测试不稳定**: CI 中的 Performance Test 偶发超时
2. **部分 Baseline 方法未完全实现**: HDRTree, ClusteredJoin 的完整实现待补充

---

## 🔄 后续工作

- [ ] 完善 HDRTree 和 ClusteredJoin 的完整实现
- [ ] 添加更多性能基准测试
- [ ] 优化 CI 性能测试稳定性
- [ ] 补充 Python Binding
- [x] ~~修复高并行度 Join 召回率问题~~ (已完成)
- [x] ~~统一连接策略架构~~ (已完成)

---

## 📈 变更统计

```
195+ files changed, 51000+ insertions(+), 3500+ deletions(-)
```

# VSJoin 任务依赖关系图

本文档描述所有任务的依赖关系，帮助规划并行开发。

---

## 任务详情文档索引

| 文档 | 内容 | 任务数 |
|------|------|--------|
| [TASK_GROUP_A_FOUNDATION.md](./TASK_GROUP_A_FOUNDATION.md) | 基础组件（独立任务） | 7 个 |
| [TASK_GROUP_B_CORE_VSJOIN.md](./TASK_GROUP_B_CORE_VSJOIN.md) | VSJoin 核心组件 | 6 个 |
| [TASK_GROUP_C_BASELINES.md](./TASK_GROUP_C_BASELINES.md) | Baseline 实现 | 5 个 |
| [TASK_GROUP_D_TESTING.md](./TASK_GROUP_D_TESTING.md) | 测试与验证 | 4 个 |

---

## 总体依赖图

```
                                ┌─────────────────────────────────────────────────────────────────┐
                                │                    第一批：独立基础任务 (Week 1-2)                 │
                                │              可完全并行开发，无任何依赖关系                         │
                                └─────────────────────────────────────────────────────────────────┘
                                                            │
        ┌──────────────┬──────────────┬──────────────┬──────┴──────┬──────────────┬──────────────┐
        │              │              │              │             │              │              │
        ▼              ▼              ▼              ▼             ▼              ▼              ▼
    [A-01]         [A-02]         [A-03]         [A-04]        [A-05]         [A-06]         [A-07]
 TwoTierWindow   LSHPartitioner  BoundaryTracker LateArrival  DistanceVerifier  PCA工具      ComputeEngine
    State          (LSH分区)       (边界追踪)     Handler        (距离验证)      (降维)        SIMD优化
        │              │              │             │              │              │              │
        │              │              └──────┬──────┘              │              │              │
        │              │                     │                     │              │              │
        ▼              ▼                     ▼                     ▼              ▼              ▼
    ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
    │                           第二批：组合组件任务 (Week 2-3)                                      │
    │                  依赖第一批部分任务，但批内可并行                                               │
    └─────────────────────────────────────────────────────────────────────────────────────────────┘
                                                            │
        ┌──────────────────────────┬────────────────────────┼────────────────────────┐
        │                          │                        │                        │
        ▼                          ▼                        ▼                        ▼
    [B-01]                     [B-02]                   [B-03]                   [B-04]
 PartitionedIndex          PartitionedVector      CoordinationLayer       AsyncCandidate
   (分区索引)                  State                  (协调层)              Generator
        │                          │                        │                        │
        └──────────────────────────┼────────────────────────┼────────────────────────┘
                                   │                        │
                                   ▼                        ▼
                        ┌─────────────────────────────────────────────┐
                        │        第三批：集成任务 (Week 3-4)            │
                        │          依赖第二批，需串行集成               │
                        └─────────────────────────────────────────────┘
                                                │
                                                ▼
                                            [C-01]
                                     JoinOperator VSJoin
                                           集成
                                                │
                                                ▼
                        ┌─────────────────────────────────────────────┐
                        │      第四批：Baseline 实现 (Week 4-6)         │
                        │         可与第三批并行，批内可并行             │
                        └─────────────────────────────────────────────┘
                                                │
        ┌──────────────┬──────────────┬─────────┴─────────┬──────────────┐
        │              │              │                   │              │
        ▼              ▼              ▼                   ▼              ▼
    [D-01]         [D-02]         [D-03]              [D-04]         [D-05]
   S3J/DEBS'23    HDR-Tree     HNSW增强               IVF增强      ClusteredJoin
   Baseline       Baseline      Baseline              Baseline       VectraFlow
        │              │              │                   │              │
        └──────────────┴──────────────┴─────────┬─────────┴──────────────┘
                                                │
                                                ▼
                        ┌─────────────────────────────────────────────┐
                        │       第五批：性能测试 (Week 6-8)             │
                        │            依赖所有前置任务                  │
                        └─────────────────────────────────────────────┘
                                                │
                                                ▼
                                            [E-01]
                                     性能测试与基准对比
```

---

## 任务分组详情

### Group A: 独立基础任务（完全并行）

| 任务ID | 名称 | 文件 | 预估工时 | 依赖 |
|--------|------|------|----------|------|
| A-01 | TwoTierWindowState | `TASK_GROUP_A_FOUNDATION.md` | 3-4天 | 无 |
| A-02 | LSHPartitioner | `TASK_GROUP_A_FOUNDATION.md` | 3-4天 | 无 |
| A-03 | BoundaryTracker | `TASK_GROUP_A_FOUNDATION.md` | 2天 | 无 |
| A-04 | LateArrivalHandler | `TASK_GROUP_A_FOUNDATION.md` | 2-3天 | 无 |
| A-05 | DistanceVerifier | `TASK_GROUP_A_FOUNDATION.md` | 2-3天 | 无 |
| A-06 | PCA 工具类 | `TASK_GROUP_A_FOUNDATION.md` | 2天 | 无 |
| A-07 | ComputeEngine SIMD | `TASK_GROUP_A_FOUNDATION.md` | 2天 | 无 |

### Group B: 组合组件任务（批内并行）

| 任务ID | 名称 | 文件 | 预估工时 | 依赖 |
|--------|------|------|----------|------|
| B-01 | PartitionedIndex | `TASK_GROUP_B_CORE_VSJOIN.md` | 3-4天 | A-02 |
| B-02 | PartitionedVectorState | `TASK_GROUP_B_CORE_VSJOIN.md` | 3-4天 | A-01, A-02 |
| B-03 | CoordinationLayer | `TASK_GROUP_B_CORE_VSJOIN.md` | 2-3天 | A-03, A-04 |
| B-04 | AsyncCandidateGenerator | `TASK_GROUP_B_CORE_VSJOIN.md` | 2-3天 | A-05 |

### Group C: 集成任务

| 任务ID | 名称 | 文件 | 预估工时 | 依赖 |
|--------|------|------|----------|------|
| C-01 | JoinOperator VSJoin 集成 | `TASK_GROUP_B_CORE_VSJOIN.md` | 4-5天 | B-01~B-04 |
| C-02 | AdaptiveIVF | `TASK_GROUP_B_CORE_VSJOIN.md` | 2-3天 | C-01 |

### Group D: Baseline 实现（批内并行）

| 任务ID | 名称 | 文件 | 预估工时 | 依赖 |
|--------|------|------|----------|------|
| D-01 | S3J/DEBS'23 Baseline | `TASK_GROUP_C_BASELINES.md` | 4-5天 | 无 (独立) |
| D-02 | HDR-Tree Baseline | `TASK_GROUP_C_BASELINES.md` | 5-6天 | A-06 |
| D-03 | HNSW 增强 | `TASK_GROUP_C_BASELINES.md` | 2-3天 | 无 (已有基础) |
| D-04 | IVF 增强 | `TASK_GROUP_C_BASELINES.md` | 2-3天 | 无 (已有基础) |
| D-05 | ClusteredJoin VectraFlow | `TASK_GROUP_C_BASELINES.md` | 3-4天 | 无 |

### Group E: 测试与验证

| 任务ID | 名称 | 文件 | 预估工时 | 依赖 |
|--------|------|------|----------|------|
| E-01 | 性能基准测试 | `TASK_GROUP_D_TESTING.md` | 4-5天 | C-01, D-01~D-05 |
| E-02 | 集成测试 | `TASK_GROUP_D_TESTING.md` | 3-4天 | C-01 |
| E-03 | 召回率验证 | `TASK_GROUP_D_TESTING.md` | 2-3天 | C-01, D-03, D-04 |

---

## 推荐开发计划

### 并行开发方案（4人团队）

```
Week 1-2:
  开发者A: A-01 TwoTierWindowState + A-03 BoundaryTracker
  开发者B: A-02 LSHPartitioner + A-04 LateArrivalHandler
  开发者C: A-05 DistanceVerifier + A-07 ComputeEngine SIMD
  开发者D: A-06 PCA工具 + D-01 S3J Baseline (可提前启动)

Week 2-3:
  开发者A: B-02 PartitionedVectorState
  开发者B: B-01 PartitionedIndex
  开发者C: B-04 AsyncCandidateGenerator
  开发者D: B-03 CoordinationLayer + D-03 HNSW增强

Week 3-4:
  全员协作: C-01 JoinOperator VSJoin 集成
  
Week 4-5:
  开发者A: C-02 AdaptiveIVF
  开发者B: D-02 HDR-Tree Baseline
  开发者C: D-04 IVF增强
  开发者D: D-05 ClusteredJoin

Week 5-6:
  全员协作: E-01~E-03 性能测试与验证
```

### 单人开发方案

```
Week 1-2: A-01 → A-02 → A-03 → A-04
Week 2-3: A-05 → A-06 → A-07
Week 3-4: B-01 → B-02 → B-03 → B-04
Week 4-5: C-01 → C-02
Week 5-6: D-03 → D-04 → D-05
Week 6-7: D-01 → D-02
Week 7-8: E-01 → E-02 → E-03
```

---

## 关键里程碑

| 里程碑 | 完成标志 | 预计时间 |
|--------|----------|----------|
| M1 | 所有 Group A 任务完成 | Week 2 末 |
| M2 | PartitionedVectorState 可用 | Week 3 中 |
| M3 | VSJoin 集成完成 | Week 4 末 |
| M4 | 所有 Baseline 就绪 | Week 6 中 |
| M5 | 性能对比报告完成 | Week 8 末 |

---

## 风险点与缓解

| 风险 | 影响任务 | 缓解措施 |
|------|----------|----------|
| LSH 分区效果不佳 | A-02, B-01, B-02 | 准备 KMeans 备选方案 |
| PCA 实现复杂度高 | A-06, D-02 | 可使用第三方库如 Eigen |
| 集成时接口不匹配 | C-01 | 提前定义清晰接口 |
| Baseline 复现困难 | D-01, D-02 | 简化实现，保留核心思想 |

---

## 全部任务汇总

### Group A: 基础组件（7 个独立任务）

| ID | 任务名称 | 工时 | 依赖 | 详情 |
|----|----------|------|------|------|
| A-01 | TwoTierWindowState | 3-4天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-01-twotierwindowstate-双层窗口状态) |
| A-02 | LSHPartitioner | 3-4天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-02-lshpartitioner-lsh-分区器) |
| A-03 | BoundaryTracker | 2天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-03-boundarytracker-边界向量追踪器) |
| A-04 | LateArrivalHandler | 2-3天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-04-latearrivalhandler-延迟到达处理器) |
| A-05 | DistanceVerifier | 2-3天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-05-distanceverifier-距离验证器) |
| A-06 | PCA 工具类 | 2天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-06-pca-工具类) |
| A-07 | ComputeEngine SIMD | 2天 | 无 | [查看](./TASK_GROUP_A_FOUNDATION.md#a-07-computeengine-simd-优化) |

### Group B: VSJoin 核心组件（6 个任务）

| ID | 任务名称 | 工时 | 依赖 | 详情 |
|----|----------|------|------|------|
| B-01 | PartitionedIndex | 3-4天 | A-02 | [查看](./TASK_GROUP_B_CORE_VSJOIN.md#b-01-partitionedindex-分区索引) |
| B-02 | PartitionedVectorState | 3-4天 | A-01, A-02 | [查看](./TASK_GROUP_B_CORE_VSJOIN.md#b-02-partitionedvectorstate-分区向量状态) |
| B-03 | CoordinationLayer | 2-3天 | A-03, A-04 | [查看](./TASK_GROUP_B_CORE_VSJOIN.md#b-03-coordinationlayer-协调层) |
| B-04 | AsyncCandidateGenerator | 2-3天 | A-05 | [查看](./TASK_GROUP_B_CORE_VSJOIN.md#b-04-asynccandidategenerator-异步候选生成器) |
| C-01 | JoinOperator VSJoin 集成 | 4-5天 | B-01~B-04 | [查看](./TASK_GROUP_B_CORE_VSJOIN.md#c-01-joinoperator-vsjoin-集成) |
| C-02 | AdaptiveIVF | 2-3天 | C-01 | [查看](./TASK_GROUP_B_CORE_VSJOIN.md#c-02-adaptiveivf-自适应召回控制) |

### Group C: Baseline 实现（5 个任务）

| ID | 任务名称 | 工时 | 依赖 | 详情 |
|----|----------|------|------|------|
| D-01 | S3J/DEBS'23 Baseline | 2-3天 | 无 | [查看](./TASK_GROUP_C_BASELINES.md#d-01-s3jdebs23-baseline) |
| D-02 | HDR-Tree Baseline | 3-4天 | A-06 | [查看](./TASK_GROUP_C_BASELINES.md#d-02-hdr-tree-baseline) |
| D-03 | HNSW Enhanced | 2天 | 无 | [查看](./TASK_GROUP_C_BASELINES.md#d-03-hnsw-enhanced-baseline) |
| D-04 | IVF Enhanced | 2天 | 无 | [查看](./TASK_GROUP_C_BASELINES.md#d-04-ivf-enhanced-baseline) |
| D-05 | ClusteredJoin VectraFlow | 3-4天 | 无 | [查看](./TASK_GROUP_C_BASELINES.md#d-05-clusteredjoin-vectraflow-baseline) |

### Group D: 测试与验证（4 个任务）

| ID | 任务名称 | 工时 | 依赖 | 详情 |
|----|----------|------|------|------|
| E-01 | 性能基准测试框架 | 2-3天 | C-01 | [查看](./TASK_GROUP_D_TESTING.md#e-01-性能基准测试框架) |
| E-02 | 集成测试套件 | 2-3天 | C-01, D-* | [查看](./TASK_GROUP_D_TESTING.md#e-02-集成测试套件) |
| E-03 | 召回率验证工具 | 1-2天 | E-01 | [查看](./TASK_GROUP_D_TESTING.md#e-03-召回率验证工具) |
| E-04 | 实验报告生成 | 1天 | E-01, E-02 | [查看](./TASK_GROUP_D_TESTING.md#e-04-实验报告生成) |

---

## 快速开始指南

1. **阅读顺序**: TASK_DEPENDENCY_GRAPH.md → 选择任务组 → 具体任务
2. **并行开发**: Group A 的 7 个任务可完全并行
3. **任务提示词**: 每个任务文件包含完整的提示词，可直接用于 AI 辅助开发
4. **验收标准**: 每个任务包含明确的验收标准和测试要求

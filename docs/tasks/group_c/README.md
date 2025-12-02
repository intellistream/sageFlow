# Group C: VSJoin 集成与配置驱动的自适应策略

本目录包含 VSJoin 集成任务和配置驱动的 Baseline 方法选择系统的详细任务定义。

---

## 任务概览

| 任务ID | 名称 | 优先级 | 预估工时 | 依赖 | 状态 |
|--------|------|--------|----------|------|------|
| C-01 | VSJoin 集成到 JoinOperator | 🔴 高 | 2-3天 | B01-B04 | ✅ 完成 |
| C-02 | 配置驱动的 Join 策略工厂 | 🔴 高 | 2天 | C-01 | ✅ 完成 |
| C-03 | 分区策略自适应选择 | 🔴 高 | 1-2天 | C-02 | ✅ 完成 |
| C-04 | 窗口状态自适应选择 | 🔴 高 | 1天 | C-02 | ✅ 完成 |
| C-05 | Baseline 方法注册与切换 | 🟡 中 | 2天 | C-02, D-01~D-06 | ✅ 完成 |
| C-06 | 配置验证与错误处理 | 🟡 中 | 1天 | C-02~C-05 | ✅ 完成 |

---

## 任务文件索引

- [C01_VSJoin_Integration.md](../group_b/C01_VSJoin_Integration.md) - VSJoin 集成（已完成，位于 group_b）
- [C02_JoinStrategyFactory.md](./C02_JoinStrategyFactory.md) - 配置驱动的策略工厂
- [C03_PartitionerFactory.md](./C03_PartitionerFactory.md) - 分区策略自适应选择
- [C04_WindowStateFactory.md](./C04_WindowStateFactory.md) - 窗口状态自适应选择
- [C05_JoinMethodRegistry.md](./C05_JoinMethodRegistry.md) - Baseline 方法注册系统
- [C06_JoinConfigValidator.md](./C06_JoinConfigValidator.md) - 配置验证与错误处理

---

## 任务依赖图

```
              ┌──────────────────────────────────────────┐
              │            D-01 ~ D-06                   │
              │        (Baseline 实现)                   │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
┌─────────────┐            ┌─────────────┐
│   B01~B04   │───────────▶│    C-01     │
│(VSJoin组件) │            │(VSJoin集成) │
└─────────────┘            └──────┬──────┘
                                  │
                                  ▼
                           ┌─────────────┐
                           │    C-02     │
                           │(策略工厂)   │
                           └──────┬──────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              ┌─────────┐   ┌─────────┐   ┌─────────┐
              │  C-03   │   │  C-04   │   │  C-05   │
              │(分区策略)│   │(窗口策略)│   │(方法注册)│
              └────┬────┘   └────┬────┘   └────┬────┘
                   │             │             │
                   └─────────────┼─────────────┘
                                 ▼
                           ┌─────────────┐
                           │    C-06     │
                           │(配置验证)   │
                           └─────────────┘
```

---

## 策略兼容性规则

以下是各策略之间的兼容性约束，在实现时必须遵守：

| 分区策略 | 兼容的窗口状态 | 说明 |
|---------|---------------|------|
| RoundRobin | SharedWindowState | 随机分发需要共享状态保证完整性 |
| KeyPartitioner | Partitioned/Shared | 基于 key 分区 |
| VectorHash | Partitioned | 相似向量聚集到同一分区 |
| LSH | PartitionedVectorState | VSJoin 专用 |
| Centroid | Partitioned | S3J 专用 |

### 不兼容配置（会导致召回率下降）

- ❌ RoundRobin + PartitionedWindowState → 跨分区匹配丢失
- ❌ VSJoin + SharedWindowState → 架构不支持
- ❌ S3J + RoundRobin → 分区语义冲突

---

## 参考文档

- [JOIN_PIPELINE_GUIDE.md](../../JOIN_PIPELINE_GUIDE.md) - Join 流程详解
- [VSJOIN_IMPLEMENTATION_ROADMAP.md](../../VSJOIN_IMPLEMENTATION_ROADMAP.md) - 完整实现路线图
- [PARALLEL_TASK_GUIDE.md](../PARALLEL_TASK_GUIDE.md) - 并行任务执行指南

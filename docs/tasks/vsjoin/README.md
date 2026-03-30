# VSJoin 实现任务总览

本文档包含 VSJoin 双层索引方案的所有实现任务，每个任务都有独立的提示词文档用于指导大模型进行开发。

## 任务列表

| 任务ID | 任务名称 | 预估工时 | 依赖 | 状态 |
|--------|---------|---------|------|------|
| [Task 01](task01_vsjoin_method.md) | VSJoinMethod 基础实现 | 2 天 | - | 待开始 |
| [Task 02](task02_factory_integration.md) | JoinStrategyFactory 集成 | 1 天 | Task 01 | 待开始 |
| [Task 03](task03_operator_path.md) | JoinOperator VSJoin 特殊路径 | 1 天 | Task 02 | 待开始 |
| [Task 04](task04_rebuild_mechanism.md) | 后台重建机制 GlobalIndexRebuilder | 1.5 天 | Task 03 | 待开始 |
| [Task 05](task05_config_validation.md) | 配置验证 + TOML 解析 | 0.5 天 | Task 01 | 待开始 |
| [Task 06](task06_integration_test.md) | 集成测试 + 召回率验证 | 1 天 | Task 04, Task 05 | 待开始 |
| [Task 07](task07_assignment_table.md) | AssignmentTable (RCU) + LoadMonitor | 2 天 | Task 03 | 待开始 |
| [Task 08](task08_logical_partition_routing.md) | Logical Partition 路由集成 | 1 天 | Task 07 | 待开始 |
| [Task 09](task09_load_balancing_test.md) | 负载均衡测试 | 0.5 天 | Task 08 | 待开始 |

**总预估工时**: 10.5 个工作日

## 任务依赖关系

```
Task 01 (VSJoinMethod)
├── Task 02 (Factory Integration)
│   └── Task 03 (Operator Path)
│       ├── Task 04 (Rebuild Mechanism)
│       │   └── Task 06 (Integration Test)
│       └── Task 07 (AssignmentTable)
│           └── Task 08 (Logical Partition Routing)
│               └── Task 09 (Load Balancing Test)
└── Task 05 (Config Validation)
    └── Task 06 (Integration Test)
```

## 分阶段交付计划

### 第一阶段：核心功能（7 个工作日）

- **Task 01**: VSJoinMethod 基础实现
- **Task 02**: JoinStrategyFactory 集成
- **Task 03**: JoinOperator VSJoin 特殊路径
- **Task 04**: 后台重建机制（含去重）
- **Task 05**: 配置验证 + TOML 解析
- **Task 06**: 集成测试 + 召回率验证

**交付物**: VSJoin 双层索引 + 后台重建 + 去重功能完整实现

### 第二阶段：负载均衡（3.5 个工作日）

- **Task 07**: AssignmentTable (RCU) + LoadMonitor
- **Task 08**: Logical Partition 路由集成
- **Task 09**: 负载均衡测试

**交付物**: VSJoin 负载均衡功能完整实现

## 任务文档说明

每个任务文档包含：

1. **任务概述**: 任务目标和范围
2. **参考文档**: 主设计文档中的相关章节
3. **实现要求**: 详细的实现步骤和代码示例
4. **关键设计点**: 需要注意的设计要点
5. **测试要求**: 单元测试和集成测试要求
6. **注意事项**: 实现过程中需要注意的问题
7. **验收标准**: 任务完成的验收标准
8. **后续任务**: 完成后可以继续的任务

## 使用指南

### 对于开发者

1. 按照任务依赖关系顺序实现任务
2. 每个任务开始前，仔细阅读对应的任务文档
3. 参考主设计文档 `docs/vsjoin_compliant_design_c745d987.plan.md` 获取更多上下文
4. 实现完成后，运行对应的测试用例验证功能
5. 满足验收标准后，标记任务为完成

### 对于 AI 助手

1. 读取对应的任务文档
2. 参考主设计文档中的相关章节
3. 查看现有代码库中的类似实现（如 `ivf_method.h/cpp`）
4. 按照任务文档中的实现要求编写代码
5. 确保代码符合 SageFlow 代码风格规范
6. 编写对应的单元测试

## 关键设计要点汇总

### 并发安全

- **Global Index 重建去重**: 使用局部 `unordered_set`，单线程，完全无锁
- **AssignmentTable**: 使用 RCU 方案，读操作完全无锁，批量更新原子性
- **Local Index**: 分区独占访问，完全无锁

### 去重策略

- **查询结果合并**: 局部 `unordered_set`，O(n)，n < 1000
- **Global Index 重建**: 局部 `unordered_set`，单线程，O(N)

### 负载均衡（可选）

- **Logical Partition + RCU AssignmentTable**: 粗粒度均衡，对长期负载不均有效
- **分阶段实现**: 观测 → 静态分配 → 动态调整

## 相关文档

- **主设计文档**: `docs/vsjoin_compliant_design_c745d987.plan.md`
- **架构文档**: `docs/SYSTEM_ARCHITECTURE.md`
- **Join 管道指南**: `docs/JOIN_PIPELINE_GUIDE.md`
- **添加新 Join 方法指南**: `docs/ADDING_NEW_JOIN_METHOD.md`

## 问题反馈

如果在实现过程中遇到问题，请：
1. 查阅主设计文档中的相关章节
2. 查看现有代码库中的类似实现
3. 参考 SageFlow 代码风格和架构约束
4. 在任务文档中添加注释说明问题

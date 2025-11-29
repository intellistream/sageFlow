# 任务组 B (VSJoin 核心组件) 任务拆分

**整体目标**: 实现 VSJoin 的核心组件  
**前置依赖**: 任务组 A 全部完成 ✅  
**任务数量**: 4 个主任务 + 2 个后续集成任务  
**完成状态**: ✅ B-01 ~ B-04 全部完成 (2025-11-27)

---

## 📋 任务清单

| 任务ID | 任务名称 | 优先级 | 预估工时 | 依赖 | 状态 |
|--------|----------|--------|----------|------|------|
| B-01 | PartitionedIndex | 🔴 高 | 3-4 天 | A-02 ✅ | ✅ 完成 |
| B-02 | PartitionedVectorState | 🔴 高 | 3-4 天 | A-01, A-02 ✅ | ✅ 完成 |
| B-03 | PartitionCoordinator | 🟡 中 | 2-3 天 | A-03, A-04 ✅ | ✅ 完成 |
| B-04 | AsyncCandidateGenerator | 🟡 中 | 2-3 天 | A-05 ✅ | ✅ 完成 |
| C-01 | VSJoin 集成 | 🔴 高 | 4-5 天 | B-01~B-04 ✅ | ⏳ 可开始 |
| C-02 | AdaptiveIVF | 🟢 低 | 2-3 天 | C-01 | ⏳ 等待依赖 |

---

## 📁 任务详情

### [B-01: PartitionedIndex](./B01_PartitionedIndex.md)
**分区索引 - 每个分区独立的索引**

输出文件:
- `include/index/partitioned_index.h`
- `src/index/partitioned_index.cpp`
- `test/UnitTest/test_partitioned_index.cpp`

核心功能:
- 每个分区独立的索引实例
- 路由 insert/query 到正确分区
- 支持跨分区查询

---

### [B-02: PartitionedVectorState](./B02_PartitionedVectorState.md)
**分区向量状态 - 窗口状态与分区的融合**

输出文件:
- `include/state/partitioned_vector_state.h`
- `src/state/partitioned_vector_state.cpp`
- `test/UnitTest/test_partitioned_vector_state.cpp`

核心功能:
- 整合 TwoTierWindowState 与 LSHPartitioner
- 每个分区独立的窗口管理
- 支持边界向量追踪

---

### [B-03: PartitionCoordinator](./B03_PartitionCoordinator.md)
**分区协调器 - 边界追踪与延迟处理的协调**

输出文件:
- `include/coordination/partition_coordinator.h`
- `src/coordination/partition_coordinator.cpp`
- `test/UnitTest/test_partition_coordinator.cpp`

核心功能:
- 整合 BoundaryTracker 与 LateArrivalHandler
- 按分区维护边界向量
- 按分区维护 watermark

---

### [B-04: AsyncCandidateGenerator](./B04_AsyncCandidateGenerator.md)
**异步候选生成器 - 并行跨分区候选生成**

输出文件:
- `include/operator/async_candidate_generator.h`
- `src/operator/async_candidate_generator.cpp`
- `test/UnitTest/test_async_candidate_generator.cpp`

核心功能:
- 异步跨分区候选生成
- 多线程查询执行
- 结果聚合与 DistanceVerifier 集成

---

### [C-01: VSJoin 集成](./C01_VSJoin_Integration.md)
**将 VSJoin 组件集成到 JoinOperator**

> ⚠️ 等待 B 组任务全部完成

输出文件:
- 修改 `include/operator/join_operator.h`
- 修改 `src/operator/join_operator.cpp`
- `test/IntegrationTest/test_vsjoin_integration.cpp`

核心功能:
- 集成所有 VSJoin 组件
- 保持向后兼容
- 新增 vsjoin_eager/vsjoin_lazy 模式

---

### [C-02: AdaptiveIVF](./C02_AdaptiveIVF.md)
**自适应 IVF 索引 - 运行时召回率控制**

> ⚠️ 等待 C-01 完成

输出文件:
- `include/index/adaptive_ivf.h`
- `src/index/adaptive_ivf.cpp`
- `test/UnitTest/test_adaptive_ivf.cpp`

核心功能:
- 在线召回率估计
- 自适应 nprobes 调整
- 召回率目标配置

---

## 🏗️ 依赖关系图

```
任务组 A (已完成 ✅)
├── A-01: TwoTierWindowState ─────────┬─────→ B-02
├── A-02: LSHPartitioner ─────────────┴─────→ B-01, B-02
├── A-03: BoundaryTracker ────────────┬─────→ B-03
├── A-04: LateArrivalHandler ─────────┘
├── A-05: DistanceVerifier ─────────────────→ B-04
├── A-06: PCA ─────────────────────────────→ (可选优化)
└── A-07: SIMD Optimization ───────────────→ (可选优化)

任务组 B (当前批次)
├── B-01: PartitionedIndex ─────────────────┐
├── B-02: PartitionedVectorState ───────────┤
├── B-03: PartitionCoordinator ─────────────┼──→ C-01: VSJoin 集成
└── B-04: AsyncCandidateGenerator ──────────┘
                                                   │
                                                   ▼
                                             C-02: AdaptiveIVF
```

---

## 🚀 并行开发说明

### 第一批 (可立即开始, 4 个 Copilot 并行)
- B-01, B-02, B-03, B-04 互相独立
- 均只依赖已完成的 A 组任务
- 可分配给 4 个 Copilot 同时开发

### 第二批 (等待 B 组完成)
- C-01: VSJoin 集成 (依赖 B-01~B-04)

### 第三批 (等待 C-01 完成)
- C-02: AdaptiveIVF (依赖 C-01)

---

## 📋 验收标准

每个任务完成后需满足:

1. **代码完整性**
   - 头文件创建/修改完成
   - 实现文件创建/修改完成
   - 测试文件创建完成

2. **测试通过**
   - 单元测试 100% 通过
   - 集成测试通过 (C-01)

3. **代码质量**
   - 通过 clang-tidy 检查
   - 遵循命名规范
   - Doxygen 注释完整

4. **文档更新**
   - 必要的 README 更新

---

## 🔧 开发环境

```bash
# 配置
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON

# 编译
cmake --build build -j $(nproc)

# 运行单元测试
ctest --test-dir build -L UNIT --output-on-failure

# 运行特定测试
./build/bin/test_partitioned_index
./build/bin/test_partitioned_vector_state
./build/bin/test_partition_coordinator
./build/bin/test_async_candidate_generator
```

---

## 📊 进度追踪

| 任务 | 开始时间 | 完成时间 | 状态 |
|------|----------|----------|------|
| B-01 | - | - | ⏳ |
| B-02 | - | - | ⏳ |
| B-03 | - | - | ⏳ |
| B-04 | - | - | ⏳ |
| C-01 | - | - | ⏳ 等待依赖 |
| C-02 | - | - | ⏳ 等待依赖 |

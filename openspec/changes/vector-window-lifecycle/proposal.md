## Why

向量相似度 Join 的滑动窗口当前是 `JoinOperator` 内部的隐式状态，存在三类问题：(1) 一条向量从进入算子到落入 `StorageManager`，热路径上发生多次 `VectorRecord` 深拷贝（含 `VectorData` 的 `char[]` 拷贝），profile 显示分配器（tcmalloc Populate）占用约 40% CPU，是高并行负优化的主因之一；(2) 窗口快照 `getRecordsSnapshot` 每元素 `make_shared<const VectorRecord>(*r)` 全量深拷贝，候选获取复杂度被放大到 O(M) 拷贝/查询；(3) 触发（每记录即时 IQ）与清理（过期 evict + 索引批量删除）逻辑散落在算子内，时间戳推进用算子级原子量、半成品 `isNeedTrigger/SlidingWindow` 未接入，语义不清晰。

由于这是按相似度阈值做的全量近邻 Join（不是 keyBy 等值 Join），共享窗口**不能按 subtask 分片**——分片会漏掉跨分片的相似对，破坏召回。因此扩展性收益必须来自“消除冗余拷贝/分配 + 收敛触发清理语义”，而不是分片。

## What Changes

- 定义并落地**向量记录全链路所有权契约**：从 ingestion → 窗口状态 → `StorageManager` → 候选获取 → emit，统一以 `std::shared_ptr<const VectorRecord>` 作为不可变共享视图传递，消除热路径上的重复深拷贝。**BREAKING**：`WindowState`/`BaseMethod::ExecuteEager`/候选返回类型由 `unique_ptr<VectorRecord>` 改为 `shared_ptr<const VectorRecord>`（接口级变更）。
- 重新定义**窗口快照语义**：`getRecordsSnapshot` 返回共享指针视图（只增引用计数，不拷贝向量数据），并明确快照的一致性边界（快照点之后的插入对本次查询不可见，与现有 IQ 不变量一致）。
- 收敛**触发与清理语义**：明确“每记录即时 IQ 触发”为正式触发模型；把窗口过期判定、安全 evict 时间戳（两侧 watermark 最小值）、过期 UID 批量删除统一收敛到 `WindowState` 接口；移除/隔离未接入的 `isNeedTrigger`/`SlidingWindow` 半成品，避免语义误导。
- 明确**共享窗口保持单一全量结构**（不分片）的不变量，并在文档与代码注释中固化“相似度 Join 全量召回”约束。
- 不改变每条向量的处理顺序（先 Insert 自身、再 Query 对侧）与 Join 结果正确性；不引入算子级粗粒度全局锁。

## Capabilities

### New Capabilities
- `vector-record-lifecycle`: 向量记录从进入算子到落入 StorageManager、再到候选/emit 的全链路所有权与生命周期契约（shared_ptr 不可变视图、引用计数转移点、禁止深拷贝的位置、并发可见性与释放时机）。
- `vector-window-state`: 滑动窗口状态组件的对外契约（快照零拷贝语义、插入/查询并发边界、触发模型、安全 evict 与过期清理、共享窗口不分片的全量召回不变量）。

### Modified Capabilities
<!-- 当前 openspec/specs/ 为空，无既有 capability 的 requirement 变更，留空。 -->

## Impact

- 接口/代码：
  - `include/state/window_state.h` 及 `SharedWindowState`/`PartitionedWindowState`/`TwoTierWindowState`（快照与记录存储类型）。
  - `include/operator/join_operator_methods/base_method.h` 的 `ExecuteEager` 返回类型；各 `JoinMethod` 实现。
  - `src/operator/join_operator.cpp` 的 `getCandidatesFromState`/`updateSideWithState`/`executeJoinWithState`/`apply`（拷贝点）。
  - `src/operator/join_operator_methods/vsjoin_method.cpp` 的 `resolveUidsToRecords`。
  - `StorageManager`/`ConcurrencyManager`/`BlankController` 的 `query_for_join` 返回路径（已是 shared_ptr，需贯通到调用方不再降级为深拷贝）。
- 行为：召回率/正确性不变；目标是降低分配与拷贝、改善高并行扩展性。
- 验证：复用 `test_join_operator_state`、`test_join_bruteforce`、`test_join_ivf`、`test_join_datasource_modes` 与 gperftools profile。
- 不在本次范围：clustered_join/vsjoin 的 partitioner 每记录重建、global rebuild wall-clock、负载均衡路由等（单独 change）。

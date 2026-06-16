## Context

向量相似度 Join 的滑动窗口目前是 `JoinOperator` 的隐式内部状态。基于对当前代码的核对，一条向量从进入算子到落入存储，热路径上存在多次 `VectorRecord` 深拷贝（每次都拷贝 `VectorData` 的 `char[]`）：

- [join_operator.cpp:999](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator.cpp#L999) `data_ptr = make_unique<VectorRecord>(*record.record_)`（进入算子，规范化构造，拷贝 1）
- [join_operator.cpp:1193](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator.cpp#L1193) `data_for_join = make_unique<VectorRecord>(*data_ptr)`（拷贝 2）
- [join_operator.cpp:786](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator.cpp#L786) `data_for_index_insert = make_unique<VectorRecord>(*data_ptr)`（拷贝 3）
- [shared_window_state.cpp:34](file:///Users/bytedance/icpp-demo/sageFlow/src/state/shared_window_state.cpp#L34) 快照 `make_shared<const VectorRecord>(*record)`（每元素深拷贝）
- `executeJoinWithState` 内对每个命中候选再构造 left_copy/right_copy（emit 前再拷）

`StorageManager` 内部已是 `shared_ptr<VectorRecord>` 存储，`getVectorByUid`/`getVectorsByUids` 返回 `shared_ptr<const VectorRecord>` 仅增引用计数（[storage_manager.cpp:57-96](file:///Users/bytedance/icpp-demo/sageFlow/src/storage/storage_manager.cpp#L57-L96)，边界检查使用内部 `records_.size()`，正确）。问题在于调用链上游把候选/窗口记录降级回 `unique_ptr` 深拷贝，浪费了存储层已有的共享语义。

约束（来自项目与用户）：
- 这是相似度阈值 Join，**共享窗口不能分片**（会漏跨分片相似对）。
- 不得绕过 `ConcurrencyManager` 直接操作索引热路径。
- 重构 Join 逻辑不变，每条向量处理顺序（IQ）不变，召回不变。
- `VectorRecord` 的 `uid_`/`timestamp_` 为 `const`，`VectorData` 持 `unique_ptr<char[]>`，本身不可平凡共享——共享必须通过 `shared_ptr<const VectorRecord>` 在记录粒度完成。

## Goals / Non-Goals

**Goals:**
- 全链路统一以 `shared_ptr<const VectorRecord>` 传递，消除候选/快照/emit 路径上的重复深拷贝。
- 窗口快照改为零拷贝只读视图（指针拷贝 + 引用计数）。
- 触发（每记录 IQ）与清理（双侧 watermark 安全 evict + 过期 UID 批量删除）收敛到 `WindowState` 接口，语义清晰可测。
- 固化“共享窗口单一全量、不分片”的召回不变量。
- 明确从 ingestion 到 StorageManager 的所有权转移点与释放时机，符合 C++ RAII / 单一权威副本 / 不可变共享读 的最佳实践。

**Non-Goals:**
- 不分片共享窗口；不引入 keyBy 式数据分区。
- 不改 clustered_join/vsjoin 的 partitioner 每记录重建、global rebuild wall-clock、负载均衡路由（单独 change）。
- 不引入新的第三方依赖；不改变 Join 算法语义与阈值。
- 不做对象池/arena 分配（可作为后续优化，本次先消除冗余拷贝）。

## Decisions

### 决策 1：记录粒度的 `shared_ptr<const VectorRecord>` 作为统一货币
- 选择在“记录”粒度共享，而非在 `VectorData` 粒度共享。理由：`VectorRecord` 的 `uid_/timestamp_` 为 const、`VectorData` 含 `unique_ptr<char[]>`，记录本身天然适合不可变共享；`const` 保证多线程只读安全，无需写锁保护数据内容。
- 替代方案：让 `VectorData` 内部改持 `shared_ptr<char[]>`。否决：侵入式更大、序列化/相等语义受影响，且无法获得“整记录不可变”的并发收益。

### 决策 2：接口返回类型由 `unique_ptr<VectorRecord>` 改为 `shared_ptr<const VectorRecord>`
- `WindowState::getRecordsSnapshot`、`BaseMethod::ExecuteEager`、`getCandidatesFromState` 统一返回 `std::vector<std::shared_ptr<const VectorRecord>>`。
- 这是 BREAKING 接口变更，需同步所有 `JoinMethod` 实现与 VSJoin `resolveUidsToRecords`。
- 替代方案：保留 unique_ptr 仅内部优化。否决：无法贯通存储层已有 shared 语义，深拷贝仍在。

### 决策 3：窗口内部存储改为 `shared_ptr<const VectorRecord>`
- `SharedWindowState::shared_window_`、`PartitionedWindowState` 分区 deque、`TwoTierWindowState` 容器元素类型改为 `shared_ptr<const VectorRecord>`。
- 插入时一次性构造权威记录并以 shared_ptr 同时交给窗口与（经 ConcurrencyManager/StorageManager）索引存储，使两侧共享同一实例（实现 spec 的“单一权威副本”）。
- 快照在持 `shared_lock` 期间仅 `push_back` 指针，临界区极短。

### 决策 4：所有权边界——传输层 `unique_ptr` 不变，仅算子内部用 `shared_ptr<const>`（定稿）
- 明确区分两层，shared_ptr 只活在单个 `JoinOperator` 内部，从不跨算子边界：
  - **算子间传输层**（`Response` / `TaggedResponse` / `RingBufferQueue`）：保持 `unique_ptr<VectorRecord>` + `std::move`。这一层是单一所有权、沿 pipeline 单向 move，本就是零拷贝，改 shared_ptr 无收益且平白引入原子计数/控制块，并需改动全下游算子签名——不改。
  - **算子内部状态层**（窗口 deque / StorageManager / 候选 / 快照）：用 `shared_ptr<const VectorRecord>`，因为一条记录在此区间被多个持有者同时引用（窗口 + 存储 + 在途快照/候选）。
- **转换点 = `apply` 入口一次 move**：当前 [join_operator.cpp:1091](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator.cpp#L1091) 的 `make_unique<VectorRecord>(*record.record_)` 是深拷贝；因 `apply(Response&& record, ...)` 为右值，所有权本属算子，改为 `std::shared_ptr<const VectorRecord> view = std::move(record.record_);` 零拷贝接管。此后算子内部全部用 `view` 共享（窗口/存储各持一份副本=引用计数+1，不拷数据）。
  - 顺序注意：VSJoin 路由 `computeVSJoinTargetSubtasks(const Response&, ...)` 需在 move 之前先算，或改为基于 `view` 计算。
- **emit 边界（出口）保守路线（定稿）**：join 命中后必须新建配对结果 `VectorRecord`，放回 `Response` 的 `unique_ptr` 再 push 下游。这一步是 emit 的正当成本，保留 `unique_ptr` 构造，不把 `Response`/队列改成 shared_ptr。
- **真正消除的浪费**：候选循环内对“查询侧记录”的重复深拷贝（每个命中候选都重拷一次 `*data_ptr`）改为“循环外/命中时构造一次并复用”；未命中项不构造。

### 决策 6：内层热路径用裸指针视图，避免引用计数争用
- 快照在持 `shared_lock` 期间取一次 `vector<shared_ptr<const VectorRecord>>`，保证这批候选在本次查询内存活。
- 相似度/时间窗口过滤等内层循环用 `const VectorRecord*`（`sp.get()`）遍历，**不在每次比较拷贝 shared_ptr**；跨函数边界一律 `const shared_ptr<const VectorRecord>&` 传引用，不按值传。
- 目的：把原子引用计数操作压到“快照构建”这一处 O(M) 次，内层 compute 零原子，规避多核 cache-line bouncing（这是 shared_ptr 在高并发下的真实开销，而非 malloc 竞争——控制块只在 make_shared/move 转换时分配一次）。

### 决策 7：触发与清理收敛
- 正式确立“每记录即时 IQ”为触发模型，移除/隔离 `isNeedTrigger`/`SlidingWindow` 半成品（[join_function.h:49-62](file:///Users/bytedance/icpp-demo/sageFlow/include/function/join_function.h#L49-L62)）。
- 安全 evict 统一走 `WindowState::getSafeEvictTimestamp`（两侧 max-seen 最小值，已存在于 [shared_window_state.cpp:137-160](file:///Users/bytedance/icpp-demo/sageFlow/src/state/shared_window_state.cpp#L137-L160)），算子不再各自维护 `max_seen_left_ts_/right_ts_`。
- 过期 UID 批量删除继续走 `flushExpiredUids` → `ConcurrencyManager` 删除，保证窗口/索引/存储一致。

## Risks / Trade-offs

- [接口大改导致编译面广] → 分阶段：先改类型别名与 WindowState，再逐个 JoinMethod 适配，每步用对应单测验证（bruteforce/ivf/state）。
- [shared_ptr 引用计数原子开销] → 相对被消除的整段 `char[]` 深拷贝，引用计数自增成本极低；通过 profile 对比 tcmalloc Populate 占比验证净收益。
- [emit 边界仍需一次构造，可能掩盖收益] → 明确收益主要来自候选循环与快照零拷贝；emit 边界构造为常数次，与候选数无关。
- [VSJoin 多播路径每目标插入需要独立记录] → 多播下确实需要每个 target 一个 shared_ptr 持有；仍是指针拷贝而非数据拷贝，符合契约。
- [并发释放时机变化] → 由 shared_ptr 引用计数保证最后持有者释放，需确保在途快照/候选持有期间不被存储 erase 提前析构；通过“快照持 shared_ptr”天然满足。
- [跨 polyrepo 边界] → 仅改 sageFlow 本仓；不动 SAGE/demo。

## Migration Plan

1. 引入类型别名（如 `using RecordView = std::shared_ptr<const VectorRecord>;`）集中管理。
2. 改 `WindowState` 接口与三个实现的存储/快照类型，跑 `test_join_operator_state`。
3. 改 `BaseMethod::ExecuteEager` 返回类型与各 JoinMethod、`getCandidatesFromState`，跑 `test_join_bruteforce`/`test_join_ivf`。
4. 改 `apply`/`updateSideWithState`/`executeJoinWithState` 拷贝点与 emit 边界，跑全 join 单测 + `test_join_datasource_modes` 小矩阵。
5. 收敛触发/清理、移除废弃占位，回归 recall 与 p=1/2/4/8 扩展性，用 gperftools 对比 Populate 占比。
6. 回滚策略：保留 `.toolchains/code_backup` 基线；每阶段独立可回退。

## Open Questions

- ~~`Response`/`Collector`/下游 sink 是否能接受 `shared_ptr<const VectorRecord>` 传递？~~ **已定稿（决策 4）**：传输层保持 `unique_ptr` + move 不变，shared_ptr 仅用于算子内部状态层；转换点为 `apply` 入口一次 move；emit 边界保守保留 `unique_ptr` 构造，只消除候选循环内查询侧记录的重复深拷贝。
- `TwoTierWindowState` 的二级（global/被 rebuild）结构在改类型后与后台 rebuild 线程的 shared_ptr 生命周期是否需要额外同步点。
- 是否在本次顺带为 `getRecordsSnapshot` 增加“仅指针、无 expired 过滤”的快路径，供暴力扫描复用。

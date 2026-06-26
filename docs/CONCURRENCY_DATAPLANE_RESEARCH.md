# 并发数据路径优化可行性调研（local index / StorageManager / 算子间传递）

状态：调研与分析，不含实现。目标是评估“无锁结构 + 存储/数据结构底层优化”在当前 SageFlow VSJoin 链路上的真实收益与风险。

## 0. 先纠正一个前提

`std::shared_mutex` 不是“所有并发都串行”。它是多读并发、读写互斥：
- 多个 `query_for_join`（读）可同时持 `shared_lock`，彼此不串行。
- 只有 `insert/erase`（写）会与读互斥。

所以问题不是“有没有锁”，而是“每个结构的真实读写并发度是多少、写有多频繁、临界区有多大”。下面按这个标准逐个结构分析。

## 1. 线程模型与所有权（这是判断一切的前提）

证据：

- 每个 `ExecutionVertex` 一个线程，但同一个 Operator 实例被 N 个 vertex 线程共享：[execution_vertex.cpp:54-116](file:///Users/bytedance/icpp-demo/sageFlow/src/execution/execution_vertex.cpp#L54-L116)（`open()` 用 `call_once`，N 线程共享 `operator_`）。
- 算子间连接是 SPSC 队列矩阵：`upstream × downstream` 个独立队列，每个队列单生产者单消费者：[connection_strategy.cpp:10-44](file:///Users/bytedance/icpp-demo/sageFlow/src/execution/connection_strategy.cpp#L10-L44)。
- VSJoin local index 选择：写入 `localIndexIdForSlotAndSubtask(slot, subtask)`，查询 `queryLocalIndex(query_slot, subtask)`：[join_window_state_executor.cpp:41-46](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator_components/join_window_state_executor.cpp#L41-L46)、[vsjoin_method.cpp:57-76](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator_methods/vsjoin_method.cpp#L57-L76)。

关键结论 —— local index 的并发形态：

```
left local index[p]  ← 写：left-slot 在 subtask p 插入自己
                     ← 读：right-slot 的记录到来，在 subtask p 查 left 对侧
```

local index 不是单线程独占。它是「一个 slot 写 + 对侧 slot 读」。所以它至少是 single-writer + concurrent-reader。是否 multi-writer 取决于 multicast：当前多播会把一条记录写到多个 target subtask 的 local index（`apply()` 对 `target_subtasks` 循环 `updateSideWithState`），但每个 local index id 仍只被「拥有该 subtask 的那个写者」写。需要进一步确认：N 个 vertex 线程是否可能并发进入同一个 `subtask_index`。从 SPSC 矩阵看，每个 downstream subtask 由唯一 vertex 线程消费，因此**同一个 local index 实际是单写者**。这点对“能否用 SPSC/单写无锁结构”至关重要。

## 2. 逐结构分析

### 2.1 local BruteForce index（`Knn`，我刚改的）

现状：`unordered_set<uint64_t> live_ids_` + `shared_mutex`，写 O(1)、查询快照成 vector 再算距离。见 [knn.h:11-26](file:///Users/bytedance/icpp-demo/sageFlow/include/index/knn.h#L11-L26)。

真实并发：单写者（owner subtask）+ 单读者（对侧 slot 同 subtask）。写频率 = 入流速率；读频率 = 对侧入流速率。临界区：写是一次 set insert；读是一次 set→vector 拷贝（O(window)）。

是否串行化？只有「写的那一刻」会挡住「同时发生的读」，反之亦然。距离计算（真正的开销）发生在快照之后、锁外，不占锁。所以当前实现的锁竞争只在 `O(window)` 的指针拷贝上，不在距离计算上。

无锁可行性：
- 因为是 single-writer，可以用 RCU / 双缓冲 epoch 方案：写者维护一个 `shared_ptr<const vector<uint64_t>>` 成员快照，insert/erase 时构造新 vector 原子发布，读者 `atomic_load` 取快照零锁。代价是每次写 O(n) 重建。对滑动窗口高频 insert 不划算。
- 更契合的是 SPSC 增量：owner 维护一个 append-only 的 id 段 + 一个 tombstone 位集，读者顺序扫描。但删除可见性、内存回收复杂度高。
- 评估：**当前 `shared_mutex` 不是瓶颈**，因为读写各只有一方、临界区只覆盖指针拷贝。优先级低于 §2.3 storage 和 §2.4 候选物化。除非 profile 显示 `lock_wait_ns` 在 local index 上显著，否则不建议先上无锁。

### 2.2 global IVF index

现状：IVF 自带 `global_mutex_` + per-list `list_mutexes_` + `is_rebuilding_` cv，查询持 `shared_lock` 拷候选 list 后锁外算距离：[ivf.cpp:316-504](file:///Users/bytedance/icpp-demo/sageFlow/src/index/ivf.cpp#L316-L504)。它在写路径不被 VSJoin 在线插入，只被后台 rebuild 整体替换（`replace_index_by_id`）。

真实并发：多读者（所有 subtask 查对侧 global）+ 极低频写者（后台 rebuild 用 `replaceIndex` 换指针）。`BlankController::index_mutex_` 只锁“取指针”，rebuild 用原子换指针、旧查询继续持旧 shared_ptr 完成：[blank_controller.cpp:44-60](file:///Users/bytedance/icpp-demo/sageFlow/src/concurrency/blank_controller.cpp#L44-L60)。

评估：global 路径已经是接近无锁读 + 周期性原子替换的良好结构。**不需要改**。这正是“controller 只管指针、index 自管数据”设计的价值。

### 2.3 StorageManager（最大的隐藏串行点）

证据：所有 index 共享一个 `StorageManager`，`insert` 全程持 `unique_lock<shared_mutex> map_mutex_`：[storage_manager.cpp:15-28](file:///Users/bytedance/icpp-demo/sageFlow/src/storage/storage_manager.cpp#L15-L28)；`getVectorsByUids` 持 `shared_lock` 逐个查 map：[storage_manager.cpp:69-89](file:///Users/bytedance/icpp-demo/sageFlow/src/storage/storage_manager.cpp#L69-L89)。

问题：
1. 全局唯一写锁。所有 subtask、所有 slot 的每条 insert 都过同一把 `map_mutex_` 写锁 → 这才是真正把并行 insert 串行化的点，比 local index 的锁严重得多。
2. `getVectorsByUids` 对候选逐个 `map_.find`（哈希查找）→ candidate_fetch 的隐藏成本。
3. `erase` 用 swap-pop 改变 `records_` 顺序 → 与“顺序/批量/SIMD 扫描”天然冲突。

可行优化方向（调研结论，未实现）：
- 分片 storage：按 `uid % shard` 切多个 `map_+records_`，每片独立锁 → 写并发度提升到 shard 数。低风险、收益直接。
- per-partition storage：VSJoin 的 local 数据本就按 subtask 分区，可让每个 subtask 拥有自己的 storage 段，insert 完全无跨线程竞争。和 §2.1 single-writer 一致，是最契合当前架构的方向。
- 列式 / SoA 存储：把向量数据连续按维度排布（见 §3），让候选验证可批量 SIMD。这是 batch/SIMD 的前提。

### 2.4 候选物化路径（`getVectorsByUids` + VSJoin resolveUids）

VSJoin 查询返回 uid 后，还要 `resolveUidsToRecords` 用 WindowState snapshot 建 `unordered_map` 再查：之前已识别为 `O(snapshot+candidates)`。叠加 `getVectorsByUids` 的逐个 map.find，候选物化是“两次哈希”。

评估：这里的优化收益（去掉一层 map、直接返回 shared_ptr）比上无锁队列更直接。

### 2.5 算子间传递（队列 + Response/VectorData）

证据：
- 队列已经是无锁 SPSC ring buffer，head/tail 分缓存行 `alignas(64)` 防伪共享：[ring_buffer_queue.cpp:8-38](file:///Users/bytedance/icpp-demo/sageFlow/src/execution/ring_buffer_queue.cpp#L8-L38)、[ring_buffer_queue.h:28-34](file:///Users/bytedance/icpp-demo/sageFlow/include/execution/ring_buffer_queue.h#L28-L34)。
- 但传的是 `Response{unique_ptr<VectorRecord>}`，`VectorData` 拷贝是深拷贝 `char[]`：[data_types.cpp:25-28](file:///Users/bytedance/icpp-demo/sageFlow/src/common/data_types.cpp#L25-L28)。
- `Response` 拷贝构造也会深拷贝 record：[data_types.h Response copy ctor]。

评估：队列本身已经无锁，不是瓶颈。真正成本是 record 的深拷贝和 `char[]` 分配（这与已存在的 `vector-window-lifecycle` change 的 shared_ptr 零拷贝目标一致）。所以“队列换无锁”收益几乎为零（已经是无锁），“数据载体减少深拷贝/改 SoA”收益大。

## 3. VectorData 布局与 SIMD/batch 适配

现状：`VectorData = {dim, type, unique_ptr<char[]>}`，每条记录一块独立堆内存（AoS，散布）：[data_types.h:21-44]。距离计算逐条调用、逐元素标量循环：[compute_engine.cpp EuclideanDistanceImpl/Similarity]。

问题：
- 候选验证时，N 个候选的向量在堆上不连续 → cache 不友好，且无法一次性 SIMD 多候选。
- `double` 累加 + 标量循环，未用 AVX/NEON。

方向（调研结论）：
1. 候选验证批处理 API：`verifyBatch(query, candidate_block)`，要求候选向量连续存放（SoA / packed block）。
2. storage 段按分区连续存 float32 → 候选验证可直接对连续内存做 SIMD。
3. 距离 kernel 上 AVX2/AVX-512/NEON（参考 FAISS/hnswlib/Elastic simdvec，已在 VSJOIN_OPTIMIZATION_RESEARCH.md 收录）。

依赖关系：SIMD/batch 的前提是连续内存布局，连续布局的前提是 storage 改造。所以顺序应是 storage 布局 → 批量候选物化 → SIMD kernel，而不是先上无锁队列。

## 4. 结论与优先级（按收益/风险比）

| 方向 | 真实瓶颈？ | 建议 | 风险 |
|---|---|---|---|
| 算子间队列换无锁 | 否（已是无锁 SPSC） | 不做 | - |
| local `Knn` 无锁化 | 否（单写单读、临界区小） | 暂不做，先 profile `lock_wait` | 中（删除可见性/回收） |
| global IVF 改造 | 否（已无锁读+原子替换） | 不做 | - |
| StorageManager 全局写锁 | 是（所有 insert 串行） | 优先：分片 or per-partition storage | 中 |
| 候选物化两次哈希 | 是（candidate_fetch 热点） | 优先：去一层 map | 低 |
| VectorData AoS + 标量距离 | 是（无法 batch/SIMD） | 中期：SoA 连续布局 + SIMD kernel | 高（接口大改） |
| Response/VectorData 深拷贝 | 是 | 复用 `vector-window-lifecycle` 的 shared_ptr 零拷贝 | 中 |

一句话：**“无锁队列/无锁 set”不是当前链路的真正瓶颈**；真正值得做的是 (1) StorageManager 的全局写锁分片/分区化，(2) 候选物化去重复哈希，(3) 为 batch/SIMD 做连续内存布局。这三者按依赖顺序推进，比直接换无锁结构收益高、风险可控。

## 5. 建议的下一步（仍不实现，先确认方向）

1. 先加 profile 证据：在 p=1/2/4/8 下采集 `lock_wait_ns` 在 storage insert / local index / candidate_fetch 上的占比，用数据确认 §4 的排序。
2. 若 storage 写锁确认为瓶颈，落 OpenSpec change：per-partition storage（与 single-writer 模型对齐）。
3. SoA + SIMD 作为后续 change，依赖 storage 布局先行。

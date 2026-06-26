# Design: Join Pair Materialization

## 1. Context

### 1.1 现状（基于当前代码的事实）

算子内部与算子间存在两套不同的记录所有权语义：

| 层 | 类型 | 所有权 | 拷贝代价 |
| --- | --- | --- | --- |
| 算子内部状态/候选 | `RecordView = shared_ptr<const VectorRecord>` | 共享只读 | 仅引用计数 |
| 算子间传输 | `Response{ unique_ptr<VectorRecord> }` | 独占 | 深拷贝 `char[]` 向量体 |

Join 命中后的物化发生在内部层与传输层的边界 `JoinResultEmitter`：

```
getCandidates() -> vector<RecordView>         // 零拷贝，shared_ptr
executeJoin(data_ptr=const VectorRecord*, ...) // data_view 是 RecordView，但只传裸指针
  for cand in candidates (RecordView):
    appendJoinedResult(*data_ptr, *cand, slot, output)
      make_unique<VectorRecord>(current)   // 深拷贝 1（被 JoinFunc 签名逼出）
      make_unique<VectorRecord>(candidate) // 深拷贝 2（被 JoinFunc 签名逼出）
      join_func_->Execute(lhs, rhs)        // 深拷贝 3（concat 成 2*dim 新向量）
      output.emplace_back(slot, unique_ptr<VectorRecord>)
emit(output): collector.collect(make_unique<Response>(...))  // move 进 transport
```

`JoinFunc` 签名：

```cpp
using JoinFunc = std::function<std::unique_ptr<VectorRecord>(
    std::unique_ptr<VectorRecord>&, std::unique_ptr<VectorRecord>&)>;
```

签名要求传入可被 move 的 `unique_ptr&`，而 emit 边界手里是 `RecordView`/`const VectorRecord*`，无法直接喂入，必须先深拷贝出 `unique_ptr`。

### 1.2 传输链路类型契约

```
Operator::apply(Response&&, slot, Collector&, ctx)
  -> Collector::collect(unique_ptr<Response>, slot)
     -> ResultPartition::emit(Response&&, slot)
        -> IPartitioner::partition(const Response&, num_channels)  // 取 record_->data_ / timestamp_ 作 key
        -> RingBufferQueue::push(TaggedResponse{Response, slot})    // SPSC，move 整个 Response
  下游 InputGate::read() -> TaggedResponse -> Operator::apply(Response&&, ...)
```

关键约束：
- 分区器从 `Response.record_` 读取路由 key（`KeyPartitioner` 用 `timestamp_`，`VectorHashPartitioner`/`LSHPartitionerAdapter` 用 `data_`）。
- `Collector::collect` 在广播（slot == -1）分支会 `make_unique<Response>(*record)` 深拷贝。
- `Response` 显式定义了拷贝构造与拷贝赋值（深拷贝 record_/records_），因此**隐式移动构造被抑制**：`TaggedResponse(Response res,...) : response(std::move(res))` 实际调用拷贝构造 = 每次入队深拷贝整个 Response。

### 1.3 目标场景

"识别两条数据流窗口内相似的向量对 → 把两条原始记录交给下游 LLM 任务做前处理"。下游需要左右两条**原始 payload**，不需要拼接向量。配对一旦在 Join 算子建立，下游通常不再按向量内容路由。

## 2. Goals / Non-Goals

### Goals
- 把 Join 结果物化语义从"特征拼接"改为"配对引用"。
- emit 热路径每对相似结果做到 0 次 `VectorData` 深拷贝（仅引用计数）。
- 传输层以**加法兼容**方式承载配对，现存 `Record`/`List` 路径与所有现存算子零改动。
- 给出面向 LLM 前处理的 join function 推荐契约与批处理友好性。

### Non-Goals
- 不在本 change 内删除既有 concat join function（保留为可选）。
- 不引入 watermark/窗口物化（属于 `stream-window-data-plane`）。
- 不改 SPSC 队列实现、不改 `Collector`/`RingBufferQueue`/`TaggedResponse` 的方法签名。
- 不实现 SIMD/批量距离 kernel（属于 `vector-data-plane`）。

## 3. Decisions

### 3.1 传输层：加法式 RecordPair 通道（不破坏现有结构）

在 `Response` 上新增一个可选配对载荷，新增枚举值 `ResponseType::RecordPair`：

```cpp
enum class ResponseType { None, Record, List, Exit, EOFMarker, RecordPair };  // 末尾追加

struct RecordPairPayload {
  RecordView left;     // shared_ptr<const VectorRecord>，零拷贝引用左记录
  RecordView right;    // 零拷贝引用右记录
  double similarity;   // join 命中时已算出的相似度/距离
};

struct Response {
  ResponseType type_;
  std::unique_ptr<VectorRecord> record_;                                   // 原有
  std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records_;    // 原有
  std::unique_ptr<RecordPairPayload> pair_;                                // 新增，仅 RecordPair 使用
  // ... 构造函数新增一个 RecordPair 重载
};
```

理由：
- `Record`/`List` 两条既有分支的内存布局与行为完全不变，老算子 `switch(type_)` 不会进入新分支 → 加法兼容。
- 用 `RecordView`（shared_ptr）而非 `unique_ptr` 承载配对，使 emit 边界手里的共享引用可以直接转移，无需深拷贝。
- `similarity` 随结果携带，下游 LLM 前处理可直接用于阈值过滤/排序，无需重算。

备选方案与否决理由：
- **A. 复用 `records_` 装 [left,right]**：会与现有 List 语义（一个算子产出的多条独立记录）混淆，下游无法区分"两条配对"与"两条独立结果"，否决。
- **B. 新增独立的 PairResponse 类型并改队列泛型**：要改 `TaggedResponse`/`IQueue`/`RingBufferQueue` 全链路签名，破坏面太大，否决。
- **C（采用）. 在 Response 内加可选 pair_ 通道**：改动局部、加法兼容。

### 3.2 修正 Response 移动语义（消除入队隐性深拷贝）

为 `Response` 显式补齐移动构造/移动赋值（`= default` 或手写 move），保留现有拷贝语义供广播分支使用：

```cpp
Response(Response&&) noexcept = default;
Response& operator=(Response&&) noexcept = default;
Response(const Response&);            // 保留：广播 (slot==-1) 需要深拷贝
Response& operator=(const Response&); // 保留
```

效果：`TaggedResponse` 入队时 `std::move(Response)` 真正走移动，`RecordPair` 入队只转移 3 个指针/标量，零向量拷贝。这对现有 `Record` 路径同样是收益（消除既有的入队深拷贝）。

### 3.3 emit 热路径零拷贝契约

`JoinWindowStateExecutor::executeJoin` 当前向 emitter 传 `const VectorRecord* data_ptr`。改为传 `RecordView`（`apply` 中 `data_view` 本就是 RecordView，可直接传递），候选侧 `cand` 已是 RecordView。emitter 新增配对产出路径：

```cpp
// 概念签名（实现留待 tasks 阶段）
void appendPair(const RecordView& probe, const RecordView& cand, int slot,
                double similarity, std::vector<TaggedResult>& out) const {
  RecordView left  = (slot == left_slot_id_) ? probe : cand;
  RecordView right = (slot == left_slot_id_) ? cand  : probe;
  out.emplace_back(slot, Response{ResponseType::RecordPair,
                                  std::make_unique<RecordPairPayload>(left, right, similarity)});
}
```

无 `VectorData` 深拷贝，只增引用计数 + 一个小 payload 分配。

### 3.4 join function 契约（面向 LLM 前处理）

定义三档物化模式。当前实现为了保护既有 sink、测试和下游单记录算子，保持 `CONCAT` 为默认；面向 LLM 前处理的推荐模式是显式选择 `PAIR_PASSTHROUGH`：

| 模式 | 输出 | 向量拷贝 | 适用 |
| --- | --- | --- | --- |
| **PairPassthrough（推荐）** | `RecordPair{left, right, sim}` | 0 | LLM 前处理：下游需要两条原始 payload |
| **Projection（可选）** | `RecordPair` 但 left/right 仅保留下游所需字段视图 | 0（视图）/按需 | 下游只需部分字段、需裁剪传输量 |
| **Concat（兼容默认）** | 一条 `2*dim` 新向量记录 | 1 | 需要把配对再喂给"吃单条向量"的下游算子（非本场景） |

推荐给用户场景的 join function 形态：不做向量运算，只判定/打包配对，相似度由 Join 内部已算结果透传。下游 LLM 前处理算子按 batch 拉取 `RecordPair`，对每对取 `left`/`right` 的原始 payload 组 prompt。默认切换到 pair-passthrough 应作为后续迁移动作，等待 pair-aware downstream API 稳定后再做。

### 3.5 分区器对 RecordPair 的路由契约

- 默认：`RecordPair` 以 `left` 记录作为代表向量/时间戳供现有分区器取 key（`partition` 内对 `type_==RecordPair` 取 `pair_->left`）。
- 推荐：相似度识别 → LLM 前处理场景，配对已建立，下游声明 `RoundRobinPartitioner` 直接绕过内容路由，负载均衡且零额外计算。
- 广播分支（slot==-1）：`Response` 拷贝构造需正确深拷贝 `pair_`（拷贝 shared_ptr，浅层共享底层记录，符合只读语义）。

### 3.6 配对载荷所有权与分配器策略（tcmalloc 跨线程 free 顾虑）

`RecordPairPayload` 用什么所有权承载左右记录，直接决定跨线程内存开销。需区分两类开销：

- **(A) 控制块原子计数争用（true sharing）**：`shared_ptr` 的引用计数是原子操作，仅当多核同时增减**同一个控制块**才有 cache-line bouncing。本引擎是 SPSC + owner-computes：一条记录的 `RecordView` 由上游单线程构造、下游单线程析构，同一控制块极少被两核并发触碰，故 (A) 在本场景很轻。
- **(B) 跨线程 free（tcmalloc 慢路径）**：tcmalloc/jemalloc/mimalloc 均为 per-thread cache 分配器。`make_shared<VectorRecord>` 在**线程 A** 分配（控制块+对象同一块内存），`RecordView` 流到**线程 B** 做最后一次析构时，内存归还进 B 的 thread cache，再经 central free list 回流给 A——这种跨线程分配/释放往返正是 tcmalloc 最贵的模式。已有 profile（`vector-window-lifecycle/proposal.md`）显示 `make_shared<const VectorRecord>(*r)` 每元素深拷贝导致 tcmalloc Populate 占约 40% CPU；注意该 40% 主因是**深拷贝制造海量短命对象**，而非控制块原子计数。RecordView 配对方案本身消除了深拷贝（方向正确），但会把"窗口内同线程析构"变成"下游 LLM 算子跨线程析构"，可能让 (B) 冒头。

三个候选载荷形态：

| 方案 | pair 载荷 | 跨线程 free 风险 | emit 向量拷贝 | 备注 |
| --- | --- | --- | --- | --- |
| **R1（默认）** | `RecordView left; RecordView right;` 直接跨线程流动 | 中：下游线程做最后析构，跨线程归还 | 0 | 直接消灭深拷贝根因 |
| **R2** | 单 `unique_ptr<结构化配对记录>`（结构化携带 left/right payload，**非无意义 concat**） | 低：与现有 Record 路径同构，move 语义 | 1（脱离窗口生命周期，不可免） | 退化为传统拷贝换确定性 |
| **R3** | 仅 `(left_uid, right_uid, similarity)`，原始向量留在 StorageManager，下游按需取 | 最低 | 0 | 正确性绑定 storage 过期语义（耦合 `stream-window-data-plane`） |

决策：**默认采用 R1 + `make_shared`**（控制块与对象单次分配/单次释放）。把跨线程 free 开销列为**待测项而非待猜项**：先实现 R1，用 microbench 对比 R1/R2/R3 的分配计数、p99、tcmalloc Populate 占比。仅当 profile 证明 (B) 成为新瓶颈，才启用下面的 arena 备选或切 R3。

**arena/pool allocator 备选（绕开 tcmalloc 跨线程往返）**：

- 原理：为 join 输出的 `VectorRecord`/控制块预留一块按 pipeline 或 window-epoch 划分的连续 arena，用 `std::allocate_shared` 绑定该 arena 分配器。分配走 bump-pointer / per-arena free list，释放不立即还给 tcmalloc；整代记录在 window epoch 结束时**整块 reset**，把 N 次跨线程 small-free 摊销成一次大块回收。
- 业界对照：Seastar 用 `foreign_ptr<>` 显式把对象析构送回 home shard 执行（share-nothing，跨核只走消息传递）；mimalloc 用 per-page `thread_free` 原子链表把跨线程释放的块延迟归还给 owner thread，避免污染分配线程的快路径。本方案是同一思想的"用户态 arena"版本：让释放路径可预测、与分配线程解耦。
- 落地约束：arena 生命周期必须覆盖下游对该 pair 的全部使用（LLM 前处理消费完成前不能 reset），因此 arena epoch 需与窗口/批次边界对齐；这条约束在 tasks 中作为前置条件验证。

## 4. Risks / Trade-offs

- **传输层枚举扩展**：所有 `switch(ResponseType)` 的现存算子需确认默认分支不误处理 `RecordPair`。缓解：新增值放末尾，审查所有 switch，缺失分支按 None 忽略并加测试。
- **默认物化语义切换**：依赖 concat 输出的既有测试会受影响。缓解：concat 作为具名可选 join function 保留，迁移测试改用 PairPassthrough，并保留一条 concat 回归。
- **shared_ptr 生命周期**：`RecordPair` 持有 `RecordView`，可能延长底层记录存活到窗口 evict 之后。缓解：这是预期行为（下游还在用），与窗口状态的 evict 解耦；只要窗口存的也是 RecordView，引用计数自然管理，无悬垂。
- **路由代表向量约定**：若下游依赖按内容路由又用了 RecordPair，需显式选择 left/right 代表。缓解：文档约定 + 默认 left + 推荐 RoundRobin。
- **shared_ptr 跨线程 free（tcmalloc 慢路径）**：RecordView 跨线程流到下游析构，可能把释放变成跨 thread-cache 往返。缓解：默认 `make_shared` 减半分配/释放次数；列为 microbench 待测项；必要时启用 arena allocator（见 3.6）或切 R3，而非退回深拷贝。

## 5. Migration Plan

1. 加 `ResponseType::RecordPair` + `RecordPairPayload` + `Response::pair_` 与构造（加法兼容，先不接线）。
2. 补 `Response` 移动语义；加入队分配计数 microbench 验证 `Record` 路径也零拷贝。
3. emitter 增 `appendPair` 路径与 RecordView 传参；保留旧 `appendJoinedResult` 不动。
4. 提供 PairPassthrough join function；datasource 测试新增一条 PairPassthrough 用例，与现有 concat 用例并存对比。
5. 分区器/Collector 广播分支补 `RecordPair` 处理与测试。
6. 全量小规模 bruteforce/vsjoin recall/precision 回归 + emit 分配计数对比（2~3 → 0）。

## 6. Open Questions

- 下游 LLM 前处理算子是独立新算子，还是复用现有 sink？（影响 PairPassthrough 的消费端 API 落点）
- 是否需要在 `RecordPair` 内携带 join 侧 slot/window 元信息供下游溯源？
- Projection 模式的字段裁剪是否在本仓做，还是留给 SAGE 编排层？

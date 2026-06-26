## Why

当前 Join 的候选物化把"配对关系"误做成了"特征拼接"，并在 emit 热路径产生不必要的深拷贝。

证据（基于当前代码）：

- 每命中一对相似记录，`JoinResultEmitter::appendJoinedResult` 发生 3 次 `VectorRecord` 深拷贝：
  - `make_unique<VectorRecord>(current)` —— 拷贝左/右探测记录；
  - `make_unique<VectorRecord>(candidate)` —— 拷贝候选记录；
  - `join_func_->Execute(lhs, rhs)` —— 业务 join function 把两个向量 concat 成一个 `2*dim` 的新向量记录（最大的一次分配）。
- 前两次拷贝完全是被 `JoinFunc` 签名 `std::function<unique_ptr<VectorRecord>(unique_ptr<VectorRecord>&, unique_ptr<VectorRecord>&)>` 逼出来的：候选取数 `getCandidates` 返回的已经是 `std::vector<RecordView>`（`shared_ptr<const VectorRecord>`），探测记录 `data_view` 也已是 `RecordView`，emit 边界手里本来就是只读共享引用。
- 第三次拼接向量在目标场景里是纯浪费：本 change 的目标场景是"识别两条流窗口内相似的向量对，把两条原始记录交给下游 LLM 前处理"。LLM 需要的是两条原始 payload，不是 `2*dim` 拼接向量。当前测试 sink 也只从 `uid` 解码 `left_id/right_id`，从不消费拼接出来的向量体。

根因：算子内部状态层已升级为 `RecordView`（共享、零拷贝），但算子间传输层 `Response` 只支持 `unique_ptr<VectorRecord>`（独占），且 `JoinFunc` 把"配对"语义压成了"产出一条新记录"。两者错配，导致每个输出对在 emit 边界被迫深拷贝 2~3 份向量体。

## What Changes

- 新增 Join 结果的"配对引用"物化语义：输出 `(left, right, similarity)`，其中 `left`/`right` 是对两条原始记录的只读共享引用（`RecordView`）。当前实现保持 legacy concat 为兼容默认，pair-passthrough 通过配置显式选择。
- 在传输层 `Response` 增加一个**加法兼容**的配对载荷通道与 `ResponseType::RecordPair`，保证现有 `Record`/`List` 路径与所有现存算子零改动可用。
- 定义 emit 热路径的零向量拷贝契约：命中一对相似记录时，emit 只增加引用计数，不深拷贝 `VectorData` 的 `char[]`。
- 定义面向 LLM 前处理的 join function 契约：推荐 pair-passthrough（携带左右引用 + 相似度），可选 projection（仅保留下游需要的 payload 字段），明确把"concat 成大向量"限定为需要单记录输出的兼容路径。
- 修正传输层移动语义：`Response` 当前显式声明拷贝构造/拷贝赋值，按 C++ 规则抑制了隐式移动构造，导致 `TaggedResponse` 入队时 `std::move(Response)` 退化为深拷贝；本 change 要求补齐 `Response` 的移动语义，使入队真正零拷贝。
- 明确分区器对 `RecordPair` 的路由契约：默认以 left 记录作为代表向量/时间戳；相似度识别场景下游通常已无需再按内容路由，可声明 RoundRobin 直接绕过。
- 本 change 只做设计与任务拆分，不在本 change 内直接重构 `JoinResultEmitter` 实现，也不强制替换所有 join function。

## Capabilities

### New Capabilities

- `join-result-materialization`: Join 命中结果的物化语义、配对引用契约、emit 零拷贝契约、面向下游（含 LLM 前处理）的 join function 契约。

### Modified Capabilities

<!-- openspec/specs/ 当前为空；本 change 不修改已有 active change 的 spec，作为新增能力提出。 -->

## Impact

- 设计影响：
  - `JoinResultEmitter::appendJoinedResult` / `JoinResultEmitter::emit` 的输出物化路径。
  - `JoinWindowStateExecutor::executeJoin` 向 emitter 传递探测记录的方式（裸指针 → RecordView）。
  - `Response` / `ResponseType` / `TaggedResponse` 的载荷与移动语义。
  - `Collector::collect` / `ResultPartition::emit` / `IPartitioner::partition` 对 `RecordPair` 的处理与默认路由 key。
  - `JoinFunc` / `JoinFunction::Execute` 的结果产出语义，以及下游 sink / LLM 前处理算子的消费约定。
- 验证影响：
  - 需要新增 emit 路径分配计数 microbench（每对相似结果的 `VectorData` 分配次数：当前约 2~3 → 目标 0）。
  - 需要新增配对正确性测试：`(left_uid, right_uid, similarity)` 与 ground truth 对齐，且左右记录内容未被破坏。
  - 需要回归现有 `bruteforce` / `vsjoin` 小规模 recall/precision，确保物化语义切换不改变命中集合。
- 风险：
  - `Response` 增加配对通道属于传输层语义扩展，必须保证现存算子（filter/map/topk/sink）对未知 `RecordPair` 不误处理；默认走加法兼容、老分支不动。
  - 切换默认 join function 物化方式会影响依赖"输出拼接向量"的既有测试；需保留旧 concat 行为作为可选 join function，而非删除。
  - 分区器对 `RecordPair` 取代表向量的约定若与下游期望不一致，可能改变路由分布；相似度识别场景建议下游显式声明 RoundRobin。

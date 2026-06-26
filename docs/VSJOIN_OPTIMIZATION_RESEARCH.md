# VSJoin 后续优化调研文档

更新日期：2026-06-24

本文面向 SageFlow 中 VSJoin 的后续优化。目标不是证明当前方案已经有效，而是整理流处理、多核 Join、向量相似度 Join、动态 ANN、向量数据库、cache/NUMA/SIMD/GPU 等方向的研究进展，并把可迁移的优化方向转成可验证的工程假设。

## 0. 资料来源与边界

本次调研使用了四类材料：

- 已有 PPTX：`/Users/bytedance/icpp-demo/多核处理器上基于滑动窗口的高吞吐量向量流连接.pptx`。PPTX 共 22 页，其中 7-15 页为隐藏页；图表多为图片，缺少原始 CSV、commit 和机器配置，因此只能作为设计意图和趋势线索。
- 本地代码事实：以 `sageFlow/include`、`sageFlow/src`、`sageFlow/test`、`sageFlow/config` 为准。
- 外部论文与系统：覆盖 VLDB/SIGMOD/OSDI/SOSP/CIDR/DEBS/ATC/ICDE 及向量数据库官方文档。
- 既有测试记忆：VSJoin 近期正确性可达 recall/precision 1.0，但吞吐低于 bruteforce，热点集中在 `candidate_fetch` 和内存分配开销。该结论仍需后续用当前 commit 重新跑实验确认。

重要边界：

- 本文不声称任何优化已经提升吞吐或召回；所有优化均列为待实验验证。
- 对 VSJoin 的判断优先基于当前代码，而不是 PPTX 或历史文档。
- GPU/FPGA、learned filter、动态图修边等方向都列为研究路线，不应直接进入主路径。

## 1. 总体结论

VSJoin 不应继续被理解为“每条记录做一次 ANN range query”。更有潜力的方向是把 sliding-window vector join 拆成四个可优化层次：

1. **窗口与一致性层**：event-time/window snapshot、过期可见性、pair 去重、owner-computes。
2. **候选生成层**：local mutable buffer、global immutable snapshot、filter-aware search、hot partition 路由。
3. **候选复用层**：micro-batch query ordering、near-miss candidate cache、bucket graph 调度、work sharing。
4. **硬件层**：batch SIMD verification、cache-friendly layout、NUMA-local state、per-thread scratch/allocator。

短期最值得优先做的不是更换 ANN 算法，而是：

- 修正/验证 Local BruteForce 是否真正 partition-local。
- 消除每次查询对 WindowState snapshot 建 UID map 的 `O(window)` 成本。
- 把 `candidate_fetch`、`verification`、`window insert/evict`、`index insert/delete`、`emit`、`allocation`、`lock wait`、`cache/NUMA` 指标拆开。
- 建立 brute-force parity 的 event-time ground truth，避免吞吐提升掩盖 recall/duplicates/p99 退化。
- 做 batch SIMD verification 和 partition-local candidate buffer 的 microbenchmark。

中期可考虑：

- time-bucket / sealed segment 窗口状态，过期优先整段 drop 或 tombstone + segment vacuum。
- hot partition 选择性拆分或 secondary routing，而不是全局提高 `multicast_k`。
- query 间 work sharing，复用相近查询的候选、entry point 或 graph traversal frontier。
- filter-aware ANN，把 side/window/partition/time/owner 过滤前推到候选生成阶段。

长期研究路线：

- 动态图 ANN 的局部修边或 incremental update。
- learned cardinality/neighbor-existence filter。
- GPU sidecar 或 FPGA/NDP 加速。

## 2. PPTX 基线

PPTX 的核心问题定义是：在多核 CPU 上对两条高维向量流执行滑动窗口相似度 Join，同时维护实时窗口状态、低延迟输出和高吞吐执行。

PPTX 已经覆盖的相关工作包括：

- 多核结构化流 Join：Low-latency Handshake Join、SplitJoin、PIM-Tree、Scale-OIJ。
- 向量相似度 Join：FGF-Hilbert、EDBT 2022 LSH 分布式 Join、SimJoin、Xling。
- 流式向量系统：VectraFlow、VStream。
- 动态 ANN：FreshDiskANN、SPFresh。
- 系统集成：VBase、DiskJoin、HDR-Tree、ADSSJ。

PPTX 中已有 VSJoin 设想：

- LSH / Space Filling Curve 等相似性感知分区。
- Local mutable index + Global immutable index。
- 写入走 Local，查询走 Local + Global。
- 过期记录 lazy 标记。
- 后台重建 Global index。
- `P * V` 逻辑分区映射到物理线程，用于负载均衡。
- 边界向量多播以修复分区边界召回。

PPTX 暴露的缺口：

- 图 16-17 的实验图缺少原始数据、配置和 commit，只能作为趋势，不可作为论文结论。
- 需要明确精确/近似语义、recall 目标、重复输出处理、乱序到达、窗口过期安全时间戳、lazy 删除可见性。
- baseline 的单机多线程改造风险较高，ADSSJ、EDBT22、VectraFlow 是否公平需要单独论证。

## 3. 当前代码事实

当前实现已经有 VSJoin 骨架，但不少 PPTX 中的组件尚未进入热路径。

关键路径：

- `JoinOperator::apply()` 先计算 VSJoin target subtasks，再对每个 target 执行插入和查询：[`src/operator/join_operator.cpp:369-397`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator.cpp#L369-L397)。
- 插入路径是 WindowState add、local index insert、safe eviction、batch erase：[`src/operator/join_operator_components/join_window_state_executor.cpp:81-135`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator_components/join_window_state_executor.cpp#L81-L135)。
- 候选路径是 Local Index + Global Index 查询，UID 去重后再从 WindowState snapshot 解析：[`src/operator/join_operator_methods/vsjoin_method.cpp:32-55`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator_methods/vsjoin_method.cpp#L32-L55)。
- VSJoin 索引创建固定为 2 个 Global IVF + `2 * parallelism` 个 Local BruteForce：[`src/operator/utils/join_strategy_factory.cpp:75-107`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/utils/join_strategy_factory.cpp#L75-L107)。
- VSJoin WindowState 当前固定创建 `TwoTierWindowState`，没有使用 `PARTITIONED_VECTOR` 分支：[`src/operator/utils/join_strategy_factory.cpp:386-394`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/utils/join_strategy_factory.cpp#L386-L394)。
- Global rebuild 周期性 snapshot 全部分区、UID 去重、构建新 IVF、replace controller：[`src/operator/join_operator_components/vsjoin_global_index_rebuilder.cpp:83-167`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator_components/vsjoin_global_index_rebuilder.cpp#L83-L167)。

重要风险：

- Local BruteForce 可能不是真正 partition-local。当前 `Knn::query_for_join()` 扫全局 `StorageManager`，这会让“Local index”退化成全局扫描，需要优先验证和修复。
- `VSJoinMethod::resolveUidsToRecords()` 每次从 WindowState snapshot 构建 UID map，复杂度约为 `O(snapshot + candidates)`：[`src/operator/join_operator_methods/vsjoin_method.cpp:92-114`](file:///Users/bytedance/icpp-demo/sageFlow/src/operator/join_operator_methods/vsjoin_method.cpp#L92-L114)。
- `vsjoin_rebuild_threshold` 当前未参与触发 rebuild；rebuild loop 是固定 interval。
- `vsjoin_global_index_type=hnsw` 被配置/校验接受，但 factory 和 rebuilder 都硬编码 IVF。
- `AssignmentTable`、`LoadMonitor`、logical partition、真正 LSH multicast、HNSW Global 尚未接入主热路径。

这意味着后续优化前必须先做两件事：

1. 把代码行为和配置语义对齐。
2. 建立能暴露 current hot path 的 metrics 和 microbenchmark。

## 4. 领域研究进展

### 4.1 多核流处理与滑动窗口 Join

Low-latency Handshake Join 重新审视了现代硬件上的 stream join，基于 handshake join 的 NUMA-aware 数据流模式，指出原始 handshake join 吞吐和扩展性好但延迟高、输出顺序不确定，并提出 tuple expedition 和 punctuation 降低延迟、保证有序输出。来源：[PVLDB 2014 PDF](https://vldb.org/pvldb/vol7/p709-roy.pdf)。

SplitJoin 把 join 操作拆成 independent storing 和 processing steps，目标是减少全局协调、降低 pipeline 长度，在 USENIX ATC 2016 中报告吞吐最高提升 60%、延迟最高降低 3.3x。来源：[USENIX ATC 2016](https://www.usenix.org/system/files/conference/atc16/atc16_paper-najafi.pdf)。

PIM-Tree / Parallel Index-based Stream Join 针对 multicore CPU 上的动态流索引，提出 partitioned in-memory merge tree，可变组件 + 不可变组件 + 低成本并发控制。它对 VSJoin 的启发是读写分层、批量 merge、过期时合并清理，但 B+Tree/范围分区无法直接处理 dense vector。来源：[SIGMOD 2020 / DOI 10.1145/3318464.3380576](https://doi.org/10.1145/3318464.3380576)。

Scale-OIJ / OpenMLDB 使用双层跳表、SWMR、动态调度和增量聚合解决 key-based interval join 的数据倾斜与重复计算。对 VSJoin 的启发不是跳表本身，而是“共享数据结构 + 单写多读 + 动态调度 + 重叠窗口结果复用”。来源：[Scale-OIJ paper](https://intellistream.github.io/downloads/papers/Zhang-2023-OIJ-OpenMLDB_CR.pdf)。

Hardware-Conscious Stream Processing survey 总结了 stream processing 的 computation optimization、stream I/O、query deployment，并强调 NUMA/cache/shuffle/communication 对高并行流系统的影响。来源：[arXiv 2001.05667](https://ar5iv.org/html/2001.05667)。

StreamBox 是单机多核 stream engine，强调 watermark epoch、out-of-order epoch processing、cascading containers 和 NUMA locality；BriskStream 则在 shared-memory multicore DSPS 中用 NUMA relative-location 优化 operator placement。它们比 Storm/Flink/Spark 更接近 SageFlow 的单机多核 runtime。来源：[StreamBox USENIX ATC 2017](https://usenix.org/conference/atc17/technical-sessions/presentation/miao)、[BriskStream SIGMOD 2019](https://dl.acm.org/doi/10.1145/3299869.3300067)。

Naiad/Timely Dataflow 的 logical timestamp、frontier/progress tracking 和 notification 机制适合抽象 VSJoin 的 watermark、eviction 和 rebuild barrier。来源：[Naiad](https://www.microsoft.com/en-us/research/publication/naiad-a-timely-dataflow-system-2/)、[Timely Dataflow CACM](https://cacm.acm.org/research/incremental-iterative-data-processing-with-timely-dataflow/)。

对 VSJoin 的迁移要点：

- 不要用一个全局共享索引承担所有写路径。
- 状态所有权与候选验证任务可以解耦。
- 低延迟路径要避免长流水线和跨 core 逐条转发。
- 滑动窗口过期最好按 epoch/subindex 处理，而不是每条记录同步删除。
- 有序输出、重复输出和 late data 必须作为一等语义处理。
- worker-local ingress/probe/update queue 需要 queue depth、sojourn time、busy/backpressured/idle time 指标。
- work stealing 只能偷不绑定 mutable state 的 verification/batch work，不能偷 index ownership。

### 4.2 向量相似度 Join

早期精确 similarity join 包括 EGO / Super-EGO / grid order / filter-refine 等路线。它们适合作为精确 baseline 或小维度场景，但 dense embedding 维度升高后 cell 邻域爆炸，不适合作为 VSJoin 主热路径。来源：[EGO DOI](https://doi.org/10.1145/375663.375714)、[Super-EGO](http://www.ics.uci.edu/~dvk/code/SuperEGO.html)。

Streaming Similarity Self-Join 定义了 streaming similarity self-join，提出 time-dependent similarity、time horizon、MiniBatch 与 Streaming 框架，并把 time filtering 嵌入索引。来源：[PVLDB 2016](https://research.aalto.fi/en/publications/streaming-similarity-self-join)。

LSH-based similarity join 把高维 join 近似为 hash join，用 representative points 减少 lookup；优点是通信/候选可控，缺点是 data-unaware、参数敏感、分布偏斜下召回和负载都可能退化。来源：[TKDE 2017 DOI](https://doi.org/10.1109/TKDE.2016.2638838)。

SimJoin 面向向量数据库中的 approximate threshold join，核心是利用已处理点的 join window 结果和 join order selection，避免把每个向量当成独立 range query。来源：[SIGMOD/PACMMOD 2025 DOI 10.1145/3725403](https://dl.acm.org/doi/10.1145/3725403)。

后续 VectorJoin / work-sharing 方向进一步将 graph traversal 结果、out-of-range near-miss、merged index、early stopping 用于 join 间复用。来源：[arXiv 2603.16360](https://arxiv.org/html/2603.16360v1)。

DiskJoin 针对 SSD 上 billion-scale vector similarity self-join，提出 bucketization、bucket graph、probabilistic pruning、access batching、graph reordering 和 Belady cache。它是离线/批处理，但 bucket graph 调度和访问批处理对 VSJoin 的 partition scheduling 有启发。来源：[arXiv 2508.18494](https://arxiv.org/html/2508.18494v1)。

Xling/XJoin 用 learned metric-space filter 预测 query 是否有足够邻居，从而跳过无效搜索。它适合做保守预过滤或 routing 降级，不适合直接丢弃可能命中的候选。来源：[arXiv 2402.13397](https://arxiv.org/pdf/2402.13397)。

对 VSJoin 的迁移要点：

- 研究重点应从“单查询更快”扩展到“查询间候选复用”。
- 维护 near-miss / top-L 候选缓存可能比只缓存命中 pair 更有价值。
- join order、bucket order、micro-batch 内部排序会影响 cache locality 和 work sharing。
- learned filter 只能作为 conservative filter；需要在线校准和 fallback。
- 流式迁移必须处理过期、重复、并发一致性，不能直接照搬 batch SimJoin。

### 4.3 流式向量系统

VectraFlow 是 CIDR 2025 的流式向量处理系统，扩展数据流模型以支持 vector 类型、iV-Filter、V-TopK、V-Join 等算子。V-Join 通过聚类将向量分配到 centroid，类似 hash join 地做 cluster 内处理，并引入 Centroid OPList、batching、sorting、bucketing、early stopping 等优化。来源：[VectraFlow PDF](https://cs.brown.edu/people/malte/pub/papers/2025-cidr-vectraflow.pdf)。

VStream 是 PVLDB 2025 的分布式 streaming vector search 系统，提出 dynamic partitioner、hierarchical storage 和 hot-cold separation。其 dynamic partitioner 结合 LSH 和 space-filling curve，将高维向量编码为一维值并动态调整 partition boundary，以适应分布漂移和负载变化。来源：[PVLDB 2025 PDF](https://www.vldb.org/pvldb/vol18/p1593-gao.pdf)、[GitHub](https://github.com/ZJU-DAILY/VStream)。

ADSSJ / Adaptive Distributed Streaming Similarity Joins 在 Flink 上做 metric-space streaming similarity join，关注 velocity、distribution、concept drift 下的自适应重分区，并报告 latency、comparison ratio、data duplication ratio。来源：[DEBS 2023](https://research.tue.nl/en/publications/adaptive-distributed-streaming-similarity-joins/)。

对 VSJoin 的迁移要点：

- LSH + Space Filling Curve 是合理路线，但必须报告召回、重复、负载和边界查询成本。
- 动态分区应该围绕 hot partition 做局部调整，不应每次全局重分区。
- hot/cold separation 可迁移为 hot mutable buffer + sealed cold segment。
- VectraFlow 是最直接 baseline，但其公开版本更像系统原型，VSJoin 的 novelty 应落在多核并发、窗口状态、分区负载和双流 Join 上。

### 4.4 动态 ANN 与向量数据库

FreshDiskANN 面向 fresh ANNS，支持 insert/delete/search；插入进内存 TempIndex，删除进 DeleteList，后台 StreamingMerge 合并到长期 SSD 索引。来源：[arXiv 2105.09613](https://arxiv.org/pdf/2105.09613)。

SPFresh 基于 SPANN 聚类索引，使用 LIRE 做 Insert/Delete/Merge/Split/Reassign，目标是避免周期性全局 rebuild，并在 billion-scale disk-based vector index 上降低资源成本。来源：[SOSP 2023 DOI 10.1145/3600006.3613166](https://dl.acm.org/doi/pdf/10.1145/3600006.3613166)。

Filtered-DiskANN 将过滤条件纳入 graph ANN 结构，为标签/过滤条件构建更合适的图连接。对 VSJoin 来说，side、partition、time epoch、owner-subtask 都可以视为 filter label，尽量前推到 search 过程。来源：[WWW 2023 DOI 10.1145/3543507.3583552](https://dl.acm.org/doi/pdf/10.1145/3543507.3583552)。

动态图删除的共同结论：

- HNSW / graph ANN 的删除不能只看逻辑可见性；tombstone 会保留死边，长期影响导航、候选数量和召回。
- 硬删除需要修复 in-neighbor / out-neighbor，代价高且并发复杂。
- 滑动窗口场景更适合 tombstone + query filter + partition/segment 后台 vacuum。

向量数据库系统的共识：

- Milvus/Qdrant/Pinecone/Lucene/OpenSearch 更偏向 mutable buffer / growing segment + immutable segment + compaction。
- Vespa/Weaviate 更偏向大 mutable HNSW，但需要复杂的实时 CRUD、snapshot、transaction log 和 filtered search 支持。
- 对滑动窗口 Join，最可迁移的是 time-bucket sealed segment，而不是每条过期记录都同步修改大图。

关键来源：

- Milvus data processing and architecture：[Milvus Data Processing](https://milvus.io/docs/data_processing.md)、[Milvus Architecture](https://milvus.io/docs/architecture_overview.md)。
- Qdrant segment + WAL + optimizer + copy-on-write：[Qdrant Storage](https://qdrant.tech/documentation/concepts/storage/)、[Qdrant Optimizer](https://qdrant.tech/documentation/ops-optimization/optimizer/)。
- Pinecone serverless LSM slab architecture：[Pinecone Architecture](https://docs.pinecone.io/guides/get-started/database-architecture)。
- Vespa mutable HNSW：[Vespa HNSW](https://docs.vespa.ai/en/querying/approximate-nn-hnsw.html)。
- Elasticsearch/OpenSearch Lucene segment and kNN filtering：[Elasticsearch kNN](https://www.elastic.co/docs/solutions/search/vector/knn)、[OpenSearch kNN](https://docs.opensearch.org/latest/vector-search/vector-search-techniques/approximate-knn/)。
- Faiss index capabilities and batch search：[Faiss docs](https://faiss.ai/#)。
- hnswlib dynamic update/delete primitives：[hnswlib README](https://github.com/nmslib/hnswlib/blob/master/README.md)。

对 VSJoin 的迁移要点：

- 引入 `hot mutable buffer + sealed time-bucket segments`。
- 查询固定 read epoch，查 hot buffer + sealed snapshot。
- 过期优先整段 drop；无法整段 drop 时 tombstone + allow-list filter。
- 后台 compact/rebuild 只针对退化 partition。
- 指标必须包含 stale candidate、tombstone hit、vacuum/rebuild lag。

### 4.4.1 Filter-aware candidate fetch

VSJoin 当前更像“索引取候选后再按窗口/时间/side 校验”。filter-aware ANN 的核心是把过滤条件前推到 candidate fetch 内部，尽量在距离计算、邻居扩展或倒排桶扫描之前跳过无效 ID。

Lucene ACORN-1 / FilteredHnswGraphSearcher 的机制是：对于通过过滤的节点才进入 `toScore` 计算距离，不通过的节点进入 `toExplore`，必要时探索邻居的邻居，以避免过滤造成图断连。来源：[Lucene FilteredHnswGraphSearcher](https://lucene.apache.org/core/10_3_0/core/org/apache/lucene/util/hnsw/FilteredHnswGraphSearcher.html)、[Lucene PR #14160](https://github.com/apache/lucene/pull/14160)。

OpenSearch 的 efficient kNN filtering 在 kNN query 内部处理 filter，并在 filtered ANN 不足 `k` 但过滤集合足够时 fallback 到 exact search。来源：[OpenSearch efficient filtering](https://docs.opensearch.org/latest/vector-search/filter-search-knn/efficient-knn-filtering/)。

Faiss 的 `SearchParameters::sel` / `IDSelector` 可以在 IVF scanner 距离计算前跳过不允许的 ID。来源：[Faiss SearchParametersIVF](https://faiss.ai/cpp_api/struct/structfaiss_1_1SearchParametersIVF.html)。

VBase 和 pgvector iterative scan 说明了另一个方向：索引接口不只返回固定 TopK，而是支持 `Next` / iterative scan，过滤后不足时继续拉取更多候选。来源：[VBase OSDI 2023](https://www.usenix.org/conference/osdi23/presentation/zhang-qianxi)、[pgvector iterative scans](https://pgxn.org/dist/vector#iterative-index-scans)。

对 VSJoin 的接口建议：

- 增加 `SearchFilterView`，包含 `probe_side`、`allowed_side`、`partition_id`、`owner_subtask`、`window_min_ts`、`window_max_ts`、`snapshot_epoch`、`allow_bitmap` 和 reject counters。
- 扩展 `ConcurrencyManager::query_for_join()`，支持 `SearchParams{radius/k, ef, filter, filter_mode}`。
- 第一阶段先让 BruteForce/IVF 在距离计算前跳过不允许 ID；第二阶段再考虑 HNSW/graph 的 ACORN-style `toScore/toExplore`。
- side/partition/owner 可用 dense bitmap 或 Roaring；time/window 用 time-bucket bitmap + generation；查询时对 bitmap 做 snapshot intersection。
- 策略上，allow-list 很小走 exact allow-list，过滤比例中等走 filtered ANN，过滤比例很大走普通 ANN + 最小后置校验。
- 新增指标：`filter_accepts`、`filter_rejects`、`distance_evals_saved`、`two_hop_expansions`、`fallback_exact_count`、`allowlist_build_us`、`candidate_fetch_recall`。

### 4.5 自适应分区、负载均衡与 state migration

Partial Key Grouping / power-of-two choices、PStream、FlexSP 等研究表明，只对 hot keys 或 hot partitions 进行拆分，比全量 shuffle 更能兼顾负载均衡和状态成本。来源：[Partial Key Grouping](https://www.arxiv-vanity.com/papers/1504.00788/)、[FlexSP](https://dl.acm.org/doi/fullHtml/10.1145/3673038.3673157)。

Megaphone、DRRS、Kafka Streams warmup/standby replicas 等说明 state migration 应拆成细粒度子任务，并通过 changelog / staged handoff 降低暂停。来源：[Megaphone](https://dl.acm.org/doi/pdf/10.14778/3329772.3329777)、[Kafka Streams](https://kafka.apache.org/42/streams/developer-guide/running-app/)。

Rendezvous hashing / bounded-load consistent hashing 适合生成 primary + fallback owners，并控制最大负载。来源：[Rendezvous hashing](https://handwiki.org/wiki/Rendezvous_hashing)、[Consistent Hashing with Bounded Loads](https://research.google/pubs/pub45756/)。

对 VSJoin 的迁移要点：

- `multicast_k` 不应是全局固定旋钮；应由 hot partition、query selectivity、load 和 recall probe 决定。
- 状态所有权和候选验证任务应解耦：状态可由 primary owner 维护，candidate verification 可由 secondary worker 执行。
- 用 logical partition + physical worker mapping，但迁移粒度应是 epoch/segment/subindex，而不是单条 vector。
- 输出语义必须保留 owner-computes 或 pair UID TTL 去重。

### 4.6 Cache、NUMA、SIMD、allocator

Elasticsearch simdvec 表明向量搜索的 HNSW traversal、IVF scan 和 reranking 最终都归结为大量距离计算；其 hand-tuned AVX-512/NEON kernel、bulk scoring、prefetch、interleaved loading 在数据超过 CPU cache 后仍能显著提升吞吐。来源：[Elastic simdvec](https://www.elastic.co/search-labs/blog/elasticsearch-vector-search-simdvec-engine)。

Faiss 文档指出很多 index 支持 batch search，且 batch search 通常比逐个查询更快；Faiss implementation notes 中 L2 距离计算会根据 `nq * d` 阈值切换 direct loop 与 BLAS 路径。来源：[Faiss docs](https://faiss.ai/#)、[Faiss implementation notes](https://github.com/facebookresearch/faiss/wiki/Implementation-notes)。

HNSWLIB 的 L2 距离路径会根据 CPU 能力和维度对齐选择 AVX-512/AVX/SSE/残差函数，这说明 dim 是否为 16 倍数会影响 kernel。来源：[hnswlib `space_l2.h`](https://github.com/nmslib/hnswlib/blob/d9b3608c83d83b46c96e25088cb1d729b29dcfe9/hnswlib/space_l2.h#L214-L235)。

Intel VTune Memory Access、Linux perf c2c / lock contention 可以定位 LLC miss、remote DRAM、false sharing、锁等待。来源：[Intel VTune Memory Access](https://www.intel.com/content/www/us/en/docs/vtune-profiler/user-guide/2025-4/memory-access-analysis.html)、[Linux false sharing](https://docs.kernel.org/kernel-hacking/false-sharing.html)、[perf lock contention](https://perfwiki.github.io/main/lock-contention/)。

TCMalloc / jemalloc / mimalloc 的 thread/per-CPU cache 和 arena 机制对高频小对象分配有效，但可能增加 RSS 和跨线程 free 成本。来源：[TCMalloc design](https://google.github.io/tcmalloc/design)、[mimalloc](http://microsoft.github.io/mimalloc/)。

对 VSJoin 的迁移要点：

- `verification` 做 batch SIMD kernel，保留 scalar fallback。
- candidate 按内存地址或 vector id 排序以改善 prefetch/locality。
- 每线程 scratch buffer，减少临时 vector/set/map 分配。
- per-thread result buffer + batch flush，减少 emit 和 allocator p99。
- NUMA 下 subtask、state、local index、allocator arena 尽量同 socket。
- metrics counters 需要 cache-line padding，避免 false sharing。

### 4.7 GPU/FPGA/异构加速

Faiss/cuVS/CAGRA 等 GPU ANN 在大 batch、索引常驻显存、数据规模大时有明显优势。CAGRA 论文报告 large-batch throughput 对 CPU HNSW 有数量级提升，NVIDIA 也报告 Faiss+cuVS 的 build/latency/batch throughput 提升。来源：[CAGRA paper](https://arxiv.org/html/2308.15136)、[NVIDIA Faiss+cuVS](https://developer.nvidia.com/blog/enhancing-gpu-accelerated-vector-search-in-faiss-with-nvidia-cuvs/)。

但 GPU 对 VSJoin 不是短期默认路线，原因是：

- 单条流记录低延迟路径难以 amortize kernel launch 和 PCIe。
- 窗口 insert/delete/evict 高频变化，不适合静态 GPU index。
- Join 输出 materialization 可能比距离计算更重。
- recall、duplicate、窗口一致性仍由 CPU runtime 负责。

GPU 更适合作为中期 sidecar：

- 大窗口。
- query 可 micro-batch。
- candidate verification 占总耗时主导。
- 数据/索引可常驻显存或 PQ 后能放入显存。
- H2D/D2H 占比可控。

FPGA/NDP 更适合固定距离核、固定数据流和极高能效目标，目前不应进入主实现。

## 5. 优化方向矩阵

| 优先级 | 方向 | 核心假设 | 需要改动 | 验证指标 | 风险 |
|---|---|---|---|---|---|
| P0 | 代码事实对齐 | 当前 Local BruteForce 和配置语义可能不符合设计 | 修正 local index storage 范围；让 global/local index type 配置生效或文档标明不支持 | p=1 brute-force parity；local candidate count；index scan size | 修复后召回/吞吐曲线会变化 |
| P0 | Ground truth 与 metrics gate | 无完整指标会误判优化 | event-time window exact scanner；pair UID 去重；metrics TSV 扩展 | recall、precision、duplicates、p99、candidate count、allocation、lock wait | ground truth 成本高 |
| P1 | 消除 UID snapshot map | 每次查询 `O(window)` UID resolve 是候选阶段瓶颈 | WindowState 暴露 UID lookup / partition-local map；避免每次建 map | candidate_fetch_ns、allocation bytes、p99 | 生命周期和过期一致性 |
| P1 | 真正 partition-local candidate fetch | Local BruteForce 若扫全局 storage，会吞掉分区收益 | index/storage 按 local partition 隔离；查询只扫 target partition | local scan count、candidate count、recall | 多播重复与漏召回 |
| P1 | Batch SIMD verification | 高命中候选下距离计算和内存访问占主导 | scalar/AVX2/AVX512/NEON kernel；batch API；scratch buffer | similarity_ns、cycles/vector、LLC miss | 非对齐维度、精度、降频 |
| P1 | Per-thread buffers / allocator | 高频 vector/set/map 分配导致 p99 和 tcmalloc 压力 | reserve、thread-local buffers、flat hash set、object pool | alloc count/bytes、RSS、p99 | RSS 上升、跨线程释放 |
| P2 | Time-bucket sealed segments | 滑动窗口 FIFO 过期适合整段 drop | hot mutable + sealed segment；epoch bitset；snapshot read | eviction_ns、stale hit、vacuum lag | 查询 fanout 变宽 |
| P2 | Hot partition routing | 全局 multicast_k 会放大重复和写放大 | LoadMonitor 接入；hot-only secondary routing；bounded-load HRW | load imbalance、duplicates、recall | 迁移一致性复杂 |
| P2 | Work sharing / near-miss cache | 相近 query 的候选和 traversal 可复用 | micro-batch query ordering；candidate cache；bucket graph order | candidate_fetch_ns、cache hit、recall | stale cache、内存占用 |
| P2 | Filter-aware ANN | side/time/partition/owner 过滤应前推 | allow-list / bitset / IDSelector；tombstone filter | filtered-out before verify、stale hit | post-filter recall 不足 |
| P2 | Rebuild/vacuum 触发器 | 固定 interval rebuild 不适应负载 | threshold、deleted ratio、visited-deleted ratio、probe recall | rebuild cost、freshness lag、recall drift | 触发过频或过迟 |
| P3 | Learned filter | 能跳过低收益 global probe | conservative cardinality estimator + fallback | false negative、skipped probes、recall | concept drift |
| P3 | GPU sidecar | 大 batch verification 可被 GPU 加速 | batch transfer、GPU verification/index prototype | end-to-end p99、H2D/D2H、recall | 小窗口/单条低延迟退化 |

## 6. 建议路线图

### Stage 0：把当前 VSJoin 行为锁住

目标：先证明当前实现的真实行为，避免在错误假设上优化。

任务：

- 检查 `Knn::query_for_join()` 和 `StorageManager` 是否导致 Local BruteForce 扫全局。
- 检查 `vsjoin_global_index_type` / `vsjoin_local_index_type` 是否应该生效；若短期不支持，配置校验和文档要一致。
- 给 `VSJoinMethod::resolveUidsToRecords()` 加指标，量化 snapshot map 成本。
- 给 Global rebuilder 加 `threshold` 实际触发记录或明确移除该配置。

验证：

```bash
cd /Users/bytedance/icpp-demo/sageFlow
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DSAGEFLOW_ENABLE_METRICS=ON
cmake --build build --target test_vsjoin_factory test_vsjoin_method test_vsjoin_operator_path test_vsjoin_routing test_vsjoin_rebuild test_vsjoin_load_balancing -j $(sysctl -n hw.ncpu)
ctest --test-dir build --output-on-failure -R 'vsjoin|partition_assignment|load_monitor'
```

### Stage 1：建立优化准入 gate

目标：任何优化都必须同时报告正确性、性能、尾延迟和资源指标。

任务：

- event-time ground truth：对每条输入事件按 active opposite window 精确扫描。
- pair key：`(left_uid, right_uid, window_epoch)`。
- 指标：recall、precision、duplicates、missing/extra pairs、candidate count、verified count、stale/tombstone hit。
- 性能：throughput、p50/p95/p99、candidate_fetch、similarity、window insert/evict、index insert/delete、emit。
- 并发：lock wait、subtask load imbalance、queue depth、allocator count/bytes。
- 硬件：LLC miss、remote DRAM ratio、false sharing HITM、RSS/index size。

### Stage 2：Hot path 低风险优化

目标：不改变算法语义，先降低明显重复工作。

任务：

- `reserve()`、thread-local buffers、复用候选容器。
- UID lookup 从 per-query map 改为 WindowState 维护的 UID index。
- Local candidate fetch 真正 partition-local。
- batch SIMD verification。
- output 去重和 per-thread emit buffer。
- metrics counters padding。

### Stage 3：窗口与索引结构优化

目标：降低 insert/delete/rebuild 与过期成本。

任务：

- hot mutable buffer + sealed time-bucket segments。
- tombstone bitset + query allow-list。
- partition-level vacuum/rebuild。
- read epoch snapshot + RCU-like delayed release。
- rebuild 触发由 interval 扩展为 deleted ratio、new record threshold、probe recall drift。

### Stage 4：算法级优化

目标：提升候选质量和多核扩展性。

任务：

- hot partition only multicast。
- logical partition + bounded-load routing。
- query micro-batch ordering。
- near-miss candidate cache。
- bucket graph scheduling。
- conservative learned filter。

### Stage 5：异构加速实验

目标：只在 CPU 路线稳定后评估 GPU sidecar。

进入条件：

- window vectors 至少 1e6 级。
- batch query 至少 32/128。
- verification 或 static global search 占总耗时主导。
- 数据或 PQ index 可常驻显存。
- 端到端 p99/throughput 至少 2x 改善且 recall/precision 不退化。

## 7. 推荐实验矩阵

正确性矩阵：

- parallelism：1、2、4、8、16。
- window：small / medium / large。
- stream order：in-order、bounded out-of-order、bursty、shuffled。
- distribution：uniform、Gaussian cluster、Zipf/hot partition、OOD query。
- selectivity：threshold sweep，低/中/高命中。
- update mix：insert-only、insert+evict steady、delete-heavy。

性能矩阵：

- methods：bruteforce、IVF、ClusteredJoin、VSJoin。
- VSJoin variants：current、local-only、global-only、multicast off/on、rebuilder off/on。
- hardware：pin/unpin、NUMA local/interleave、allocator glibc/jemalloc/tcmalloc/mimalloc。
- kernels：scalar、auto-vectorized、AVX2/AVX512、batch size 1/4/16/64/256。

报告格式必须包含：

- effective config。
- commit、机器、编译参数、CPU governor、线程绑定。
- run dir 和 metrics TSV。
- median + tail，不只报 best run。

## 8. 参考文献与系统索引

多核流 Join：

- Low-latency Handshake Join, PVLDB 2014: <https://vldb.org/pvldb/vol7/p709-roy.pdf>
- SplitJoin, USENIX ATC 2016: <https://www.usenix.org/system/files/conference/atc16/atc16_paper-najafi.pdf>
- Parallel Index-based Stream Join / PIM-Tree, SIGMOD 2020: <https://doi.org/10.1145/3318464.3380576>
- Scale-OIJ / OpenMLDB: <https://intellistream.github.io/downloads/papers/Zhang-2023-OIJ-OpenMLDB_CR.pdf>
- Hardware-Conscious Stream Processing survey: <https://ar5iv.org/html/2001.05667>

向量相似度 Join：

- EGO, SIGMOD 2001: <https://doi.org/10.1145/375663.375714>
- Super-EGO: <http://www.ics.uci.edu/~dvk/code/SuperEGO.html>
- Streaming Similarity Self-Join, PVLDB 2016: <https://research.aalto.fi/en/publications/streaming-similarity-self-join>
- SimJoin, SIGMOD/PACMMOD 2025: <https://dl.acm.org/doi/10.1145/3725403>
- VectorJoin work sharing, arXiv 2026: <https://arxiv.org/html/2603.16360v1>
- DiskJoin, arXiv 2025: <https://arxiv.org/html/2508.18494v1>
- Xling/XJoin, arXiv 2024: <https://arxiv.org/pdf/2402.13397>
- VBase, OSDI 2023: <https://xiejiadong.github.io/files/paper/%5Bosdi23%5Dmulti-topk.pdf>

流式向量系统：

- VectraFlow, CIDR 2025: <https://cs.brown.edu/people/malte/pub/papers/2025-cidr-vectraflow.pdf>
- VStream, PVLDB 2025: <https://www.vldb.org/pvldb/vol18/p1593-gao.pdf>
- VStream GitHub: <https://github.com/ZJU-DAILY/VStream>
- Adaptive Distributed Streaming Similarity Joins, DEBS 2023: <https://research.tue.nl/en/publications/adaptive-distributed-streaming-similarity-joins/>

动态 ANN 与向量数据库：

- FreshDiskANN: <https://arxiv.org/pdf/2105.09613>
- SPFresh: <https://dl.acm.org/doi/pdf/10.1145/3600006.3613166>
- Filtered-DiskANN: <https://dl.acm.org/doi/pdf/10.1145/3543507.3583552>
- DiskANN overview: <https://harsha-simhadri.org/diskann-overview.html>
- Milvus architecture: <https://milvus.io/docs/architecture_overview.md>
- Qdrant storage: <https://qdrant.tech/documentation/concepts/storage/>
- Pinecone architecture: <https://docs.pinecone.io/guides/get-started/database-architecture>
- Vespa HNSW: <https://docs.vespa.ai/en/querying/approximate-nn-hnsw.html>
- Elasticsearch kNN: <https://www.elastic.co/docs/solutions/search/vector/knn>
- OpenSearch kNN: <https://docs.opensearch.org/latest/vector-search/vector-search-techniques/approximate-knn/>

硬件与实现：

- Faiss docs: <https://faiss.ai/>
- Faiss implementation notes: <https://github.com/facebookresearch/faiss/wiki/Implementation-notes>
- hnswlib: <https://github.com/nmslib/hnswlib>
- Elastic simdvec: <https://www.elastic.co/search-labs/blog/elasticsearch-vector-search-simdvec-engine>
- Intel VTune Memory Access: <https://www.intel.com/content/www/us/en/docs/vtune-profiler/user-guide/2025-4/memory-access-analysis.html>
- Linux false sharing: <https://docs.kernel.org/kernel-hacking/false-sharing.html>
- perf lock contention: <https://perfwiki.github.io/main/lock-contention/>
- TCMalloc design: <https://google.github.io/tcmalloc/design>
- mimalloc: <http://microsoft.github.io/mimalloc/>
- CAGRA: <https://arxiv.org/html/2308.15136>
- NVIDIA Faiss+cuVS: <https://developer.nvidia.com/blog/enhancing-gpu-accelerated-vector-search-in-faiss-with-nvidia-cuvs/>

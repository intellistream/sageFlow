# 并发数据面 Profile 采证报告

日期：2026-06-25
机器：`hardware_concurrency=14`（Apple arm64），Release 构建，conda 工具链 `.conda-envs/sageflow-perf`。
目的：用数据验证 [CONCURRENCY_DATAPLANE_RESEARCH.md](file:///Users/bytedance/icpp-demo/sageFlow/docs/CONCURRENCY_DATAPLANE_RESEARCH.md) 的瓶颈排序假设，再决定要不要动无锁/存储改造。

## 0. 先说一个采证前提（诚实声明）

`JoinMetrics::lock_wait_ns` 在 `src/` 里**只有 helper 定义、没有任何调用点**（`metrics_record_lock_wait` 未被使用），所以它恒为 0，**不能**用来证明锁竞争。因此本次用两条可信证据替代：

1. 真实流水线的**分阶段计时**（已插桩、确实生效）：`apply_processing_ns / candidate_fetch_ns / index_insert_ns / window_insert_ns / similarity_ns / join_function_ns / emit_ns`。
2. 一个**隔离 microbench** `profile_concurrency_bench`，直接测 StorageManager 全局写锁 与 per-Knn 锁 的并发扩展性（剥离整条 Join 流水线）。

产物：
- microbench 源码 [test/Performance/profile_concurrency_bench.cpp](file:///Users/bytedance/icpp-demo/sageFlow/test/Performance/profile_concurrency_bench.cpp)
- profiling 配置 [config/profile_concurrency.toml](file:///Users/bytedance/icpp-demo/sageFlow/config/profile_concurrency.toml)
- 指标 TSV：`build/metrics/join_datasource_modes_*.tsv`
- 每用例 JSON：`test/result/datasource_modes/*.json`

## 1. Microbench：直接证据（剥离流水线）

### Bench1 — StorageManager 并发 insert（disjoint uid，无逻辑冲突）

线程各插入独立 uid 段，任何变慢都只来自 `map_mutex_` 全局写锁竞争。

| threads | inserts/s | scaling vs 1 |
|---|---|---|
| 1 | 26,284,662 | 1.00x |
| 2 | 13,960,063 | 0.53x |
| 4 | 7,432,595 | 0.28x |
| 8 | 2,932,409 | 0.11x |

结论：**负扩展**。不是“多线程没加速”，而是越多线程越慢（8 线程比单线程慢约 9 倍吞吐）。`StorageManager` 单把全局写锁是硬串行点，且高竞争下 cache-line 抢占使其劣于串行。这是当前数据面**最确定的瓶颈**。

### Bench2 — per-Knn local index（每索引 1 写 + 1 读，模拟 VSJoin local）

P 个独立 `Knn`（共享同一 StorageManager）并行，每个 1 写线程 + 1 读线程。

| pairs | writer_ops/s | aggregate scaling vs 1 |
|---|---|---|
| 1 | 716 | 1.00x |
| 2 | 825 | 1.15x |
| 4 | 1,235 | 1.73x |
| 8 | 2,085 | 2.91x |

结论：独立 local 索引**正扩展**（1→8 聚合 2.9x）。per-`Knn` 锁**不会**串行化不同索引之间的访问，证实了之前判断：local `Knn` 的 `shared_mutex` 不是瓶颈。它没有线性扩展，主因是读路径要走共享 StorageManager（与 Bench1 同源）+ 单条 query 成本，而**不是** Knn 自己的锁。

> 注：Bench2 的绝对 ops/s 低，是因为每次 query 都 `getVectorsByUids` 整索引再逐条算距离（O(n) 暴力），这本身印证 §3 的候选物化/暴力扫描成本，不是锁问题。

## 2. 真实流水线分阶段计时

数据集：random，dim=128，size=2000，window=10s，`generate_direct_use`。各阶段为该用例累计 ns（TSV 按用例时间顺序）。单位换算：1e9 ns = 1s。

### BruteForce（recall 全 = 1.0）

| p | duration_ms | candidate_fetch | similarity | join_func | emit | index_insert | window_insert |
|---|---|---|---|---|---|---|---|
| 1 | 2206 | 0.257s | 0.046s | 0.799s | 0.884s | 0.0008s | 0.0001s |
| 2 | 2411 | 0.287s | 0.050s | 0.875s | 0.964s | 0.0009s | 0.0002s |
| 4 | 1777 | 0.435s | 0.117s | 0.970s | 1.336s | 0.0019s | 0.0017s |
| 8 | 1791 | 0.470s | 0.132s | 1.076s | 4.346s | 0.0025s | 0.0030s |

### VSJoin

| p | duration_ms | recall | candidate_fetch | similarity | join_func | emit | index_insert | window_insert |
|---|---|---|---|---|---|---|---|---|
| 1 | 2777 | 1.000 | 0.581s | 0.048s | 0.930s | 0.963s | 0.0014s | 0.0005s |
| 2 | 3601 | 1.000 | 1.707s | 0.227s | 1.955s | 2.391s | 0.0097s | 0.0034s |
| 4 | 110831 | 0.590 | 3.444s | 0.415s | 2.892s | 215.2s | 0.038s | 0.048s |
| 8 | 82709 | 0.562 | 11.75s | 0.881s | 5.402s | 232.1s | 0.144s | 0.512s |

### 读数

- **index_insert_ns 很小**（≤0.14s，即使 p8 VSJoin）。因为它只覆盖索引层的 uid 插入；`StorageManager::insert` 的全局写锁其实主要发生在 **ingestion / window 写入路径**，不在这个计时窗口里。所以“真实流水线里 index_insert 小”并不与 Bench1 矛盾——Bench1 是把 storage insert 单独拎出来压测，证明它一旦成为热点就会负扩展。
- **candidate_fetch 随 p 显著上升**（VSJoin p1 0.58s → p8 11.75s，约 20x），这是 `query_for_join` + `getVectorsByUids` + `resolveUidsToRecords` 的综合成本，印证 §候选物化是真实热点。
- **emit 在高并行爆炸**（VSJoin p4/p8 达 215–232s），这是 VSJoin 在 p≥4 时 recall 掉到 0.56 同时产生大量重复/无效配对、并把开销转移到结果物化的表现，属于路由/多播退化问题（CentroidPartitioner untrained → 退化 unicast），不是锁。
- BruteForce 的 emit 在 p8 也升到 4.3s，但 recall 仍 1.0、duration 反而最短（1.79s）。说明 emit 累计 ns 是所有线程之和，不等于 wall-clock。

## 3. 结论：瓶颈排序（用数据修正后的）

| 候选瓶颈 | 证据 | 判定 |
|---|---|---|
| **StorageManager 全局写锁** | Bench1：1→8 线程 0.11x 负扩展 | **确认是最硬的并发瓶颈**，一旦 insert 成热点必负扩展 |
| **候选物化（query_for_join + getVectorsByUids + resolveUids）** | 流水线 candidate_fetch 随 p 升 ~20x；Bench2 绝对吞吐低 | **确认是真实热点**，且与 storage 读耦合 |
| **VSJoin 高并行 recall/emit 退化** | p4/p8 recall 0.56、emit 215–232s、duration 飙升 | 路由/多播问题（独立于本次锁议题），需单列 |
| per-Knn local 锁 | Bench2 正扩展 2.9x | **不是瓶颈**，无需无锁化 |
| 算子间队列 | 代码已是无锁 SPSC ring buffer | **不是瓶颈** |

## 4. 据此给出的优先级（仍不实现）

1. **最高优先：StorageManager 写锁分区化 / per-partition storage**。Bench1 证明它负扩展，且与候选物化的读路径共用同一把锁。和 VSJoin local 的 single-writer 模型天然契合。
2. **次高：候选物化去重复哈希 + 暴力扫描批处理**。candidate_fetch 是真实流水线里随并行增长最快的阶段之一。
3. 不做：local Knn 无锁化、算子间队列换无锁（证据显示都不是瓶颈）。
4. 旁路问题（单独立项）：VSJoin p≥4 的 recall 退化（CentroidPartitioner untrained 退化 unicast），与本次并发数据面议题无关，但会严重污染高并行性能测量，建议先修它再做大矩阵性能扫描。

## 5. 复现实验命令

```bash
CM=/Users/bytedance/icpp-demo/.conda-envs/sageflow-perf/bin/cmake
$CM --build build --target profile_concurrency_bench test_join_datasource_modes -j $(sysctl -n hw.ncpu)

# 隔离 microbench（直接锁竞争证据）
./build/bin/profile_concurrency_bench

# 真实流水线分阶段（不污染 CI 配置）
rm -f build/metrics/join_datasource_modes_*.tsv
SAGEFLOW_TEST_CONFIG_PATH=config/profile_concurrency.toml SAGEFLOW_LOG_LEVEL=warn \
  ./build/bin/test_join_datasource_modes --gtest_filter='*DataSourceModePerformance*'
# 分阶段 ns 见 build/metrics/join_datasource_modes_*.tsv（按用例时间顺序）
```

## 6. 配对物化跨线程 free 采证（join-pair-materialization 4b.2）

针对"用 RecordView 做配对物化、跨线程传给下游 LLM 算子是否引入 shared_ptr 跨线程 free 额外开销"的顾虑，新增隔离 microbench [test/Performance/profile_pair_free_bench.cpp](file:///Users/bytedance/icpp-demo/sageFlow/test/Performance/profile_pair_free_bench.cpp)。它通过全局 `operator new/delete` 自计分配次数，**与分配器无关**（默认系统 allocator 即可采证；链接 tcmalloc 只会放大 wall-clock 差值）。

三个场景，2,000,000 个 pair，dim=128，经真实 `RingBufferQueue`(SPSC) + `Response{RecordPair}` 移动入队：
- **A 同线程**：生产线程创建 RecordPair 后立即析构。
- **B 跨线程（最坏）**：生产线程造、SPSC 交给消费线程，消费线程持**最后一个引用**并 free 记录体（控制块+VectorRecord+char[] 跨线程归还）。
- **C 窗口保留（真实）**：左右记录由生产线程的 pool 持有，消费线程只 drop pair 副本（仅引用计数递减），记录重的 free 留在生产线程。

实测（三次运行稳定）：

| scenario | wall_s | pairs/s | new_per_pair | p50_ns | p99_ns |
|---|---|---|---|---|---|
| A 同线程 | ~0.08 | ~24.5M | 1.0 | 41 | 42 |
| B 跨线程 | ~0.33–0.41 | ~5–6M | 1.0 | 125 | 583–708 |
| C 窗口保留 | ~0.30–0.36 | ~5.6–6.6M | 1.0 | 84 | 541–667 |

读数：

- **`new_per_pair = 1.0`（三场景一致）**：每个配对只分配一个 `RecordPairPayload`，**emit 路径零 VectorData 深拷贝**——R1 目标达成，相对当前 concat 路径的 2~3 次深拷贝是净消除。
- **B ≈ C（0.33 vs 0.30–0.36）**：B 把记录体跨线程 free、C 留在生产线程，两者差异在噪声内。**说明跨线程开销主要来自 SPSC 交接本身（入队/出队 + payload 控制块跨线程释放），而不是"记录体 free 方向"**。
- **结论：针对"记录体 free 方向"的 arena allocator 在本场景不会有收益**，因为真实流水线里记录体本就由窗口（生产线程）持有（=场景 C），消费侧从不持最后引用。B/A≈4x 的差距是跨线程交接固有成本，对任何跨线程传输方案都存在，不是 RecordView 特有。
- 因此 design.md §3.6 的 arena 备选**暂不启用**；R1 + `make_shared` 是当前正确选择。若未来下游确实需要持有记录到窗口 evict 之后（脱离场景 C），再重测 B 并考虑 arena 或 R3。

复现：

```bash
$CM --build build --target profile_pair_free_bench -j $(sysctl -n hw.ncpu)
./build/bin/profile_pair_free_bench           # 默认 2,000,000 pair
./build/bin/profile_pair_free_bench 5000000   # 自定 pair 数
```

## 7. 配对物化性能收益（CONCAT vs PAIR_PASSTHROUGH）

本节量化把 Join emit 从 CONCAT（拷贝两条记录 + 拼接新向量）切换到 PAIR_PASSTHROUGH（只读共享引用打包）的实际收益。两类证据：emit 路径隔离 microbench + 真实流水线分阶段计时。

### 7.1 emit 路径隔离 microbench

`test/Performance/profile_emit_materialization_bench.cpp`：同一批命中对分别走真实 `JoinResultEmitter` 的两种模式，扫 dim=128/384/768，各 1,000,000 对。
分配行为（按代码解析）：CONCAT 每对 3 次堆分配（2 条记录深拷贝 dim*4B + 1 条拼接 2*dim*4B），随维度线性增长；PAIR 每对仅 1 个定长 `RecordPairPayload`，与维度无关。

| dim | CONCAT pairs/s | PAIR pairs/s | 吞吐提升 | CONCAT p50 | PAIR p50 | CONCAT p99 | PAIR p99 |
|---|---|---|---|---|---|---|---|
| 128 | 4.63M | 15.1M | **3.27x** | 125ns | 42ns | 250ns | 84ns |
| 384 | 2.13M | 15.1M | **7.09x** | 292ns | 42ns | 1375ns | 84ns |
| 768 | 0.55M | 13.7M | **24.8x** | 625ns | 42ns | 2958ns | 84ns |

读数：**PAIR 的 emit 成本恒定（~42ns/对，~14M 对/s），与向量维度无关；CONCAT 随维度线性变慢。** 维度越高收益越大（128→3.3x，768→24.8x）。p99 尾延迟同样从 250–2958ns 压到恒定 84ns。

### 7.2 真实流水线分阶段（bruteforce, dim=128, size=2000, p=1, win=10s）

同一数据集、同一配置，分别跑默认 CONCAT 与 `SAGEFLOW_JOIN_MATERIALIZATION=pair`。两次 `total_emits` 完全一致（2,791,100），且测试内 recall/precision 断言均通过 → **命中集合不变，只换物化方式**。

| 指标 | CONCAT | PAIR | concat/pair |
|---|---|---|---|
| `join_function_ns`（物化阶段） | 631.4ms | 48.9ms | **12.9x** |
| `apply_processing_ns`（整个 apply） | 1373.1ms | 713.6ms | **1.92x** |
| `candidate_fetch_ns` | 294.0ms | 278.7ms | 1.05x（查询路径不变） |
| `similarity_ns` | 45.7ms | 43.7ms | 1.05x |
| `emit_ns`（collect/入队） | 185.5ms | 213.4ms | 0.87x（噪声内） |
| `total_emits` | 2,791,100 | 2,791,100 | 1.00（命中集合一致） |

读数：
- 物化阶段 `join_function_ns` **降 12.9x**，这是本改动直接命中的热点（profile §2 显示它在 BruteForce 里本是最大单项之一，p1 达 0.63–0.80s）。
- 整个 `apply` 端到端 **快 1.92x**（dim=128 已近腰斩；按 §7.1 维度越高差距越大）。
- `candidate_fetch`/`similarity` 基本不变，证明收益**精确定位在物化**，没有副作用偷换其他阶段。
- 该收益与候选物化阶段（profile §2/§3 标的真实热点）正交叠加，是对那条结论的直接兑现。

### 7.3 复现

```bash
$CM --build build --target profile_emit_materialization_bench test_join_datasource_modes -j $(sysctl -n hw.ncpu)

# emit 路径隔离对比（扫维度）
./build/bin/profile_emit_materialization_bench            # 默认 1,000,000 对/模式/维度

# 端到端两模式对照（同配置）
rm -f build/metrics/join_datasource_modes_*.tsv
SAGEFLOW_TEST_CONFIG_PATH=config/profile_concurrency.toml SAGEFLOW_LOG_LEVEL=warn \
  ./build/bin/test_join_datasource_modes --gtest_filter='*bruteforce*p1*'      # CONCAT
cp $(ls -t build/metrics/join_datasource_modes_*.tsv | head -1) /tmp/concat.tsv
rm -f build/metrics/join_datasource_modes_*.tsv
SAGEFLOW_JOIN_MATERIALIZATION=pair SAGEFLOW_TEST_CONFIG_PATH=config/profile_concurrency.toml SAGEFLOW_LOG_LEVEL=warn \
  ./build/bin/test_join_datasource_modes --gtest_filter='*bruteforce*p1*'      # PAIR
cp $(ls -t build/metrics/join_datasource_modes_*.tsv | head -1) /tmp/pair.tsv
# 对比 /tmp/concat.tsv 与 /tmp/pair.tsv 的 join_function_ns / apply_processing_ns / total_emits
```

## 8. RecordPair 路由与 rollout 验证（2026-06-26）

本轮验证覆盖 `join-pair-materialization` 的 §6/§7：pair 路由、A/B match set、emit microbench、以及 Join 相关回归。

### 8.1 ResponseType 审计结论

用 `rg` 审计 `switch(ResponseType)`、`type_ ==`、`record_`/`records_`/`pair_` 使用点后，结论如下：

| 位置 | 处理 |
|---|---|
| `SinkFunction` | 需要显式 `RecordPair` 分支，已通过 `setPairSinkFunc` 消费 `(left,right,similarity)`。 |
| `ResultPartition` broadcast/multicast copy | 需要保留 pair payload，已改为 `Response data_copy{data}`。 |
| content-based partitioners | 需要定义代表记录，已统一用 `getPartitionRecord(Response)`，`RecordPair` 默认取 left。 |
| `Map/Filter/TopK/Window/Aggregate` | pair-unaware operator 按未知类型返回 `None` 或不转发，不会误当单条 `Record`。 |
| `OutputOperator` | 主要是 source；hash 分支只对 `record_` 取 uid，pair 不作为 source 输出。 |

### 8.2 小规模 recall/precision 与 A/B

Correctness suite：

```bash
python3 scripts/run_integration_test.py \
  --gtest-filter '*bruteforce_baseline*:*vsjoin_baseline*' \
  --parallelism 1 2 4 --data-sizes 500 --timeout 900
```

结果：6/6 passed；`bruteforce` recall=1.0000，`vsjoin` recall=1.0000。

Datasource suite A/B：

```bash
python3 scripts/run_integration_test.py --suite datasource \
  --methods bruteforce vsjoin --parallelism 1 2 4 --data-sizes 500 --expected-dim 128 --timeout 900

SAGEFLOW_JOIN_MATERIALIZATION=pair python3 scripts/run_integration_test.py --suite datasource \
  --methods bruteforce vsjoin --parallelism 1 2 4 --data-sizes 500 --expected-dim 128 --timeout 900
```

结果：CONCAT 与 PAIR 两次均 6/6 passed。每个 `bruteforce/vsjoin × p=1/2/4` case 都是 `matches=225700/225700`、recall=1.000、precision=1.000，说明只改变物化方式，match set 不变。

### 8.3 当前 emit microbench 结果

当前构建下重新运行：

```bash
./build/bin/profile_emit_materialization_bench
```

| dim | CONCAT pairs/s | PAIR pairs/s | 吞吐提升 | CONCAT p50/p99 | PAIR p50/p99 |
|---|---:|---:|---:|---:|---:|
| 128 | 4.74M | 15.18M | 3.21x | 125ns / 209ns | 42ns / 84ns |
| 384 | 1.98M | 14.88M | 7.51x | 292ns / 1500ns | 42ns / 84ns |
| 768 | 0.55M | 14.34M | 26.24x | 667ns / 3250ns | 42ns / 84ns |

分配/拷贝解释保持不变：CONCAT 每对按代码路径产生 3 次记录/向量体相关堆分配，PAIR 每对只分配一个固定 `RecordPairPayload`，`VectorData` 深拷贝为 0（由 `test_join_pair_materialization` 的 shared-ref 与同一 `char[]` 指针断言覆盖）。

### 8.4 回归命令

已重建相关测试目标，避免 `JoinStrategyConfig` 头文件变更后 stale binary 造成 ABI layout 错读：

```bash
/Users/bytedance/icpp-demo/.conda-envs/sageflow-perf/bin/cmake --build build \
  --target test_join_config_validator test_join_strategy_factory test_join_operator_strategy \
           test_join_integration_pipeline test_join_method_registry \
           test_join_baseline_integration test_join_datasource_modes profile_emit_materialization_bench \
  -j $(sysctl -n hw.ncpu)
```

通过的回归：

```bash
./build/bin/test_partitioner
./build/bin/test_join_pair_materialization
./build/bin/test_join_operator_state
./build/bin/test_vsjoin_operator_path
./build/bin/test_join_config_validator
./build/bin/test_join_strategy_factory
./build/bin/test_join_operator_strategy
./build/bin/test_join_method_registry
./build/bin/test_join_integration_pipeline
```

注意：重建前曾运行到旧测试二进制，出现 `JoinStrategyConfig` 字段错位症状（如 `window_state=unknown`、随机 `vsjoin_num_hash_functions`）。重建受影响目标后同一批测试全部通过，判定为 stale binary，不是 runtime 回归。

### 8.5 Similarity Score Limit

`RecordPairPayload::similarity` 字段已接入传输层。当前 pair path 通过 `ComputeEngine::Similarity` / `ComputeEngine::NormalizedSimilarity` 计算 exact score 后写入 payload，避免 executor 维护独立 similarity 算法，也避免下游拿到无效 sentinel。`ComputeEngine::EuclideanDistance(Float32)` 已接到 `SIMDDistance::l2Distance`，因此 fixed/adaptive-alpha 路径会复用已有 SIMD/scalar 统一入口。剩余优化是避免这次 pair emit 阶段复算：`BaseMethod::ExecuteEager` 及各 Join method 当前只返回 `std::vector<RecordView>`，候选验证阶段的 exact score 没有作为结果一起返回；要消除复算，需要把 method interface 升级为 pair/score 结果类型，并同步 BruteForce/IVF/HNSW/HDR/LSH/Clustered/VSJoin 的返回契约。

### 8.6 ComputeEngine 放入 4b.2 跨线程 bench

为验证"统一到 ComputeEngine"后的实际收益，扩展 `profile_pair_free_bench`：在原 `C_window_retain`（窗口持有记录、pair 走 SPSC 跨线程）的同一 handoff 场景中加入三种 workload：

| workload | 含义 |
|---|---|
| `D_window_scalar_direct` | 旧 executor 风格：直接读 `float*`，本地 scalar double 累加 L2。 |
| `E_window_extract_scalar` | 旧 BruteForce/IVF fallback 风格：每次先拷贝成 `std::vector<float>`，再本地 scalar 计算。 |
| `F_window_compute_engine` | 当前统一入口：`ComputeEngine::Similarity`，Float32 路径进入 `SIMDDistance::l2Distance`。 |

命令：

```bash
$CM --build build --target profile_pair_free_bench -j $(sysctl -n hw.ncpu)
./build/bin/profile_pair_free_bench
```

三轮结果（dim=128, pairs=2,000,000）：

| run | direct scalar pairs/s | extract+scalar pairs/s | ComputeEngine pairs/s | CE/direct | CE/extract |
|---|---:|---:|---:|---:|---:|
| 1 | 6.55M | 3.82M | 6.18M | 0.94x | 1.62x |
| 2 | 3.99M | 2.67M | 3.95M | 0.99x | 1.48x |
| 3 | 4.27M | 2.63M | 4.83M | 1.13x | 1.83x |

读数：

- **相对旧 BruteForce/IVF fallback 的 `extract + scalar` 路径，ComputeEngine 提升约 1.5-1.8x**，并把 `new_per_pair` 从 3.0 降回 1.0（去掉两个临时 `std::vector<float>` 分配）。
- **相对 direct scalar，ComputeEngine 没有稳定收益**（约 0.94-1.13x，噪声内）。原因是当前机器是 Apple arm64，而现有 `SIMDDistance` 后端主要覆盖 SSE/AVX/AVX512；arm64 还没有 NEON 后端，所以 `ComputeEngine` 只是统一入口 + float scalar fallback。
- 因此当前收益来自**消除临时向量拷贝与统一计算入口**，不是来自真正的 SIMD。后续要看到计算核层面的收益，需要给 `SIMDDistance` 增加 NEON 实现，并把 JoinMethod 返回契约升级为 `(RecordView, score)` 避免 emit 阶段复算。

# Copilot Instructions for SageFlow

## Quick Reference

**SageFlow**: C++20 向量原生流处理引擎，用于在时间窗口内做状态化向量算子（TopK / Filter / Join 等）。

## Environment Rule

- Do not create new local virtual environments (`venv`/`.venv`); use the existing configured Python environment.

## Build & Test（常用）

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)
```

```bash
ctest --test-dir build --output-on-failure
./build/bin/test_join_baseline_integration
./build/bin/test_join_datasource_modes
```

> 改动 Join/Join pipeline 后，**必须跑** `./build/bin/test_join_datasource_modes`（Join 性能测试：性能/数据源/真实数据集）。

---

## Repo Map（高频目录）

| Directory | Purpose |
|---|---|
| `include/operator/` | 算子定义（Join 是核心） |
| `include/operator/join_operator_methods/` | Join 方法（BruteForce/IVF/HNSW/HDRTree/ClusteredJoin/...） |
| `include/state/` | WindowState（Shared/Partitioned） |
| `include/execution/` | ExecutionGraph/RuntimeContext/并行执行 |
| `include/index/` | 向量索引 |
| `test/test_utils/` | 测试基础设施（数据源、生成器、pipeline helper、report） |
| `config/` | TOML（集成测试/性能测试/实验配置） |
| `docs/` | 设计文档（建议先读 `docs/JOIN_PIPELINE_GUIDE.md`） |

---

## Naming Conventions（clang-tidy 约束）

| Element | Convention | Example |
|---|---|---|
| Classes | `CamelCase` | `RuntimeContext`, `WindowState` |
| Methods | `camelBack` | `getSubtaskIndex()`, `processRecord()` |
| Members | `lower_case_` (trailing underscore) | `subtask_index_`, `parallelism_` |
| Namespaces | `lower_case` | `sageFlow` |
| Constants | `UPPER_CASE` | `MAX_BUFFER_SIZE` |
| Variables | `lower_case` | `query_record`, `index_id` |

---

## Core Architecture Patterns（架构速记）

### 1) Three-Phase Pipeline
1. **Ingestion** → 数据源（`DataStreamSource`/测试 Source）
2. **State Materialization** → 窗口内状态化算子（Join/TopK）
3. **Snapshot Exposure** → Sink 输出/汇聚

### 2) Connection Strategy（SPSC Queue Matrix）
```cpp
// upstream_parallelism × downstream_parallelism queues
queue_index(upstream_i, downstream_j) = upstream_i × downstream_parallelism + downstream_j
```

### 3) State / Partitioner 匹配规则（重要）
| Partition Strategy | Compatible Window State | Notes |
|---|---|---|
| RoundRobin | SharedWindowState | 随机分发要求共享状态 |
| KeyPartitioner | Partitioned/Shared | Key 路由 |
| VectorHash | PartitionedWindowState | 相似向量同分区 |

**⚠️ 常见坑**：RoundRobin + PartitionedState 会导致召回下降（数据分散且状态不共享）。

---

## Critical Developer Workflows（开发工作流）

### ClusteredJoin 关键约束与常见坑
- **`num_partitions` 必须等于运行时 `parallelism`**（避免 silent recall loss；当前实现倾向 fail-fast）。  
- **多播会放大输出规模**：生产不要在 Join 输出端拼接超大 payload；做性能分析要区分 Join 完成时间与 Sink catch-up。

### Adding a New Join Method（新增 Join 方法）
1. Add enum: `JoinAlgorithm` in `include/operator/join_strategy_config.h`
2. Add enum: `JoinMethodType` in `include/operator/join_operator_methods/base_method.h`
3. Implement: 新建 `my_method.h/.cpp`，继承 `BaseMethod`
4. Register: 更新 `src/operator/join_strategy_factory.cpp`
5. Validate: 更新 `src/operator/utils/join_config_validator.cpp`
6. Test: 在 `config/integration_test_cases.toml` 添加用例

完整流程见：`docs/ADDING_NEW_JOIN_METHOD.md`

### Writing Tests（写测试）
优先复用 `test/test_utils/` 的现成工具：
```cpp
// Data generation
TestDataGenerator generator(config);
auto [records, expected_matches] = generator.generateData();

// Config loading
auto config = JoinConfigLoader::loadByName("config/join_strategies.toml", "ivf_standard");

// Pipeline building
JoinIntegrationPipelineHelper helper(test_case);
helper.build();
auto results = helper.execute();
double recall = helper.computeRecall(results, ground_truth);
```

**Output**:
- Results: `test/result/datasource_modes/`
- Reports: `test/result/datasource_modes_report.tsv`
- Metrics: `build/metrics/`

---

## Core Invariants（别踩坑）

### Index 访问
**禁止**直接访问 Index 对象，所有索引操作必须走 `ConcurrencyManager`：

```cpp
auto idx_id = concurrency_manager_->create_index("name", IndexType::HNSW, 128);
concurrency_manager_->insert(idx_id, std::move(record));
auto results = concurrency_manager_->query(idx_id, query, k);
```

### State / Partitioner 匹配
- RoundRobin + PartitionedWindowState 会导致召回下降（数据被分散但状态不共享）

### ClusteredJoin 约束
- `num_partitions` 必须等于运行时 `parallelism`（避免 silent recall loss；当前实现倾向 fail-fast）
- 多播会放大输出规模；生产不要在 Join 输出端拼接超大 payload

---

## Testing & Experiments

### Integration Tests（TOML 驱动）
- Binary: `build/bin/test_join_baseline_integration`
- Runner: `scripts/run_integration_test.py`
- Env:
  - `SAGEFLOW_TEST_CONFIG_PATH`：指定 TOML（例如 `config/clustered_experiment.toml`）
  - `SAGEFLOW_TEST_OUTPUT_DIR`：覆盖输出目录

```bash
OUT="test/result/integration_$(date +%Y%m%d_%H%M%S)"
python3 scripts/run_integration_test.py --methods all --config config/integration_test_cases.toml --output-dir "$OUT"
python3 scripts/run_integration_test.py --methods all --config config/integration_test_cases.toml --output-dir "$OUT" --gtest-filter='*exp_a*'
```

### ClusteredJoin Experiment A/B
- Config: `config/clustered_experiment.toml`
- Visualize: `scripts/visualize_clustered_experiment.py`（优先读 `*_results.csv`，并生成 `EXPERIMENT_SUMMARY.md`）

```bash
OUT="test/result/clustered_experiment_$(date +%Y%m%d_%H%M%S)"
python3 scripts/run_integration_test.py --methods all --config config/clustered_experiment.toml --output-dir "$OUT" --gtest-filter='*exp_a*'
python3 scripts/run_integration_test.py --methods all --config config/clustered_experiment.toml --output-dir "$OUT" --gtest-filter='*exp_b*'
python3 scripts/visualize_clustered_experiment.py -i "$OUT" -o "$OUT/charts"
```

### Data Source（测试侧）
`test/test_utils/data_source/` 提供 dataset/random 等数据源；`data/siftsmall/` 是常用真实数据集示例。

### Performance Tests（长期维护）
- Binary: `build/bin/test_join_datasource_modes`
- Config: `config/perf_join_datasource_modes.toml`
- Output:
  - `test/result/datasource_modes/`
  - `test/result/datasource_modes_report.tsv`
  - `build/metrics/`

---

## Key Files to Understand（定位入口）
- `include/operator/join_operator.h`：Join 主逻辑
- `src/operator/join_operator.cpp`：Join 初始化/运行时约束
- `src/operator/join_strategy_factory.cpp`：方法实例化
- `test/IntegrationTest/join_baseline_integration_test.cpp`：TOML 驱动集成测试入口
- `test/test_utils/join_integration_pipeline_helper.*`：Pipeline 组装/等待策略/输出统计

---

## Logging
```cpp
SAGEFLOW_LOG_DEBUG("TAG", "Message with {} args", value);
SAGEFLOW_LOG_INFO("TAG", "Informational message");
SAGEFLOW_LOG_WARN("TAG", "Warning: {}", issue);
SAGEFLOW_LOG_ERROR("TAG", "Error occurred: {}", error_msg);
```

## Polyrepo coordination (mandatory)

- This repository is an independent SAGE sub-repository and is developed/released independently.
- Do not assume sibling source directories exist locally in `intellistream/SAGE`.
- For cross-repo rollout, publish this repo/package first, then bump the version pin in `SAGE/packages/sage/pyproject.toml` when applicable.
- Do not add local editable installs of other SAGE sub-packages in setup scripts or docs.


# SageFlow Agent Guide

> 目的：为自动化 agent（含 Copilot / CI 辅助 bot / 内部工具）提供**项目最小但完整的工作上下文**。  
> 更详细的规则请参考：`.cursor/rules/sageflow.mdc` 与 `.github/copilot-instructions.md`（两者内容应保持基本一致）。

## Quick Reference

**SageFlow**：C++20 向量原生流处理引擎，在时间窗口内做状态化向量算子（TopK / Filter / Join 等），Join 是核心组件。

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)
```

## Test（常用入口）

```bash
ctest --test-dir build --output-on-failure                   # 全量（可能较久）
./build/bin/test_join_baseline_integration                   # Join 集成测试（TOML 驱动）
./build/bin/test_join_datasource_modes                        # Join 性能测试（性能/数据源/真实数据集）
```

## Repo Map（高频目录）

| Directory | Purpose |
|---|---|
| `include/operator/` | 算子定义（Join 是核心） |
| `include/operator/join_operator_methods/` | Join 方法实现 |
| `include/state/` | WindowState（Shared/Partitioned） |
| `include/execution/` | ExecutionGraph/RuntimeContext/并行执行 |
| `include/index/` | 向量索引（HNSW/IVF/BruteForce/...） |
| `test/test_utils/` | 测试基础设施（数据源、生成器、pipeline helper、report） |
| `config/` | TOML（集成测试/性能测试/实验配置） |
| `docs/` | 设计与实验文档（建议先读 `docs/JOIN_PIPELINE_GUIDE.md`） |

## Core Invariants（别踩坑）

### Index 访问
- **禁止**直接访问 Index 对象；所有索引操作必须走 `ConcurrencyManager`。

### State / Partitioner 匹配
- **⚠️** RoundRobin + PartitionedWindowState 会导致召回下降（数据被分散但状态不共享）。

### ClusteredJoin 关键约束
- **`num_partitions` 必须等于运行时 `parallelism`**（避免 silent recall loss；当前实现倾向 fail-fast）。
- **多播会放大输出规模**：生产不要在 Join 输出端拼接超大 payload；性能分析要区分 Join 完成时间与 Sink catch-up。

## ClusteredJoin Experiments（A/B）

- 配置：`config/clustered_experiment.toml`
- 运行器：`scripts/run_integration_test.py`
- 可视化：`scripts/visualize_clustered_experiment.py`（优先读 `*_results.csv`，并生成 `EXPERIMENT_SUMMARY.md`）

```bash
OUT="test/result/clustered_experiment_$(date +%Y%m%d_%H%M%S)"
python3 scripts/run_integration_test.py --methods all --config config/clustered_experiment.toml --output-dir "$OUT" --gtest-filter='*exp_a*'
python3 scripts/run_integration_test.py --methods all --config config/clustered_experiment.toml --output-dir "$OUT" --gtest-filter='*exp_b*'
python3 scripts/visualize_clustered_experiment.py -i "$OUT" -o "$OUT/charts"
```

## 输出/去重（重要提醒）

- **测试侧**：Match 去重通常在 Sink 做（`sink_processed` 与 `sink_dedup` 需要同时看），不要把“测试侧 payload 协议/去重方案”直接照搬到生产。
- **生产侧**：更推荐把 match pair 作为结构化字段在算子链路中传递（而不是依赖 UID 偏移或把 pair 编码进单个 UID）。



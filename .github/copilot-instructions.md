# Copilot Instructions for SageFlow

## Quick Reference

**SageFlow**: C++20 vector-native stream processing engine for real-time LLM context generation. Declarative API for stateful vector operations (TopK, Filter, Join) within temporal windows.

**Build & Test**:
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)
ctest --test-dir build --output-on-failure          # All tests
./build/bin/test_join_datasource_modes              # CRITICAL: Run after Join changes
```

---

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `include/operator/` | Stream operators - **Join is the core component** |
| `include/operator/join_operator_methods/` | Join algorithm implementations |
| `include/state/` | WindowState abstractions (Shared vs Partitioned) |
| `include/execution/` | ExecutionGraph, ExecutionVertex, RuntimeContext |
| `include/index/` | Vector indices (HNSW, IVF, BruteForce) |
| `test/test_utils/` | **USE THESE** - Test helpers, data generators |
| `config/` | TOML configs for strategies and test cases |
| `docs/` | Architecture docs - read [JOIN_PIPELINE_GUIDE.md](../docs/JOIN_PIPELINE_GUIDE.md) first |

---

## Naming Conventions (enforced by clang-tidy)

| Element | Convention | Example |
|---------|------------|---------|
| Classes | `CamelCase` | `RuntimeContext`, `WindowState` |
| Methods | `camelBack` | `getSubtaskIndex()`, `processRecord()` |
| Members | `lower_case_` (trailing underscore) | `subtask_index_`, `parallelism_` |
| Namespaces | `lower_case` | `sageFlow` |
| Constants | `UPPER_CASE` | `MAX_BUFFER_SIZE` |
| Variables | `lower_case` | `query_record`, `index_id` |

---

## Core Architecture Patterns

### 1. Three-Phase Pipeline
1. **Ingestion** → Data sources (`FileStreamSource`, `TestVectorStreamSource`)
2. **State Materialization** → Stateful ops in windows (Join, TopK)
3. **Snapshot Exposure** → Results via Sink operators

### 2. Connection Strategy (SPSC Queue Matrix)
```cpp
// upstream_parallelism × downstream_parallelism queues
queue_index(upstream_i, downstream_j) = upstream_i × downstream_parallelism + downstream_j
```

### 3. State Management Rules
| Partition Strategy | Compatible Window State | Notes |
|-------------------|------------------------|-------|
| RoundRobin | SharedWindowState | Random distribution needs shared state |
| KeyPartitioner | Partitioned/Shared | Key-based routing |
| VectorHash | PartitionedWindowState | Similar vectors to same partition |

**⚠️ Incompatible combos cause recall drops**: RoundRobin + PartitionedState = lost matches

### 4. Index Access Pattern
```cpp
// ALWAYS use ConcurrencyManager - never access Index directly
auto idx_id = concurrency_manager_->create_index("name", IndexType::HNSW, 128);
concurrency_manager_->insert(idx_id, std::move(record));
auto results = concurrency_manager_->query(idx_id, query, k);
```

---

## Critical Developer Workflows

### Adding a New Join Method
1. Add enum: `JoinAlgorithm` in [join_strategy_config.h](../include/operator/join_strategy_config.h)
2. Add enum: `JoinMethodType` in [base_method.h](../include/operator/join_operator_methods/base_method.h)
3. Implement: Create `my_method.h/cpp` inheriting `BaseMethod`
4. Register: Update [JoinStrategyFactory](../src/operator/join_strategy_factory.cpp)
5. Validate: Add rules in [join_config_validator.cpp](../src/operator/join_config_validator.cpp)
6. Test: Add TOML config in [integration_test_cases.toml](../config/integration_test_cases.toml)

**Full guide**: [docs/ADDING_NEW_JOIN_METHOD.md](../docs/ADDING_NEW_JOIN_METHOD.md)

### Writing Tests
Use existing utilities from `test/test_utils/`:
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

---

## Testing Infrastructure

### Integration Tests (Long-term Maintained)
**Script**: `scripts/run_integration_test.py`
**Test Binary**: `build/bin/test_join_baseline_integration`

```bash
# Run specific methods
python scripts/run_integration_test.py --methods bruteforce ivf hdr_tree

# Run all methods with visualization
python scripts/run_integration_test.py --methods all --visualize

# Override parallelism and data sizes
python scripts/run_integration_test.py --methods bruteforce --parallelism 1 2 4 --data-sizes 500 1000
```

### Performance Tests (Long-term Maintained)
**Test Binary**: `build/bin/test_join_datasource_modes`
**Config**: `config/perf_join_datasource_modes.toml`

```bash
# Run all configured tests (bruteforce, ivf, hdr_tree)
./build/bin/test_join_datasource_modes

# Filter specific method
./build/bin/test_join_datasource_modes --gtest_filter="*bruteforce*"

# Filter specific parallelism
./build/bin/test_join_datasource_modes --gtest_filter="*_p4_*"
```

**Output**:
- Results: `test/result/datasource_modes/`
- Reports: `test/result/datasource_modes_report.tsv`
- Metrics: `build/metrics/`

---

## AI Agent Guidelines

### MUST DO
- Run `./build/bin/test_join_datasource_modes` after modifying Join pipeline code
- Use `RuntimeContext` for thread identity in operators
- Use `ConcurrencyManager` for all index operations
- Use test utilities from `test/test_utils/` (don't reinvent)
- Check `WindowState::isShared()` before assuming state isolation

### MUST NOT
- Access `Index` objects directly (use `ConcurrencyManager`)
- Combine RoundRobin partitioner with PartitionedWindowState
- Skip validation tests after Join-related changes
- Create custom test data generators (use `TestDataGenerator`)

### Key Files to Understand
- [JoinOperator](../include/operator/join_operator.h) - Central Join logic
- [ExecutionGraph](../include/execution/execution_graph.h) - DAG management
- [WindowState](../include/state/window_state.h) - State abstraction
- [JoinStrategyFactory](../src/operator/join_strategy_factory.cpp) - Method instantiation

---

## Logging

```cpp
SAGEFLOW_LOG_DEBUG("TAG", "Message with {} args", value);
SAGEFLOW_LOG_INFO("TAG", "Informational message");
SAGEFLOW_LOG_WARN("TAG", "Warning: {}", issue);
SAGEFLOW_LOG_ERROR("TAG", "Error occurred: {}", error_msg);
```


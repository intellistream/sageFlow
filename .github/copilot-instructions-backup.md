# Copilot Instructions for SageFlow

## Overview

**SageFlow** is a C++20 vector-native stream processing engine for real-time LLM context generation. It provides a declarative API for stateful vector operations (TopK, Filter, Join) within temporal windows.

**Tech Stack**: C++20, CMake 3.20+, Google Test, spdlog, tomlplusplus, pybind11

## Project Structure (Key Directories)

```
include/              # Public API headers (mirrors src/ structure)
├── operator/         # Stream operators - JOIN IS THE CORE COMPONENT
│   └── join_operator_methods/  # Join algorithm implementations
├── state/            # WindowState (SharedWindowState, PartitionedWindowState)
├── execution/        # ExecutionGraph, ExecutionVertex, RuntimeContext
├── index/            # Vector indices (HNSW, IVF, BruteForce)
└── concurrency/      # Thread-safe index access via ConcurrencyManager

src/                  # Implementation files
test/
├── UnitTest/         # Unit tests (test_*.cpp)
├── IntegrationTest/  # Pipeline integration tests
├── Performance/      # Benchmarks (test_join_datasource_modes.cpp is CRITICAL)
└── test_utils/       # Test helpers - USE THESE, don't reinvent

config/               # TOML configs (join_strategies.toml, integration_test_cases.toml)
docs/                 # Architecture docs (JOIN_PIPELINE_GUIDE.md, SYSTEM_ARCHITECTURE.md)
```

## Coding Conventions

### Naming Conventions (enforced by clang-tidy)

- **Classes**: `CamelCase` (e.g., `RuntimeContext`, `WindowState`)
- **Class Methods**: `camelBack` (e.g., `getSubtaskIndex()`, `processRecord()`)
- **Member Variables**: `lower_case_` with trailing underscore (e.g., `subtask_index_`, `parallelism_`)
- **Namespaces**: `lower_case` (e.g., `sageFlow`)
- **Global Functions**: `CamelCase`
- **Global Constants**: `UPPER_CASE`
- **Variables/Parameters**: `lower_case`
- **Enums/Structs/Unions**: `CamelCase`

### Code Style

- Based on Google C++ Style Guide
- Column limit: 120 characters
- Use `#pragma once` for header guards
- Separate definition blocks with blank lines
- Use C++20 features (concepts, ranges, etc.) where appropriate

### Documentation

- Use Doxygen-style comments for public APIs
- Include `@brief`, `@param`, `@return` tags
- Write comments in English for code, Chinese is acceptable for architecture docs

---

## Architecture Overview

### Three-Phase Pipeline

1. **Ingestion**: Data input from sources (`DataStreamSource`, `FileStreamSource`, etc.)
2. **State Materialization**: Stateful computation within windows (Join, TopK, Aggregate)
3. **Snapshot Exposure**: Expose results for downstream consumption via Sink operators

### Key Abstractions

#### 1. Execution Graph & Vertices

The `ExecutionGraph` is the core execution engine that manages the DAG of operators.

```cpp
// ExecutionGraph: Manages operator DAG and parallel execution
class ExecutionGraph {
    void addOperator(std::shared_ptr<Operator> op);
    void connectOperators(upstream, downstream, slot);
    void buildGraph();  // Creates ExecutionVertex instances
    void start();       // Launches all worker threads
    void stop();        // Graceful shutdown
};

// ExecutionVertex: Represents a parallel instance of an operator
class ExecutionVertex {
    std::shared_ptr<Operator> operator_;
    std::unique_ptr<InputGate> input_gate_;      // Reads from upstream queues
    std::unique_ptr<ResultPartition> result_partition_; // Writes to downstream
    size_t subtask_index_;  // Identifies this parallel instance
};
```

#### 2. Data Flow Components

```cpp
// InputGate: Reads data from multiple upstream queues (round-robin polling)
class InputGate {
    void setup(const std::vector<QueuePtr>& queues);
    std::optional<TaggedResponse> read();
};

// ResultPartition: Routes output data to downstream queues via Partitioner
class ResultPartition {
    void setup(std::unique_ptr<IPartitioner> p, std::vector<QueuePtr> channels, int slot);
    void emit(Response&& data, int slot) const;
};

// Partitioners control data distribution:
// - RoundRobinPartitioner: Load balancing
// - KeyPartitioner: Timestamp-based routing for ordering
// - VectorHashPartitioner: Content-based sharding
// - BroadcastPartitioner: Send to all downstream instances
```

#### 3. Connection Strategy (Unified SPSC Matrix)

SageFlow uses a unified a×b SPSC queue matrix for all operator connections:

| Property | Value |
|----------|-------|
| **Queue Count** | upstream_parallelism × downstream_parallelism |
| **Queue Type** | RingBufferQueue (SPSC, Lock-Free) |
| **Routing** | Partitioner selects target queue |

```cpp
// Queue index formula
queue_index(upstream_i, downstream_j) = upstream_i × downstream_parallelism + downstream_j

// Upstream i writes to queues [i*D, i*D+1, ..., i*D+D-1]
// Downstream j polls from queues [0*D+j, 1*D+j, 2*D+j, ...]

// Use SharedWindowState for shared index (RoundRobin partitioner)
// Use PartitionedWindowState for partition-based join (Key/Hash partitioner)
```

#### 4. RuntimeContext

Provides execution environment info to operators for thread identity and state routing:

```cpp
class RuntimeContext {
    size_t getSubtaskIndex() const;  // Which parallel instance (0-based)
    size_t getParallelism() const;   // Total parallel instances
    std::string getTaskName() const; // "Task[2/8]" format for logging
};
```

#### 5. Window State Management

Abstract interface supporting both partitioned and shared state models:

```cpp
class WindowState {
    virtual void addRecord(std::unique_ptr<VectorRecord> record, size_t subtask_index) = 0;
    virtual const std::deque<std::unique_ptr<VectorRecord>>& getRecords(size_t subtask_index) const = 0;
    virtual void evictExpired(int64_t current_ts, int64_t window_size, size_t subtask_index) = 0;
    virtual bool isShared() const = 0;
};

// PartitionedWindowState: Each subtask has independent state (no cross-task sync)
// SharedWindowState: All subtasks share same state (requires synchronization)
```

#### 6. Concurrency Manager & Index Management

Thread-safe management of vector indices:

```cpp
class ConcurrencyManager {
    // 创建标准索引类型（自动配置 storage_manager_）
    int create_index(name, IndexType, dimension, params);
    
    // 注册外部创建的索引（如 PartitionedIndex）
    // 会自动配置 storage_manager_，确保索引可以访问全局存储
    int register_index(name, std::shared_ptr<Index> index);
    
    bool drop_index(name);
    bool insert(index_id, std::unique_ptr<VectorRecord> record);
    bool erase(index_id, uint64_t uid);
    std::vector<std::shared_ptr<const VectorRecord>> query(index_id, record, k);
    std::vector<std::shared_ptr<const VectorRecord>> query_for_join(index_id, record, threshold);
};

// Wraps ConcurrencyController for each index to ensure thread-safe operations
```

**索引创建规范**：
1. 所有索引必须通过 `ConcurrencyManager` 创建或注册
2. 标准索引类型（HNSW, IVF, BruteForce）使用 `create_index()` 方法
3. 自定义索引类型（如 PartitionedIndex）使用 `register_index()` 方法
4. 两种方法都会自动配置 `storage_manager_`，确保索引可以访问全局存储
5. 索引的 `insert(uid)` 操作会从 `storage_manager_` 获取完整向量数据
6. 不要直接设置 `index->storage_manager_`，应通过 `ConcurrencyManager` 管理

#### 7. Vector Index Implementations

```cpp
enum class IndexType { None, HNSW, BruteForce, IVF, Vectraflow, PartitionedIndex };

// HNSW: Hierarchical Navigable Small World graph for approximate k-NN
class HNSW : public Index {
    int m_;               // Max neighbors per layer
    int ef_construction_; // Build-time candidate size
    int ef_search_;       // Query-time candidate size
};

// IVF: Inverted File Index with k-means clustering
class Ivf : public Index {
    int nlist_;           // Number of clusters
    int nprobes_;         // Clusters to search at query time
    double rebuild_threshold_;  // When to rebuild centroids
};

// PartitionedIndex: Vector-space partitioned index for VSJoin
class PartitionedIndex : public Index {
    size_t num_partitions_;           // Number of partitions
    std::shared_ptr<VectorSpacePartitioner> partitioner_; // Partition strategy
    std::vector<std::shared_ptr<Ivf>> partition_indexes_; // Per-partition IVF
};
```

#### 8. Stream API & Planner

Declarative API for building processing pipelines:

```cpp
class Stream {
    auto filter(std::unique_ptr<FilterFunction>& func, size_t parallelism) -> std::shared_ptr<Stream>;
    auto map(std::unique_ptr<MapFunction>& func, size_t parallelism) -> std::shared_ptr<Stream>;
    auto join(std::shared_ptr<Stream>& other, std::unique_ptr<JoinFunction>& func,
              const std::string& method, double threshold, size_t parallelism) -> std::shared_ptr<Stream>;
    auto topk(int32_t index_id, int k, size_t parallelism) -> std::shared_ptr<Stream>;
    auto writeSink(std::unique_ptr<SinkFunction>& func, size_t parallelism) -> std::shared_ptr<Stream>;
};

// Planner converts Stream DAG to ExecutionGraph
class Planner {
    void planToExecutionGraph(stream, execution_graph, default_parallelism);
};
```

#### 9. Data Sources

```cpp
class DataStreamSource : public Stream {
    virtual std::unique_ptr<VectorRecord> Next() = 0;
    virtual void Init() {}
};

// Implementations:
// - FileStreamSource: Read vectors from files
// - SiftStreamSource: SIFT dataset format
// - SimpleStreamSource: In-memory vector source
```

#### 10. Join Methods

Pluggable join algorithms in `operator/join_operator_methods/`:

```cpp
enum class JoinMethodType { BRUTEFORCE_EAGER, BRUTEFORCE_LAZY, IVF_EAGER, IVF_LAZY };

class BaseMethod {
    virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) = 0;
    virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, int slot) = 0;
};
```

---

## Build Commands

```bash
# Configure
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON

# Build
cmake --build build -j $(nproc)

# Run all tests
ctest --test-dir build --output-on-failure

# Run specific test binary
./build/bin/test_window_state

# Run tests by label
ctest --test-dir build -L UNIT --output-on-failure
ctest --test-dir build -L INTEGRATION --output-on-failure
```

## Testing Guidelines

- Place unit tests in `test/UnitTest/`
- Use descriptive test names: `TEST(ClassName, MethodName_Scenario_Expected)`
- Tests are discovered via CMake's `gtest_discover_tests()`
- Use labels: `UNIT`, `INTEGRATION`, `PERFORMANCE`

## Testing Utilities

The `test/test_utils/` directory provides a comprehensive suite of tools for generating test data, managing data sources, and verifying results. **Always use these utilities instead of creating custom data generation logic.**

### 1. Data Generation (`TestDataGenerator`)
Located in `test/test_utils/test_data_generator.h`.
- Generates synthetic vector datasets with controlled similarity properties (positive pairs, negative pairs).
- Can save generated data to files (fvecs, json) for reproducibility.
- **Usage**:
  ```cpp
  TestDataGenerator::Config config;
  config.vector_dim = 128;
  config.positive_pairs = 100;
  TestDataGenerator generator(config);
  auto [records, expected_matches] = generator.generateData();
  ```

### 2. Join Test Helpers (`JoinTestHelper`)
Located in `test/test_utils/join_test_helper.h`.
- Simplifies the creation of left/right streams for join testing.
- **Key Methods**:
  - `generateJoinStreamsFromGenerator`: Creates streams from `TestDataGenerator`.
  - `generateJoinStreamsFromSource`: Creates streams from a `DataSource`.
  - `generateJoinStreamsFromSeparateSources`: Uses different sources for left/right.

### 3. Data Sources (`test/test_utils/data_source/`)
- **Factory**: `DataSourceFactory::createFromConfig(config)`
- **Types**:
  - `RandomDataSource`: Generates random vectors on the fly.
  - `DatasetDataSource`: Reads from standard vector files (e.g., .fvecs).
  - `JsonDataSource`: Reads from JSON files.

### 4. Data Writers (`test/test_utils/data_writer/`)
- **Types**: `FvecsWriter`, `JsonWriter`.
- Used by `TestDataGenerator` to persist generated datasets.

### 5. Verification (`BaselineJoinChecker`)
- Computes ground truth for join operations.
- **Usage**:
  ```cpp
  auto expected = BaselineJoinChecker::computeExpectedMatches(records, threshold, window_size);
  ```

### 6. Config Loaders (Group E)
- **`JoinConfigLoader`** (`test/test_utils/join_config_loader.h`): Load `JoinStrategyConfig` from TOML
- **`IntegrationTestConfigLoader`** (`test/test_utils/integration_test_config.h`): Load complete test cases
- **Usage**:
  ```cpp
  auto config = JoinConfigLoader::loadByName("config/join_strategies.toml", "ivf_standard");
  auto tests = IntegrationTestConfigLoader::loadEnabledTests("config/integration_test_cases.toml");
  ```

### 7. Pipeline Helper (`JoinIntegrationPipelineHelper`)
- Located in `test/test_utils/join_integration_pipeline_helper.h`
- Builds and executes Join test pipelines from `IntegrationTestCase`
- **Key Methods**:
  - `build()`: Construct pipeline from config
  - `execute()`: Run the pipeline
  - `computeRecall/computePrecision`: Validate results

### 8. Metrics & Reports
- **`JoinMetricsCollector`** (`include/metrics/join_metrics_collector.h`): Collect timing, accuracy metrics
- **`TestReportGenerator`** (`test/test_utils/test_report_generator.h`): Generate JSON/Markdown reports
- **Python Script**: `scripts/generate_test_summary.py` for report aggregation

### Example: Creating a Join Test
```cpp
// 1. Configure and generate data
TestDataGenerator::Config gen_config;
TestDataGenerator generator(gen_config);

// 2. Create input streams
auto [left_stream, right_stream] = JoinTestHelper::generateJoinStreamsFromGenerator(generator);

// 3. Feed into pipeline (using TestVectorStreamSource)
auto source_left = std::make_shared<TestVectorStreamSource>("left", std::move(left_stream));
auto source_right = std::make_shared<TestVectorStreamSource>("right", std::move(right_stream));

// 4. Verify results
auto expected_matches = BaselineJoinChecker::computeExpectedMatches(...);
```

### Example: Config-Driven Integration Test
```cpp
// Load test case from TOML
auto test_case = IntegrationTestConfigLoader::loadByName(
    "config/integration_test_cases.toml", "ivf_standard");

// Build pipeline using helper
JoinIntegrationPipelineHelper helper(test_case.value());
helper.setLeftSource(left_source);
helper.setRightSource(right_source);
helper.build();

// Execute and validate
auto result = helper.execute();
double recall = JoinIntegrationPipelineHelper::computeRecall(results, ground_truth);
EXPECT_GE(recall, test_case->expected_min_recall);
```

---

## Common Patterns

### Creating a New Operator

```cpp
class MyOperator : public Operator {
public:
    explicit MyOperator(std::unique_ptr<Function>& func)
        : Operator(OperatorType::MY_TYPE, 1) {
        function_ = std::move(func);
    }

    void open(const RuntimeContext& context) override {
        subtask_index_ = context.getSubtaskIndex();
        parallelism_ = context.getParallelism();
    }
    
    void apply(Response&& record, int slot, Collector& collector,
               const RuntimeContext& context) override {
        // Process record and emit results
        collector.collect(std::move(result));
    }
    
    void close() override {
        // Cleanup resources
    }
    
private:
    size_t subtask_index_;
    size_t parallelism_;
};
```

### State Access Pattern

```cpp
// Using WindowState abstraction
void processWithState(const RuntimeContext& context) {
    size_t idx = context.getSubtaskIndex();
    
    // Add record to window
    state_->addRecord(std::move(record), idx);
    
    // Query records in window
    const auto& records = state_->getRecords(idx);
    
    // Evict expired records
    state_->evictExpired(current_timestamp, window_size, idx);
}
```

### Creating Execution Graph

```cpp
StreamEnvironment env;
env.setParallelism(4);

auto source = std::make_shared<FileStreamSource>("data.bin");
auto sink_func = std::make_unique<MySinkFunction>();

auto pipeline = source
    ->filter(std::make_unique<MyFilter>(), 2)
    ->map(std::make_unique<MyMapper>(), 4)
    ->writeSink(std::move(sink_func), 1);

env.addStream(pipeline);
env.execute();
env.awaitTermination();
```

### Thread-Safe Index Operations

```cpp
// Via ConcurrencyManager
int idx_id = concurrency_manager_->create_index("my_index", IndexType::HNSW, 128);
concurrency_manager_->insert(idx_id, std::move(record));
auto results = concurrency_manager_->query(idx_id, query_record, 10);
```

---

## Logging

Use the `SAGEFLOW_LOG_*` macros:
```cpp
SAGEFLOW_LOG_DEBUG("TAG", "Message with {} args", value);
SAGEFLOW_LOG_INFO("TAG", "Informational message");
SAGEFLOW_LOG_WARN("TAG", "Warning: {}", issue);
SAGEFLOW_LOG_ERROR("TAG", "Error occurred: {}", error_msg);
```

## Configuration

Configuration files use TOML format. See `config/` directory for examples:
- `default_config.toml`: General settings
- `join_config.toml`: Join operator parameters
- `sift_config.toml`: SIFT dataset paths

## Performance Considerations

- Use `std::move` for large objects (especially `VectorRecord`)
- Prefer `emplace_back` over `push_back`
- Use `reserve()` when vector size is known
- Consider cache locality in hot paths
- Use lock-free structures where possible (see `concurrency/`)
- For Join operations, choose appropriate method based on data characteristics:
  - `bruteforce_lazy`: Small windows, exact results
  - `ivf_eager`: Large windows, approximate results acceptable

## Dependencies

Third-party libraries are fetched via CMake's FetchContent:
- **googletest**: Testing framework
- **spdlog**: Fast logging
- **fmt**: Modern formatting
- **argparse**: Command-line parsing
- **tomlplusplus**: TOML config parsing

---

## Join 策略与配置

### 策略兼容性规则

| 分区策略 | 兼容的窗口状态 | 说明 |
|---------|---------------|------|
| RoundRobin | SharedWindowState | 随机分发需要共享状态保证完整性 |
| KeyPartitioner | Partitioned/Shared | 基于 key 分区 |
| VectorHash | Partitioned | 相似向量聚集到同一分区 |
| LSH | PartitionedVectorState | VSJoin 专用 |
| Centroid | Partitioned | S3J 专用 |

### 关键约束

**不兼容的配置组合会导致召回率下降**：
- ❌ RoundRobin + PartitionedWindowState → 跨分区匹配丢失
- ❌ VSJoin + SharedWindowState → 架构不支持

### 配置驱动开发

参考文档：
- `docs/tasks/TASK_GROUP_C_INTEGRATION.md`: 配置驱动策略任务
- `docs/tasks/TASK_GROUP_C_BASELINES.md`: Baseline 实现任务
- `docs/JOIN_PIPELINE_GUIDE.md`: Join 流程详解
- `docs/TEST_TOOLS_GUIDE.md`: 测试工具完整使用指南
- `docs/ADDING_NEW_JOIN_METHOD.md`: 添加新 Join 方法的详细步骤

---

## Adding New Join Methods

To add a new Join algorithm, follow these key steps (see `docs/ADDING_NEW_JOIN_METHOD.md` for full details):

1. **Add enum values**: `JoinAlgorithm` in `join_strategy_config.h`, `JoinMethodType` in `base_method.h`
2. **Implement algorithm**: Create `my_new_join_method.h/cpp` inheriting from `BaseMethod`
3. **Add config params**: Extend `JoinStrategyConfig` struct with algorithm-specific parameters
4. **Update validator**: Add validation rules in `join_config_validator.cpp`
5. **Register in factory**: Update `JoinStrategyFactory::createMethod()`
6. **Add tests**: Unit tests and TOML config in `config/integration_test_cases.toml`
7. **Update docs**: `copilot-instructions.md`, `JOIN_PIPELINE_GUIDE.md`

**Critical validation**: After adding a new method, run:
```bash
./build/bin/test_join_datasource_modes
```

---

## Notes for AI Assistants

1. **RuntimeContext**: Always pass and use `RuntimeContext` in operator methods for thread identity
2. **Connection Strategy**: Respect the current connection strategy when modifying data flow
3. **State Management**: Use `WindowState` abstraction; check `isShared()` before assuming isolation
4. **Index Thread Safety**: Always use `ConcurrencyManager` for index operations, never access `Index` directly
5. **Index Creation**: Use `create_index()` for standard types, `register_index()` for custom types (e.g., PartitionedIndex)
6. **Queue Management**: Use `QueuePtr` (shared_ptr<IQueue>) for inter-operator communication
7. **Slot IDs**: Join operators use slot IDs to distinguish left/right inputs
8. **clang-tidy**: All new code must pass clang-tidy checks (see `.clang-tidy`)
9. **Testing**: Write tests for new functionality in appropriate `test/` subdirectory
10. **Documentation**: Update docs for public API changes
11. **Parallel Execution**: Consider subtask index when accessing partitioned state
12. **Join Strategy Compatibility**: Ensure partition strategy matches window state type (see compatibility table above)
13. **Baseline Methods**: Each baseline has recommended partition and window state - use `JoinStrategyConfig::inferDefaults()` when implementing
14. **Join Integration Test**: **CRITICAL** - When modifying code that may affect Join pipeline (connection strategy, queue routing, WindowState, Partitioner, JoinOperator, etc.), MUST run `test_join_datasource_modes` to verify correctness:
    ```bash
    ./build/bin/test_join_datasource_modes
    ```
    This test validates Join recall/precision across multiple parallelism levels (1,2,4,8,16) and methods (bruteforce, ivf).
15. **Test Utilities**: Use existing test utilities from `test/test_utils/` instead of creating custom implementations:
    - `JoinConfigLoader` for loading strategy configs
    - `IntegrationTestConfigLoader` for test cases
    - `JoinIntegrationPipelineHelper` for building test pipelines
    - `TestDataGenerator` for generating test data
    - `JoinMetricsCollector` for metrics collection
    - `TestReportGenerator` for report generation
16. **Adding New Join Methods**: Follow the checklist in `docs/ADDING_NEW_JOIN_METHOD.md`. Key files to modify:
    - `include/operator/join_strategy_config.h` (enums and params)
    - `include/operator/join_operator_methods/base_method.h` (method type enum)
    - `src/operator/join_strategy_factory.cpp` (factory registration)
    - `src/operator/join_config_validator.cpp` (validation rules)
    - `config/join_strategies.toml` (strategy config)
    - `config/integration_test_cases.toml` (test cases)



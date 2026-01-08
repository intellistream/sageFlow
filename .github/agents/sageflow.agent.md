---
description: 'SageFlow 项目专用开发助手，专注于向量流处理引擎的 C++20 开发、测试与调试。'
tools: ['vscode', 'execute', 'read', 'agent', 'edit', 'search', 'web', 'pylance-mcp-server/*', 'todo', 'github.vscode-pull-request-github/copilotCodingAgent', 'github.vscode-pull-request-github/issue_fetch', 'github.vscode-pull-request-github/suggest-fix', 'github.vscode-pull-request-github/searchSyntax', 'github.vscode-pull-request-github/doSearch', 'github.vscode-pull-request-github/renderIssues', 'github.vscode-pull-request-github/activePullRequest', 'github.vscode-pull-request-github/openPullRequest', 'ms-python.python/getPythonEnvironmentInfo', 'ms-python.python/getPythonExecutableCommand', 'ms-python.python/installPythonPackage', 'ms-python.python/configurePythonEnvironment']
---

# SageFlow Development Agent

## 项目概述

**SageFlow** 是一个向量原生流处理引擎，专为实时 LLM 生成任务设计。它提供声明式 API 来组合时间窗口内的有状态向量操作，为动态变化的数据集提供快速、高效的语义上下文更新。

### 核心应用场景
- **实时 LLM 生成**: 为大语言模型提供新鲜的、有状态的上下文快照
- **动态上下文维护**: 适用于具有快速演变上下文的对话式 AI
- **流式向量分析**: 对向量数据进行高速语义查询
- **自适应推荐系统**: 基于流式事件的实时推荐

## Agent 职责

### 我能帮助的任务
- ✅ C++20 代码开发（Operator、Index、State 等核心组件）
- ✅ CMake 构建系统配置与调试
- ✅ Google Test 单元测试和集成测试编写
- ✅ Join 算法实现与优化（BruteForce, IVF, HNSW）
- ✅ 并发控制与线程安全代码审查
- ✅ Python bindings (pybind11) 开发
- ✅ 性能分析与优化建议
- ✅ 代码审查与 clang-tidy 合规检查
- ✅ GitHub PR/Issue 管理与代码审查

### 我不会做的事情
- ❌ 修改不属于 SageFlow 项目的代码
- ❌ 跳过必要的测试验证
- ❌ 违反项目命名规范和代码风格
- ❌ 直接访问 Index 而不通过 ConcurrencyManager

---

## 技术栈

| 类别 | 技术 |
|------|------|
| **语言** | C++20 |
| **构建系统** | CMake (≥3.20) |
| **测试框架** | Google Test (gtest) |
| **日志** | spdlog |
| **配置** | tomlplusplus (TOML) |
| **格式化** | fmt library |
| **CLI** | argparse |
| **Python 绑定** | pybind11 |

---

## 项目结构

```
sageFlow/
├── include/          # 头文件 (公共 API)
│   ├── common/       # 通用工具和数据类型 (VectorRecord, Response)
│   ├── compute_engine/  # 相似度/距离计算 (cosine, L2)
│   ├── concurrency/  # 并发控制器 (线程安全索引访问)
│   ├── execution/    # 执行图、顶点、运行时上下文、队列
│   ├── function/     # 用户自定义函数 (Filter, Map, Join)
│   ├── index/        # 向量索引实现 (HNSW, IVF, Bruteforce)
│   ├── operator/     # 流操作符 (TopK, Filter, Join, Map)
│   ├── query/        # 查询优化器和规划器
│   ├── state/        # 窗口状态管理 (partitioned/shared)
│   ├── storage/      # 向量记录存储管理器
│   ├── stream/       # 流抽象和数据源
│   └── utils/        # 工具函数、日志、配置
├── src/              # 实现文件 (镜像 include/ 结构)
├── test/             # 测试文件
│   ├── UnitTest/     # 单元测试
│   ├── IntegrationTest/  # 集成测试
│   ├── Performance/  # 性能基准测试
│   └── test_utils/   # 测试工具 (数据生成、验证)
├── examples/         # 示例应用
├── config/           # 配置文件 (TOML)
├── docs/             # 文档
└── python/           # Python 绑定 (pybind11)
```

---

## 命名规范 (clang-tidy 强制执行)

| 类型 | 规范 | 示例 |
|------|------|------|
| **类** | `CamelCase` | `RuntimeContext`, `WindowState` |
| **类方法** | `camelBack` | `getSubtaskIndex()`, `processRecord()` |
| **成员变量** | `lower_case_` (尾随下划线) | `subtask_index_`, `parallelism_` |
| **命名空间** | `lower_case` | `sageFlow` |
| **全局函数** | `CamelCase` | `CreateIndex()` |
| **全局常量** | `UPPER_CASE` | `MAX_BUFFER_SIZE` |
| **变量/参数** | `lower_case` | `record_count`, `window_size` |
| **枚举/结构体** | `CamelCase` | `IndexType`, `VectorRecord` |

---

## 核心架构概念

### 三阶段流水线
1. **Ingestion**: 从数据源输入 (`DataStreamSource`, `FileStreamSource`)
2. **State Materialization**: 窗口内的有状态计算 (Join, TopK, Aggregate)
3. **Snapshot Exposure**: 通过 Sink 操作符暴露结果

### 关键抽象

#### ExecutionGraph & ExecutionVertex
```cpp
// ExecutionGraph: 管理操作符 DAG 和并行执行
class ExecutionGraph {
    void addOperator(std::shared_ptr<Operator> op);
    void connectOperators(upstream, downstream, slot);
    void buildGraph();  // 创建 ExecutionVertex 实例
    void start();       // 启动所有工作线程
    void stop();        // 优雅关闭
};
```

#### RuntimeContext
```cpp
class RuntimeContext {
    size_t getSubtaskIndex() const;  // 当前并行实例 (0-based)
    size_t getParallelism() const;   // 总并行度
    std::string getTaskName() const; // "Task[2/8]" 格式
};
```

#### WindowState
```cpp
class WindowState {
    virtual void addRecord(std::unique_ptr<VectorRecord> record, size_t subtask_index) = 0;
    virtual const std::deque<std::unique_ptr<VectorRecord>>& getRecords(size_t subtask_index) const = 0;
    virtual void evictExpired(int64_t current_ts, int64_t window_size, size_t subtask_index) = 0;
    virtual bool isShared() const = 0;
};
// PartitionedWindowState: 每个 subtask 独立状态
// SharedWindowState: 所有 subtask 共享状态 (需要同步)
```

#### ConcurrencyManager (索引管理)
```cpp
class ConcurrencyManager {
    int create_index(name, IndexType, dimension, params);      // 标准索引
    int register_index(name, std::shared_ptr<Index> index);    // 自定义索引
    bool insert(index_id, std::unique_ptr<VectorRecord> record);
    std::vector<std::shared_ptr<const VectorRecord>> query(index_id, record, k);
};
```

---

## 构建与测试命令

```bash
# 配置
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON

# 构建
cmake --build build -j $(nproc)

# 运行所有测试
ctest --test-dir build --output-on-failure

# 运行特定测试
./build/bin/test_window_state

# 按标签运行测试
ctest --test-dir build -L UNIT --output-on-failure
ctest --test-dir build -L INTEGRATION --output-on-failure
```

---

## Join 策略兼容性规则

这部分容易误导：当前仓库虽然定义了多种 `PartitionStrategy/WindowStateType`，但**端到端链路真正稳定跑通**的主路径主要有两条（其余视为“暂未完全实现/不保证可用”）。

### 策略 1：共享索引（Shared Index Join，主路径）
- **组合**：`partition_strategy=ROUND_ROBIN` + `window_state_type=SHARED` + `index_strategy=SHARED`
- **说明**：RoundRobin 负载均衡必须共享状态；否则跨分区匹配会丢失（recall drop）。

### 策略 2：ClusteredJoin（分区索引 Join，主路径）
- **组合**：`partition_strategy=CENTROID` + `window_state_type=PARTITIONED/TWO_TIER` + `index_strategy=PARTITIONED`
- **强约束**：**`num_partitions == parallelism`**
- **说明**：使用 `CentroidPartitioner`（cold_start + overlap_ratio/multicast_k），用于 ClusteredJoin 实验（A/B）与真实数据集评测。

### 其他策略（暂未完全实现/不保证可用）
- 例如基于 key 的分区、VSJoin 的 LSH 分区、S3J 等：即使枚举/validator 中存在，当前版本可能缺少适配器或完整测试覆盖，**不要默认可用**。

---

## 开发规范

### 日志使用
```cpp
SAGEFLOW_LOG_DEBUG("TAG", "Message with {} args", value);
SAGEFLOW_LOG_INFO("TAG", "Informational message");
SAGEFLOW_LOG_WARN("TAG", "Warning: {}", issue);
SAGEFLOW_LOG_ERROR("TAG", "Error occurred: {}", error_msg);
```

### 测试工具 (test/test_utils/)
- `TestDataGenerator`: 生成具有可控相似度属性的合成向量数据集
- `JoinTestHelper`: 简化 Join 测试的左/右流创建
- `BaselineJoinChecker`: 计算 Join 操作的 ground truth

### 性能优化建议
- 对大对象使用 `std::move` (尤其是 `VectorRecord`)
- 优先使用 `emplace_back` 而非 `push_back`
- 已知大小时使用 `reserve()`
- 热路径考虑缓存局部性
- 尽可能使用无锁结构 (参见 `concurrency/`)

---

## 关键约束 (必须遵守)

1. **RuntimeContext**: 在操作符方法中始终传递和使用 `RuntimeContext` 获取线程标识
2. **Index 线程安全**: 始终通过 `ConcurrencyManager` 操作索引，永不直接访问 `Index`
3. **索引创建**: 标准类型用 `create_index()`，自定义类型用 `register_index()`
4. **状态管理**: 使用 `WindowState` 抽象；访问前检查 `isShared()`
5. **clang-tidy**: 所有新代码必须通过 clang-tidy 检查
6. **Join 集成测试**: 修改 Join 相关代码后，**必须**运行(不用全量测试，跑关键用例):
   ```bash
   ./build/bin/test_join_datasource_modes
   ```

---

## 输入/输出规范

### 理想输入
- 明确的功能需求描述
- 相关的代码文件路径
- 测试用例或期望行为
- 性能要求（如适用）

### 输出格式
- 代码更改通过编辑工具直接应用
- 提供简洁的变更说明
- 必要时附上测试命令
- 重大更改时建议 code review 要点

### 进度报告
- 使用 todo 工具跟踪多步骤任务
- 完成每个步骤后及时更新状态
- 遇到阻塞时明确说明原因和需要的信息
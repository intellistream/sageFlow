# Group C: VSJoin 集成与配置驱动的自适应策略

本文档定义 VSJoin 集成任务和配置驱动的 Baseline 方法选择系统。

---

## 任务概览

| 任务ID | 名称 | 优先级 | 预估工时 | 依赖 | 状态 |
|--------|------|--------|----------|------|------|
| C-01 | VSJoin 集成到 JoinOperator | 🔴 高 | 2-3天 | B01-B04 | ✅ 完成 |
| C-02 | 配置驱动的 Join 策略工厂 | 🔴 高 | 2天 | C-01 | ⬜ 待开始 |
| C-03 | 分区策略自适应选择 | 🔴 高 | 1-2天 | C-02 | ⬜ 待开始 |
| C-04 | 窗口状态自适应选择 | 🔴 高 | 1天 | C-02 | ⬜ 待开始 |
| C-05 | Baseline 方法注册与切换 | 🟡 中 | 2天 | C-02, D-01~D-06 | ⬜ 待开始 |
| C-06 | 配置验证与错误处理 | 🟡 中 | 1天 | C-02~C-05 | ⬜ 待开始 |

---

## C-01: VSJoin 集成到 JoinOperator ✅ 已完成

### 完成内容

1. **VSJoinConfig 配置结构**
   - 位置: `include/operator/join_operator.h`
   - 包含所有 VSJoin 相关参数

2. **VSJoin 组件初始化**
   - `initVSJoinComponents()` 方法
   - 初始化 PartitionedVectorState, PartitionedIndex, PartitionCoordinator, AsyncCandidateGenerator, DistanceVerifier

3. **VSJoin 执行路径**
   - `applyVSJoin()` 方法
   - `executeVSJoinEager()` 和 `executeVSJoinLazy()` 方法

4. **索引注册规范**
   - 通过 `ConcurrencyManager::register_index()` 注册外部创建的索引
   - 自动配置 `storage_manager_`

---

## C-02: 配置驱动的 Join 策略工厂

**优先级**: 🔴 高  
**预估工时**: 2天  
**依赖**: C-01  
**输出文件**:
- `include/operator/join_strategy_factory.h`
- `src/operator/join_strategy_factory.cpp`
- `include/operator/join_strategy_config.h`
- `config/join_strategies.toml`
- `test/UnitTest/test_join_strategy_factory.cpp`

### 任务描述

实现一个工厂类，根据 TOML 配置文件动态选择和创建 Join 策略，包括：
- Join 算法（BruteForce, IVF, HNSW, HDR-Tree, S3J, ClusteredJoin, VSJoin）
- 分区策略（RoundRobin, KeyPartitioner, VectorHash, VSJoinPartitioner）
- 窗口状态（Shared, Partitioned, TwoTier, PartitionedVector）

### 提示词

```
你是 sageFlow 项目的开发者，需要实现配置驱动的 Join 策略工厂。

## 背景
SageFlow 需要支持多种 Join Baseline 方法的对比实验，每种方法对分区策略和窗口状态有不同的要求。需要通过 TOML 配置文件统一管理这些选择，避免硬编码。

## 任务目标
1. 定义统一的 JoinStrategyConfig 配置结构
2. 实现 JoinStrategyFactory 工厂类
3. 修改 JoinOperator 使用工厂创建策略

## 文件位置
- 配置结构: include/operator/join_strategy_config.h
- 工厂类: include/operator/join_strategy_factory.h
- 工厂实现: src/operator/join_strategy_factory.cpp
- 配置文件: config/join_strategies.toml

## JoinStrategyConfig 定义

```cpp
#pragma once

#include <string>
#include <optional>

namespace sageFlow {

/**
 * @brief Join 算法类型枚举
 */
enum class JoinAlgorithm {
    BRUTEFORCE,      // Ground Truth baseline
    IVF,             // IVF-based approximate join
    HNSW,            // HNSW-based approximate join
    HDR_TREE,        // HDR-Tree baseline (D-02)
    CLUSTERED_JOIN,  // VectraFlow ClusteredJoin (D-05)
    S3J,             // DEBS'23 S3J baseline (D-06)
    VSJOIN           // Our method
};

/**
 * @brief 分区策略类型枚举
 */
enum class PartitionStrategy {
    ROUND_ROBIN,     // 轮询分发（需要 SharedWindowState）
    KEY_HASH,        // 基于 key 的哈希分区
    VECTOR_HASH,     // 基于向量内容的哈希分区
    LSH,             // 局部敏感哈希分区（VSJoin）
    CENTROID         // 基于质心的分区（S3J）
};

/**
 * @brief 窗口状态类型枚举
 */
enum class WindowStateType {
    SHARED,              // SharedWindowState（所有实例共享）
    PARTITIONED,         // PartitionedWindowState（每个 subtask 独立）
    TWO_TIER,            // TwoTierWindowState（写友好层+紧凑层）
    PARTITIONED_VECTOR   // PartitionedVectorState（向量空间分区）
};

/**
 * @brief 索引类型枚举
 */
enum class IndexStrategy {
    SHARED,           // 共享索引（所有实例使用同一索引）
    PARTITIONED       // 分区索引（每个分区独立索引）
};

/**
 * @brief Join 策略完整配置
 */
struct JoinStrategyConfig {
    // 基础配置
    JoinAlgorithm algorithm = JoinAlgorithm::BRUTEFORCE;
    bool is_eager = false;  // true=Eager模式, false=Lazy模式
    double similarity_threshold = 0.8;
    
    // 分区配置
    PartitionStrategy partition_strategy = PartitionStrategy::ROUND_ROBIN;
    int num_partitions = 4;  // 向量空间分区数（用于 LSH/CENTROID）
    
    // 窗口状态配置
    WindowStateType window_state_type = WindowStateType::SHARED;
    int64_t window_size_ms = 10000;
    int64_t step_size_ms = 1000;
    
    // 索引配置
    IndexStrategy index_strategy = IndexStrategy::SHARED;
    
    // IVF 参数
    int ivf_nlist = 100;
    int ivf_nprobes = 10;
    double ivf_rebuild_threshold = 0.3;
    
    // HNSW 参数
    int hnsw_m = 16;
    int hnsw_ef_construction = 200;
    int hnsw_ef_search = 50;
    
    // VSJoin 特定参数
    int vsjoin_num_hash_functions = 8;
    double vsjoin_boundary_threshold = 0.1;
    int vsjoin_async_threads = 2;
    int64_t vsjoin_allowed_lateness = 1000;
    
    // S3J 特定参数
    int s3j_num_centroids = 16;
    
    // HDR-Tree 特定参数
    int hdr_projected_dim = 8;
    int hdr_max_node_size = 100;
    size_t hdr_delta_buffer_size = 1000;
    
    /**
     * @brief 验证配置的一致性
     * @return 错误信息，空表示验证通过
     */
    std::string validate() const;
    
    /**
     * @brief 推断默认的分区和窗口策略
     * 根据算法类型自动设置合适的策略
     */
    void inferDefaults();
};

/**
 * @brief 从 TOML 配置加载 JoinStrategyConfig
 */
JoinStrategyConfig loadJoinStrategyConfig(const std::string& config_path);

} // namespace sageFlow
```

## JoinStrategyFactory 定义

```cpp
#pragma once

#include "operator/join_strategy_config.h"
#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"
#include "execution/partitioner.h"
#include "concurrency/concurrency_manager.h"
#include <memory>

namespace sageFlow {

/**
 * @brief Join 策略工厂
 * 
 * 根据配置创建完整的 Join 策略组件，包括：
 * - JoinMethod (候选生成和执行逻辑)
 * - WindowState (左右窗口状态)
 * - Partitioner (上游到 Join 算子的分区器)
 * - Index (共享或分区索引)
 */
class JoinStrategyFactory {
public:
    /**
     * @brief 策略组件集合
     */
    struct StrategyComponents {
        std::unique_ptr<BaseMethod> join_method;
        std::unique_ptr<WindowState> left_state;
        std::unique_ptr<WindowState> right_state;
        std::unique_ptr<IPartitioner> partitioner;
        
        // 索引 ID（如果使用共享索引）
        int left_index_id = -1;
        int right_index_id = -1;
        
        // 分区索引（如果使用分区索引）
        std::shared_ptr<Index> left_partitioned_index;
        std::shared_ptr<Index> right_partitioned_index;
        
        // VSJoin 专用组件
        std::shared_ptr<VectorSpacePartitioner> vector_partitioner;
        std::unique_ptr<PartitionCoordinator> coordinator;
        std::unique_ptr<AsyncCandidateGenerator> left_async_gen;
        std::unique_ptr<AsyncCandidateGenerator> right_async_gen;
        std::shared_ptr<DistanceVerifier> verifier;
    };
    
    /**
     * @brief 根据配置创建策略组件
     * @param config 策略配置
     * @param concurrency_manager 并发管理器
     * @param dimension 向量维度
     * @param parallelism 算子并行度
     * @return 策略组件集合
     */
    static StrategyComponents create(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> concurrency_manager,
        int dimension,
        size_t parallelism);
    
private:
    static std::unique_ptr<BaseMethod> createJoinMethod(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int dimension);
    
    static std::unique_ptr<WindowState> createWindowState(
        const JoinStrategyConfig& config,
        size_t parallelism);
    
    static std::unique_ptr<IPartitioner> createPartitioner(
        const JoinStrategyConfig& config,
        int dimension);
};

} // namespace sageFlow
```

## TOML 配置文件格式

```toml
# config/join_strategies.toml

# ============================================================
# 默认配置（可被具体策略覆盖）
# ============================================================
[default]
similarity_threshold = 0.8
window_size_ms = 10000
step_size_ms = 1000
is_eager = false

# ============================================================
# 预定义策略配置
# ============================================================

[strategies.bruteforce_baseline]
algorithm = "bruteforce"
is_eager = false
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "shared"

[strategies.ivf_baseline]
algorithm = "ivf"
is_eager = true
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "shared"
ivf_nlist = 100
ivf_nprobes = 10

[strategies.hnsw_baseline]
algorithm = "hnsw"
is_eager = true
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "shared"
hnsw_m = 16
hnsw_ef_construction = 200
hnsw_ef_search = 50

[strategies.hdr_tree_baseline]
algorithm = "hdr_tree"
is_eager = true
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "shared"
hdr_projected_dim = 8
hdr_max_node_size = 100
hdr_delta_buffer_size = 1000

[strategies.clustered_join_baseline]
algorithm = "clustered_join"
is_eager = true
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
num_partitions = 16

[strategies.s3j_baseline]
algorithm = "s3j"
is_eager = true
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "partitioned"
s3j_num_centroids = 16
num_partitions = 16

[strategies.vsjoin]
algorithm = "vsjoin"
is_eager = true
partition_strategy = "lsh"
window_state_type = "partitioned_vector"
index_strategy = "partitioned"
num_partitions = 8
vsjoin_num_hash_functions = 8
vsjoin_boundary_threshold = 0.1
vsjoin_async_threads = 2
vsjoin_allowed_lateness = 1000

# ============================================================
# 策略兼容性规则
# ============================================================
# 
# 以下是各策略之间的兼容性约束：
#
# 1. partition_strategy = "round_robin" 
#    => 必须使用 window_state_type = "shared"
#    => 原因：RoundRobin 会随机分发记录，分区状态会导致匹配丢失
#
# 2. partition_strategy = "lsh" | "centroid" | "vector_hash"
#    => 可以使用 window_state_type = "partitioned" | "partitioned_vector"
#    => 原因：基于内容的分区保证相似向量在同一分区
#
# 3. algorithm = "vsjoin"
#    => 必须使用 partition_strategy = "lsh"
#    => 必须使用 window_state_type = "partitioned_vector"
#    => 必须使用 index_strategy = "partitioned"
#
# 4. algorithm = "s3j"
#    => 必须使用 partition_strategy = "centroid"
#
```

## 实现要点

1. **配置加载**:
   - 使用 tomlplusplus 解析配置文件
   - 支持默认值覆盖
   - 支持通过策略名称引用预定义配置

2. **配置验证**:
   - 检查策略组合的兼容性
   - 在 `validate()` 中返回详细错误信息
   - 在工厂创建时提前失败

3. **策略推断**:
   - `inferDefaults()` 根据算法类型设置默认的分区和窗口策略
   - 例如：选择 VSJOIN 时自动设置 LSH 分区和 PartitionedVectorState

4. **工厂创建流程**:
   ```cpp
   StrategyComponents JoinStrategyFactory::create(...) {
       // 1. 验证配置
       auto error = config.validate();
       if (!error.empty()) throw std::runtime_error(error);
       
       // 2. 创建索引（共享或分区）
       // 3. 创建 JoinMethod
       // 4. 创建 WindowState
       // 5. 创建 Partitioner
       // 6. 如果是 VSJoin，创建额外组件
       
       return components;
   }
   ```

## 测试要求

```cpp
TEST(JoinStrategyFactoryTest, LoadConfigFromToml) {
    auto config = loadJoinStrategyConfig("config/join_strategies.toml");
    // 验证加载正确
}

TEST(JoinStrategyFactoryTest, ValidateCompatibility) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;
    
    auto error = config.validate();
    EXPECT_FALSE(error.empty());  // 应该报错
}

TEST(JoinStrategyFactoryTest, CreateVSJoinStrategy) {
    auto config = loadJoinStrategyConfig("config/join_strategies.toml");
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.inferDefaults();
    
    auto components = JoinStrategyFactory::create(config, cm, 128, 4);
    EXPECT_NE(components.vector_partitioner, nullptr);
    EXPECT_NE(components.coordinator, nullptr);
}

TEST(JoinStrategyFactoryTest, CreateAllBaselineStrategies) {
    // 遍历所有预定义策略，验证都能正确创建
}
```

## 验收标准
1. 所有单元测试通过
2. 配置文件格式清晰，易于扩展
3. 错误配置有明确的错误提示
4. 与现有 JoinOperator 兼容
```

---

## C-03: 分区策略自适应选择

**优先级**: 🔴 高  
**预估工时**: 1-2天  
**依赖**: C-02  
**输出文件**:
- `include/execution/partitioner_factory.h`
- `src/execution/partitioner_factory.cpp`
- 修改 `src/execution/partitioned_connection_strategy.cpp`

### 任务描述

根据 JoinStrategyConfig 动态选择分区器，并集成到连接策略中。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现分区策略的自适应选择。

## 背景
不同的 Join Baseline 方法需要不同的分区策略：
- BruteForce/IVF/HNSW: 使用 RoundRobin + SharedWindowState
- S3J: 使用 CentroidPartitioner + PartitionedWindowState
- VSJoin: 使用 LSHPartitioner + PartitionedVectorState

当前 PartitionedConnectionStrategy 硬编码使用 RoundRobinPartitioner，需要支持动态选择。

## 任务目标
1. 实现 PartitionerFactory 根据配置创建分区器
2. 修改 PartitionedConnectionStrategy 支持自定义分区器
3. 在 ExecutionGraph 构建时传入正确的分区器

## 实现要点

### 1. PartitionerFactory

```cpp
class PartitionerFactory {
public:
    static std::unique_ptr<IPartitioner> create(
        PartitionStrategy strategy,
        int dimension,
        const JoinStrategyConfig& config);
};
```

### 2. 修改 PartitionedConnectionStrategy

```cpp
class PartitionedConnectionStrategy : public IConnectionStrategy {
public:
    // 新增：设置自定义分区器
    void setPartitioner(std::unique_ptr<IPartitioner> partitioner);
    
    void setupResultPartition(...) override {
        if (custom_partitioner_) {
            result_partition->setup(std::move(custom_partitioner_), ...);
        } else {
            // 默认 RoundRobin
            result_partition->setup(std::make_unique<RoundRobinPartitioner>(), ...);
        }
    }

private:
    std::unique_ptr<IPartitioner> custom_partitioner_;
};
```

### 3. LSHPartitioner 实现

```cpp
class LSHPartitioner : public IPartitioner {
public:
    LSHPartitioner(int dimension, int num_hash_functions, int num_partitions);
    
    int partition(const Response& record, int num_channels) override {
        if (!record.record_) return 0;
        auto hash = computeLSHHash(record.record_->getVector());
        return hash % num_channels;
    }

private:
    std::vector<std::vector<float>> random_projections_;
    
    uint32_t computeLSHHash(const std::vector<float>& vec);
};
```

### 4. CentroidPartitioner 实现

```cpp
class CentroidPartitioner : public IPartitioner {
public:
    CentroidPartitioner(int num_centroids);
    
    void initCentroids(const std::vector<std::vector<float>>& samples);
    void updateCentroids(const std::vector<std::vector<float>>& new_centroids);
    
    int partition(const Response& record, int num_channels) override;

private:
    std::vector<std::vector<float>> centroids_;
};
```

## 验收标准
1. 不同策略创建正确的分区器
2. LSH 分区保证相似向量局部性
3. 与现有连接策略兼容
```

---

## C-04: 窗口状态自适应选择

**优先级**: 🔴 高  
**预估工时**: 1天  
**依赖**: C-02  
**输出文件**:
- `include/state/window_state_factory.h`
- `src/state/window_state_factory.cpp`
- 修改 `src/operator/join_operator.cpp`

### 任务描述

根据 JoinStrategyConfig 动态选择窗口状态类型。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现窗口状态的自适应选择。

## 背景
不同的 Join 方法需要不同的窗口状态：
- SharedWindowState: 共享状态，适用于 RoundRobin 分区
- PartitionedWindowState: 分区状态，适用于内容分区
- TwoTierWindowState: 双层结构，优化写入和查询
- PartitionedVectorState: 向量空间分区状态，VSJoin 专用

## 任务目标
1. 实现 WindowStateFactory
2. 修改 JoinOperator::open() 使用工厂创建状态

## WindowStateFactory 接口

```cpp
class WindowStateFactory {
public:
    static std::unique_ptr<WindowState> create(
        WindowStateType type,
        size_t parallelism,
        const JoinStrategyConfig& config,
        std::shared_ptr<VectorSpacePartitioner> partitioner = nullptr);
};
```

## 实现要点

1. 根据 WindowStateType 创建对应的状态实现
2. TwoTierWindowState 需要传入压缩参数
3. PartitionedVectorState 需要传入向量分区器

## 验收标准
1. 所有窗口状态类型正确创建
2. 配置参数正确传递
3. 与现有 JoinOperator 兼容
```

---

## C-05: Baseline 方法注册与切换

**优先级**: 🟡 中  
**预估工时**: 2天  
**依赖**: C-02, D-01~D-06  
**输出文件**:
- `include/operator/join_method_registry.h`
- `src/operator/join_method_registry.cpp`
- 修改现有 JoinMethod 实现

### 任务描述

实现 Baseline 方法的统一注册和动态切换机制。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 Baseline 方法的注册系统。

## 背景
随着 Baseline 实现的增加（D-01 ~ D-06），需要一个统一的方式来：
- 注册新的 Join 方法
- 根据配置动态选择方法
- 获取方法的元信息（名称、参数、特性）

## 任务目标
1. 实现 JoinMethodRegistry 单例
2. 各 Baseline 方法自注册
3. 支持运行时查询可用方法

## JoinMethodRegistry 接口

```cpp
class JoinMethodRegistry {
public:
    using MethodCreator = std::function<
        std::unique_ptr<BaseMethod>(const JoinStrategyConfig&, 
                                    std::shared_ptr<ConcurrencyManager>,
                                    int dimension)>;
    
    struct MethodInfo {
        std::string name;
        std::string description;
        JoinAlgorithm algorithm;
        bool supports_eager;
        bool supports_lazy;
        PartitionStrategy recommended_partition;
        WindowStateType recommended_window_state;
    };
    
    static JoinMethodRegistry& instance();
    
    void registerMethod(JoinAlgorithm algorithm, 
                       MethodInfo info,
                       MethodCreator creator);
    
    std::unique_ptr<BaseMethod> createMethod(
        JoinAlgorithm algorithm,
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int dimension);
    
    std::vector<MethodInfo> getAvailableMethods() const;
    
    const MethodInfo& getMethodInfo(JoinAlgorithm algorithm) const;
};

// 自动注册宏
#define REGISTER_JOIN_METHOD(Algorithm, Info, Creator) \
    static bool _registered_##Algorithm = []() { \
        JoinMethodRegistry::instance().registerMethod( \
            Algorithm, Info, Creator); \
        return true; \
    }();
```

## 使用示例

```cpp
// 在 bruteforce_join_method.cpp 中
REGISTER_JOIN_METHOD(
    JoinAlgorithm::BRUTEFORCE,
    {
        .name = "BruteForce",
        .description = "Ground truth baseline with brute-force scan",
        .algorithm = JoinAlgorithm::BRUTEFORCE,
        .supports_eager = true,
        .supports_lazy = true,
        .recommended_partition = PartitionStrategy::ROUND_ROBIN,
        .recommended_window_state = WindowStateType::SHARED
    },
    [](const JoinStrategyConfig& config, auto cm, int dim) {
        return std::make_unique<BruteForceJoinMethod>(
            config.similarity_threshold, config.window_size_ms);
    }
);
```

## 验收标准
1. 所有 Baseline 方法正确注册
2. 动态创建功能正常
3. 方法信息查询正确
```

---

## C-06: 配置验证与错误处理

**优先级**: 🟡 中  
**预估工时**: 1天  
**依赖**: C-02~C-05  
**输出文件**:
- `include/operator/join_config_validator.h`
- `src/operator/join_config_validator.cpp`
- `test/UnitTest/test_join_config_validator.cpp`

### 任务描述

实现配置验证器，确保用户配置的合法性和一致性。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现配置验证系统。

## 背景
用户可能配置了不兼容的策略组合，如：
- RoundRobin 分区 + PartitionedWindowState（会导致召回率下降）
- VSJoin 算法 + SharedWindowState（不支持）
需要在启动时检测这些问题并给出明确提示。

## 验证规则

1. **分区-窗口兼容性**
   - RoundRobin → 必须 SharedWindowState
   - LSH/Centroid/VectorHash → 可以 Partitioned/PartitionedVector

2. **算法-策略兼容性**
   - VSJoin → 必须 LSH + PartitionedVector + PartitionedIndex
   - S3J → 必须 Centroid + Partitioned

3. **参数范围检查**
   - similarity_threshold: [0.0, 1.0]
   - ivf_nprobes <= ivf_nlist
   - num_partitions > 0

4. **依赖检查**
   - HDR-Tree 需要 PCA 组件
   - VSJoin 需要 PartitionCoordinator 组件

## JoinConfigValidator 接口

```cpp
class JoinConfigValidator {
public:
    struct ValidationResult {
        bool valid;
        std::vector<std::string> errors;
        std::vector<std::string> warnings;
    };
    
    static ValidationResult validate(const JoinStrategyConfig& config);
    
    static void throwIfInvalid(const JoinStrategyConfig& config);
};
```

## 验收标准
1. 检测所有不兼容配置
2. 错误信息清晰可操作
3. 警告信息提示潜在问题
```

---

## 配置示例：完整的 Pipeline 配置

```toml
# config/experiment_config.toml

# ============================================================
# 实验配置：对比不同 Baseline 方法
# ============================================================

[experiment]
name = "baseline_comparison"
output_dir = "results/baseline_comparison"

# 选择使用的策略（引用 join_strategies.toml 中的预定义配置）
strategy = "vsjoin"

# ============================================================
# 数据源配置
# ============================================================
[data]
left_source = "data/sift/sift_base.fvecs"
right_source = "data/sift/sift_query.fvecs"
dimension = 128

# ============================================================
# Pipeline 配置
# ============================================================
[pipeline]
parallelism_source = 1
parallelism_join = 4
parallelism_sink = 1

# ============================================================
# 覆盖默认策略参数（可选）
# ============================================================
[strategy_override]
similarity_threshold = 0.85
window_size_ms = 5000
vsjoin_num_hash_functions = 12

# ============================================================
# 性能监控
# ============================================================
[monitoring]
enable_metrics = true
enable_profiling = false
profile_output = "profiles/experiment.prof"
```

---

## 任务依赖图

```
              ┌──────────────────────────────────────────┐
              │            D-01 ~ D-06                   │
              │        (Baseline 实现)                   │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
┌─────────────┐            ┌─────────────┐
│   B01~B04   │───────────▶│    C-01     │
│(VSJoin组件) │            │(VSJoin集成) │
└─────────────┘            └──────┬──────┘
                                  │
                                  ▼
                           ┌─────────────┐
                           │    C-02     │
                           │(策略工厂)   │
                           └──────┬──────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              ┌─────────┐   ┌─────────┐   ┌─────────┐
              │  C-03   │   │  C-04   │   │  C-05   │
              │(分区策略)│   │(窗口策略)│   │(方法注册)│
              └────┬────┘   └────┬────┘   └────┬────┘
                   │             │             │
                   └─────────────┼─────────────┘
                                 ▼
                           ┌─────────────┐
                           │    C-06     │
                           │(配置验证)   │
                           └─────────────┘
```

---

## 检查清单

| 任务ID | 名称 | 状态 | 负责人 | 开始日期 | 完成日期 |
|--------|------|------|--------|----------|----------|
| C-01 | VSJoin 集成 | ✅ | - | - | 已完成 |
| C-02 | 策略工厂 | ⬜ | - | - | - |
| C-03 | 分区策略选择 | ⬜ | - | - | - |
| C-04 | 窗口状态选择 | ⬜ | - | - | - |
| C-05 | 方法注册系统 | ⬜ | - | - | - |
| C-06 | 配置验证 | ⬜ | - | - | - |

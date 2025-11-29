# C-02: 配置驱动的 Join 策略工厂

**优先级**: 🔴 高  
**预估工时**: 2天  
**依赖**: C-01 (VSJoin 集成)  
**状态**: ⬜ 待开始

---

## 任务概述

实现一个工厂类，根据 TOML 配置文件动态选择和创建 Join 策略，包括：
- Join 算法（BruteForce, IVF, HNSW, HDR-Tree, S3J, ClusteredJoin, VSJoin）
- 分区策略（RoundRobin, KeyPartitioner, VectorHash, LSHPartitioner, CentroidPartitioner）
- 窗口状态（Shared, Partitioned, TwoTier, PartitionedVector）

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_strategy_config.h` | 策略配置结构定义 |
| `src/operator/join_strategy_config.cpp` | 配置加载和验证实现 |
| `include/operator/join_strategy_factory.h` | 工厂类定义 |
| `src/operator/join_strategy_factory.cpp` | 工厂类实现 |
| `config/join_strategies.toml` | 预定义策略配置文件 |
| `test/UnitTest/test_join_strategy_factory.cpp` | 单元测试 |

---

## 接口设计

### JoinStrategyConfig 定义

```cpp
#pragma once

#include <string>
#include <cstdint>

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
 * @brief 索引策略类型枚举
 */
enum class IndexStrategy {
    SHARED,           // 共享索引（所有实例使用同一索引）
    PARTITIONED       // 分区索引（每个分区独立索引）
};

/**
 * @brief Join 策略完整配置
 */
struct JoinStrategyConfig {
    // ==================== 基础配置 ====================
    JoinAlgorithm algorithm = JoinAlgorithm::BRUTEFORCE;
    bool is_eager = false;  // true=Eager模式, false=Lazy模式
    double similarity_threshold = 0.8;
    
    // ==================== 分区配置 ====================
    PartitionStrategy partition_strategy = PartitionStrategy::ROUND_ROBIN;
    int num_partitions = 4;  // 向量空间分区数（用于 LSH/CENTROID）
    
    // ==================== 窗口状态配置 ====================
    WindowStateType window_state_type = WindowStateType::SHARED;
    int64_t window_size_ms = 10000;
    int64_t step_size_ms = 1000;
    
    // ==================== 索引配置 ====================
    IndexStrategy index_strategy = IndexStrategy::SHARED;
    
    // ==================== IVF 参数 ====================
    int ivf_nlist = 100;
    int ivf_nprobes = 10;
    double ivf_rebuild_threshold = 0.3;
    
    // ==================== HNSW 参数 ====================
    int hnsw_m = 16;
    int hnsw_ef_construction = 200;
    int hnsw_ef_search = 50;
    
    // ==================== VSJoin 参数 ====================
    int vsjoin_num_hash_functions = 8;
    double vsjoin_boundary_threshold = 0.1;
    int vsjoin_async_threads = 2;
    int64_t vsjoin_allowed_lateness = 1000;
    
    // ==================== S3J 参数 ====================
    int s3j_num_centroids = 16;
    
    // ==================== HDR-Tree 参数 ====================
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

/**
 * @brief 枚举类型与字符串转换
 */
std::string toString(JoinAlgorithm algo);
std::string toString(PartitionStrategy ps);
std::string toString(WindowStateType ws);
std::string toString(IndexStrategy is);

JoinAlgorithm parseJoinAlgorithm(const std::string& s);
PartitionStrategy parsePartitionStrategy(const std::string& s);
WindowStateType parseWindowStateType(const std::string& s);
IndexStrategy parseIndexStrategy(const std::string& s);

} // namespace sageFlow
```

### JoinStrategyFactory 定义

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

---

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
```

---

## 实现要点

### 1. 配置加载

```cpp
JoinStrategyConfig loadJoinStrategyConfig(const std::string& config_path) {
    auto config = toml::parse_file(config_path);
    JoinStrategyConfig result;
    
    // 解析 algorithm
    if (auto algo = config["algorithm"].value<std::string>()) {
        result.algorithm = parseJoinAlgorithm(*algo);
    }
    
    // 解析 partition_strategy
    if (auto ps = config["partition_strategy"].value<std::string>()) {
        result.partition_strategy = parsePartitionStrategy(*ps);
    }
    
    // ... 其他字段
    
    return result;
}
```

### 2. 配置验证

```cpp
std::string JoinStrategyConfig::validate() const {
    std::vector<std::string> errors;
    
    // 规则1: RoundRobin 必须配 SHARED
    if (partition_strategy == PartitionStrategy::ROUND_ROBIN &&
        window_state_type != WindowStateType::SHARED) {
        errors.push_back(
            "RoundRobin partition requires SharedWindowState");
    }
    
    // 规则2: VSJoin 必须配 LSH + PARTITIONED_VECTOR
    if (algorithm == JoinAlgorithm::VSJOIN) {
        if (partition_strategy != PartitionStrategy::LSH) {
            errors.push_back("VSJoin requires LSH partition strategy");
        }
        if (window_state_type != WindowStateType::PARTITIONED_VECTOR) {
            errors.push_back("VSJoin requires PartitionedVectorState");
        }
        if (index_strategy != IndexStrategy::PARTITIONED) {
            errors.push_back("VSJoin requires partitioned index");
        }
    }
    
    // 规则3: S3J 必须配 CENTROID
    if (algorithm == JoinAlgorithm::S3J &&
        partition_strategy != PartitionStrategy::CENTROID) {
        errors.push_back("S3J requires Centroid partition strategy");
    }
    
    // 规则4: 参数范围检查
    if (similarity_threshold < 0.0 || similarity_threshold > 1.0) {
        errors.push_back("similarity_threshold must be in [0.0, 1.0]");
    }
    if (ivf_nprobes > ivf_nlist) {
        errors.push_back("ivf_nprobes cannot exceed ivf_nlist");
    }
    if (num_partitions <= 0) {
        errors.push_back("num_partitions must be positive");
    }
    
    // 返回错误信息
    if (errors.empty()) return "";
    
    std::string result;
    for (const auto& e : errors) {
        result += e + "; ";
    }
    return result;
}
```

### 3. 策略推断

```cpp
void JoinStrategyConfig::inferDefaults() {
    switch (algorithm) {
        case JoinAlgorithm::VSJOIN:
            partition_strategy = PartitionStrategy::LSH;
            window_state_type = WindowStateType::PARTITIONED_VECTOR;
            index_strategy = IndexStrategy::PARTITIONED;
            break;
            
        case JoinAlgorithm::S3J:
            partition_strategy = PartitionStrategy::CENTROID;
            window_state_type = WindowStateType::PARTITIONED;
            index_strategy = IndexStrategy::PARTITIONED;
            break;
            
        case JoinAlgorithm::CLUSTERED_JOIN:
            partition_strategy = PartitionStrategy::CENTROID;
            window_state_type = WindowStateType::PARTITIONED;
            index_strategy = IndexStrategy::PARTITIONED;
            break;
            
        case JoinAlgorithm::BRUTEFORCE:
        case JoinAlgorithm::IVF:
        case JoinAlgorithm::HNSW:
        case JoinAlgorithm::HDR_TREE:
        default:
            partition_strategy = PartitionStrategy::ROUND_ROBIN;
            window_state_type = WindowStateType::SHARED;
            index_strategy = IndexStrategy::SHARED;
            break;
    }
}
```

### 4. 工厂创建流程

```cpp
StrategyComponents JoinStrategyFactory::create(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int dimension,
    size_t parallelism) {
    
    // 1. 验证配置
    auto error = config.validate();
    if (!error.empty()) {
        throw std::runtime_error("Invalid config: " + error);
    }
    
    StrategyComponents components;
    
    // 2. 创建 JoinMethod
    components.join_method = createJoinMethod(config, cm, dimension);
    
    // 3. 创建 WindowState
    components.left_state = createWindowState(config, parallelism);
    components.right_state = createWindowState(config, parallelism);
    
    // 4. 创建 Partitioner
    components.partitioner = createPartitioner(config, dimension);
    
    // 5. 如果是 VSJoin，创建额外组件
    if (config.algorithm == JoinAlgorithm::VSJOIN) {
        // 创建 VectorSpacePartitioner
        // 创建 PartitionCoordinator
        // 创建 AsyncCandidateGenerator
        // 创建 DistanceVerifier
    }
    
    return components;
}
```

---

## 测试要求

```cpp
TEST(JoinStrategyConfigTest, LoadFromToml) {
    auto config = loadJoinStrategyConfig("config/join_strategies.toml");
    // 验证加载正确
}

TEST(JoinStrategyConfigTest, ValidateIncompatibleConfig) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;
    
    auto error = config.validate();
    EXPECT_FALSE(error.empty());  // 应该报错
    EXPECT_TRUE(error.find("SharedWindowState") != std::string::npos);
}

TEST(JoinStrategyConfigTest, InferDefaults) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.inferDefaults();
    
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::LSH);
    EXPECT_EQ(config.window_state_type, WindowStateType::PARTITIONED_VECTOR);
    EXPECT_EQ(config.index_strategy, IndexStrategy::PARTITIONED);
}

TEST(JoinStrategyFactoryTest, CreateBruteForceStrategy) {
    auto config = loadJoinStrategyConfig("config/join_strategies.toml");
    // 选择 bruteforce_baseline 策略
    
    auto cm = std::make_shared<ConcurrencyManager>();
    auto components = JoinStrategyFactory::create(config, cm, 128, 4);
    
    EXPECT_NE(components.join_method, nullptr);
    EXPECT_NE(components.left_state, nullptr);
    EXPECT_NE(components.partitioner, nullptr);
}

TEST(JoinStrategyFactoryTest, CreateVSJoinStrategy) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.inferDefaults();
    
    auto cm = std::make_shared<ConcurrencyManager>();
    auto components = JoinStrategyFactory::create(config, cm, 128, 4);
    
    EXPECT_NE(components.vector_partitioner, nullptr);
    EXPECT_NE(components.coordinator, nullptr);
}

TEST(JoinStrategyFactoryTest, CreateWithInvalidConfig) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;
    
    auto cm = std::make_shared<ConcurrencyManager>();
    EXPECT_THROW(
        JoinStrategyFactory::create(config, cm, 128, 4),
        std::runtime_error
    );
}

TEST(JoinStrategyFactoryTest, CreateAllBaselineStrategies) {
    // 遍历所有预定义策略，验证都能正确创建
    std::vector<std::string> strategies = {
        "bruteforce_baseline",
        "ivf_baseline",
        "hnsw_baseline",
        "s3j_baseline",
        "vsjoin"
    };
    
    for (const auto& strategy : strategies) {
        // 加载并创建，验证无异常
    }
}
```

---

## 验收标准

1. ✅ 所有单元测试通过
2. ✅ 配置文件格式清晰，易于扩展
3. ✅ 错误配置有明确的错误提示
4. ✅ 与现有 JoinOperator 兼容
5. ✅ 代码符合项目编码规范
6. ✅ clang-tidy 检查通过

---

## 参考资料

- [TASK_GROUP_C_INTEGRATION.md](../TASK_GROUP_C_INTEGRATION.md) - 主任务文档
- [JOIN_PIPELINE_GUIDE.md](../../JOIN_PIPELINE_GUIDE.md) - Join 流程详解
- tomlplusplus 文档: https://github.com/marzer/tomlplusplus

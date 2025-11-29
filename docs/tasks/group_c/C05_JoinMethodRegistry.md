# C-05: Baseline 方法注册与切换

**优先级**: 🟡 中  
**预估工时**: 2天  
**依赖**: C-02, D-01~D-06 (所有 Baseline 实现)  
**状态**: ⬜ 待开始

---

## 任务概述

实现 Baseline 方法的统一注册和动态切换机制。随着 Baseline 实现的增加（D-01 ~ D-06），需要一个统一的方式来：
- 注册新的 Join 方法
- 根据配置动态选择方法
- 获取方法的元信息（名称、参数、特性）

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_method_registry.h` | 方法注册中心定义 |
| `src/operator/join_method_registry.cpp` | 方法注册中心实现 |
| `test/UnitTest/test_join_method_registry.cpp` | 单元测试 |
| 修改各 Baseline 方法的 .cpp 文件 | 添加自注册代码 |

---

## 接口设计

### JoinMethodRegistry

```cpp
#pragma once

#include "operator/join_strategy_config.h"
#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"
#include <functional>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <memory>

namespace sageFlow {

/**
 * @brief Join 方法注册中心
 * 
 * 单例模式，用于管理所有 Baseline 方法的注册和创建
 */
class JoinMethodRegistry {
public:
    /**
     * @brief 方法创建器类型
     */
    using MethodCreator = std::function<
        std::unique_ptr<BaseMethod>(const JoinStrategyConfig&, 
                                    std::shared_ptr<ConcurrencyManager>,
                                    int dimension)>;
    
    /**
     * @brief 方法元信息
     */
    struct MethodInfo {
        std::string name;                           // 方法名称
        std::string description;                    // 方法描述
        JoinAlgorithm algorithm;                    // 算法类型
        bool supports_eager;                        // 是否支持 Eager 模式
        bool supports_lazy;                         // 是否支持 Lazy 模式
        PartitionStrategy recommended_partition;    // 推荐的分区策略
        WindowStateType recommended_window_state;   // 推荐的窗口状态
        std::string paper_reference;                // 论文引用（可选）
    };
    
    /**
     * @brief 获取单例实例
     */
    static JoinMethodRegistry& instance();
    
    /**
     * @brief 注册方法
     * @param algorithm 算法类型
     * @param info 方法元信息
     * @param creator 创建器函数
     */
    void registerMethod(JoinAlgorithm algorithm, 
                       MethodInfo info,
                       MethodCreator creator);
    
    /**
     * @brief 创建方法实例
     * @param algorithm 算法类型
     * @param config 策略配置
     * @param cm 并发管理器
     * @param dimension 向量维度
     * @return 方法实例
     */
    std::unique_ptr<BaseMethod> createMethod(
        JoinAlgorithm algorithm,
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> cm,
        int dimension);
    
    /**
     * @brief 获取所有可用方法
     */
    std::vector<MethodInfo> getAvailableMethods() const;
    
    /**
     * @brief 获取指定方法的元信息
     */
    const MethodInfo& getMethodInfo(JoinAlgorithm algorithm) const;
    
    /**
     * @brief 检查方法是否已注册
     */
    bool hasMethod(JoinAlgorithm algorithm) const;
    
    /**
     * @brief 获取已注册方法数量
     */
    size_t getRegisteredCount() const;

private:
    JoinMethodRegistry() = default;
    JoinMethodRegistry(const JoinMethodRegistry&) = delete;
    JoinMethodRegistry& operator=(const JoinMethodRegistry&) = delete;
    
    std::unordered_map<JoinAlgorithm, MethodInfo> infos_;
    std::unordered_map<JoinAlgorithm, MethodCreator> creators_;
    mutable std::mutex mutex_;
};

/**
 * @brief 自动注册宏
 * 
 * 使用方法：在各 Baseline 的 .cpp 文件末尾调用此宏
 */
#define REGISTER_JOIN_METHOD(Algorithm, Info, Creator) \
    namespace { \
    static bool _registered_##Algorithm = []() { \
        ::sageFlow::JoinMethodRegistry::instance().registerMethod( \
            Algorithm, Info, Creator); \
        return true; \
    }(); \
    }

} // namespace sageFlow
```

---

## 实现要点

### 1. JoinMethodRegistry 实现

```cpp
#include "operator/join_method_registry.h"
#include <stdexcept>

namespace sageFlow {

JoinMethodRegistry& JoinMethodRegistry::instance() {
    static JoinMethodRegistry instance;
    return instance;
}

void JoinMethodRegistry::registerMethod(
    JoinAlgorithm algorithm, 
    MethodInfo info,
    MethodCreator creator) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (creators_.find(algorithm) != creators_.end()) {
        // 已注册，可以选择覆盖或报警告
        SAGEFLOW_LOG_WARN("JoinMethodRegistry", 
            "Method {} already registered, overwriting", info.name);
    }
    
    infos_[algorithm] = std::move(info);
    creators_[algorithm] = std::move(creator);
    
    SAGEFLOW_LOG_INFO("JoinMethodRegistry", 
        "Registered join method: {}", infos_[algorithm].name);
}

std::unique_ptr<BaseMethod> JoinMethodRegistry::createMethod(
    JoinAlgorithm algorithm,
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int dimension) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = creators_.find(algorithm);
    if (it == creators_.end()) {
        throw std::runtime_error(
            "Unknown join algorithm: " + toString(algorithm));
    }
    
    return it->second(config, cm, dimension);
}

std::vector<JoinMethodRegistry::MethodInfo> 
JoinMethodRegistry::getAvailableMethods() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<MethodInfo> result;
    result.reserve(infos_.size());
    
    for (const auto& [algo, info] : infos_) {
        result.push_back(info);
    }
    
    return result;
}

const JoinMethodRegistry::MethodInfo& 
JoinMethodRegistry::getMethodInfo(JoinAlgorithm algorithm) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = infos_.find(algorithm);
    if (it == infos_.end()) {
        throw std::runtime_error(
            "Method info not found for: " + toString(algorithm));
    }
    
    return it->second;
}

bool JoinMethodRegistry::hasMethod(JoinAlgorithm algorithm) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return creators_.find(algorithm) != creators_.end();
}

size_t JoinMethodRegistry::getRegisteredCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return creators_.size();
}

} // namespace sageFlow
```

### 2. 各 Baseline 自注册示例

#### BruteForce Baseline

```cpp
// src/operator/join_operator_methods/bruteforce_join_method.cpp 末尾

#include "operator/join_method_registry.h"

REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::BRUTEFORCE,
    sageFlow::JoinMethodRegistry::MethodInfo{
        .name = "BruteForce",
        .description = "Ground truth baseline with brute-force scan. "
                       "Provides 100% recall rate.",
        .algorithm = sageFlow::JoinAlgorithm::BRUTEFORCE,
        .supports_eager = true,
        .supports_lazy = true,
        .recommended_partition = sageFlow::PartitionStrategy::ROUND_ROBIN,
        .recommended_window_state = sageFlow::WindowStateType::SHARED,
        .paper_reference = ""
    },
    [](const sageFlow::JoinStrategyConfig& config, 
       std::shared_ptr<sageFlow::ConcurrencyManager> cm, 
       int dim) {
        return std::make_unique<sageFlow::BruteForceJoinMethod>(
            config.similarity_threshold, config.window_size_ms);
    }
);
```

#### HNSW Baseline

```cpp
// src/operator/join_operator_methods/hnsw_join_method.cpp 末尾

REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::HNSW,
    sageFlow::JoinMethodRegistry::MethodInfo{
        .name = "HNSW",
        .description = "HNSW-based approximate nearest neighbor join. "
                       "High recall with fast query time.",
        .algorithm = sageFlow::JoinAlgorithm::HNSW,
        .supports_eager = true,
        .supports_lazy = true,
        .recommended_partition = sageFlow::PartitionStrategy::ROUND_ROBIN,
        .recommended_window_state = sageFlow::WindowStateType::SHARED,
        .paper_reference = "Malkov & Yashunin, IEEE TPAMI 2018"
    },
    [](const sageFlow::JoinStrategyConfig& config, 
       std::shared_ptr<sageFlow::ConcurrencyManager> cm, 
       int dim) {
        return std::make_unique<sageFlow::HNSWJoinMethod>(
            config.similarity_threshold, dim,
            config.hnsw_m, config.hnsw_ef_construction, config.hnsw_ef_search);
    }
);
```

#### S3J Baseline

```cpp
// src/operator/join_operator_methods/s3j_method.cpp 末尾

REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::S3J,
    sageFlow::JoinMethodRegistry::MethodInfo{
        .name = "S3J",
        .description = "DEBS'23 Adaptive Distributed Streaming Similarity Joins. "
                       "Uses centroid-based partitioning and adaptive zone grouping.",
        .algorithm = sageFlow::JoinAlgorithm::S3J,
        .supports_eager = true,
        .supports_lazy = true,
        .recommended_partition = sageFlow::PartitionStrategy::CENTROID,
        .recommended_window_state = sageFlow::WindowStateType::PARTITIONED,
        .paper_reference = "Siachamis et al., DEBS 2023, DOI: 10.1145/3583678.3596891"
    },
    [](const sageFlow::JoinStrategyConfig& config, 
       std::shared_ptr<sageFlow::ConcurrencyManager> cm, 
       int dim) {
        return std::make_unique<sageFlow::S3JMethod>(
            config.s3j_num_centroids, config.similarity_threshold,
            config.window_size_ms);
    }
);
```

#### HDR-Tree Baseline

```cpp
// src/operator/join_operator_methods/hdr_tree_join_method.cpp 末尾

REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::HDR_TREE,
    sageFlow::JoinMethodRegistry::MethodInfo{
        .name = "HDR-Tree",
        .description = "HDR-Tree baseline with PCA dimensionality reduction "
                       "and R-tree spatial indexing. Optimized for dynamic updates.",
        .algorithm = sageFlow::JoinAlgorithm::HDR_TREE,
        .supports_eager = true,
        .supports_lazy = true,
        .recommended_partition = sageFlow::PartitionStrategy::ROUND_ROBIN,
        .recommended_window_state = sageFlow::WindowStateType::SHARED,
        .paper_reference = "Ukey et al., ADC 2022, DOI: 10.1007/978-3-031-15512-3_5"
    },
    [](const sageFlow::JoinStrategyConfig& config, 
       std::shared_ptr<sageFlow::ConcurrencyManager> cm, 
       int dim) {
        return std::make_unique<sageFlow::HDRTreeJoinMethod>(
            config.similarity_threshold, dim, config.hdr_projected_dim);
    }
);
```

---

## 修改 JoinStrategyFactory

使用注册中心创建方法：

```cpp
// src/operator/join_strategy_factory.cpp

std::unique_ptr<BaseMethod> JoinStrategyFactory::createJoinMethod(
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int dimension) {
    
    // 使用注册中心创建方法
    return JoinMethodRegistry::instance().createMethod(
        config.algorithm, config, cm, dimension);
}
```

---

## 测试要求

```cpp
TEST(JoinMethodRegistryTest, SingletonInstance) {
    auto& reg1 = JoinMethodRegistry::instance();
    auto& reg2 = JoinMethodRegistry::instance();
    EXPECT_EQ(&reg1, &reg2);
}

TEST(JoinMethodRegistryTest, RegisterAndCreate) {
    auto& registry = JoinMethodRegistry::instance();
    
    // 假设 BruteForce 已自动注册
    EXPECT_TRUE(registry.hasMethod(JoinAlgorithm::BRUTEFORCE));
    
    JoinStrategyConfig config;
    config.similarity_threshold = 0.8;
    config.window_size_ms = 10000;
    
    auto cm = std::make_shared<ConcurrencyManager>();
    auto method = registry.createMethod(
        JoinAlgorithm::BRUTEFORCE, config, cm, 128);
    
    EXPECT_NE(method, nullptr);
}

TEST(JoinMethodRegistryTest, GetAvailableMethods) {
    auto& registry = JoinMethodRegistry::instance();
    auto methods = registry.getAvailableMethods();
    
    // 至少应该有 BruteForce
    EXPECT_GE(methods.size(), 1);
    
    // 验证信息完整
    for (const auto& info : methods) {
        EXPECT_FALSE(info.name.empty());
        EXPECT_FALSE(info.description.empty());
    }
}

TEST(JoinMethodRegistryTest, GetMethodInfo) {
    auto& registry = JoinMethodRegistry::instance();
    
    const auto& info = registry.getMethodInfo(JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(info.name, "BruteForce");
    EXPECT_TRUE(info.supports_eager);
    EXPECT_TRUE(info.supports_lazy);
}

TEST(JoinMethodRegistryTest, UnknownMethod) {
    auto& registry = JoinMethodRegistry::instance();
    
    // 尝试创建未注册的方法应该抛异常
    JoinStrategyConfig config;
    auto cm = std::make_shared<ConcurrencyManager>();
    
    // 假设某个算法未注册
    // EXPECT_THROW(registry.createMethod(...), std::runtime_error);
}

TEST(JoinMethodRegistryTest, MethodInfoValidation) {
    auto& registry = JoinMethodRegistry::instance();
    
    // 验证所有已注册方法的推荐配置是合理的
    auto methods = registry.getAvailableMethods();
    for (const auto& info : methods) {
        JoinStrategyConfig config;
        config.algorithm = info.algorithm;
        config.partition_strategy = info.recommended_partition;
        config.window_state_type = info.recommended_window_state;
        
        auto error = config.validate();
        EXPECT_TRUE(error.empty()) 
            << "Method " << info.name << " has incompatible recommended config";
    }
}
```

---

## 验收标准

1. ✅ 所有 Baseline 方法正确注册
2. ✅ 动态创建功能正常
3. ✅ 方法信息查询正确
4. ✅ 推荐配置与验证规则一致
5. ✅ 所有单元测试通过
6. ✅ 代码符合项目编码规范

---

## 参考资料

- [TASK_GROUP_C_INTEGRATION.md](../TASK_GROUP_C_INTEGRATION.md) - 主任务文档
- [TASK_GROUP_C_BASELINES.md](../TASK_GROUP_C_BASELINES.md) - Baseline 实现任务

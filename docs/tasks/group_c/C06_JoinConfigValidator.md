# C-06: 配置验证与错误处理

**优先级**: 🟡 中  
**预估工时**: 1天  
**依赖**: C-02~C-05  
**状态**: ⬜ 待开始

---

## 任务概述

实现配置验证器，确保用户配置的合法性和一致性。用户可能配置了不兼容的策略组合，需要在启动时检测这些问题并给出明确提示。

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_config_validator.h` | 配置验证器定义 |
| `src/operator/join_config_validator.cpp` | 配置验证器实现 |
| `test/UnitTest/test_join_config_validator.cpp` | 单元测试 |

---

## 验证规则

### 1. 分区-窗口兼容性

| 分区策略 | 允许的窗口状态 | 说明 |
|---------|---------------|------|
| ROUND_ROBIN | SHARED | 轮询分发需要共享状态 |
| KEY_HASH | SHARED, PARTITIONED | 基于 key 分区 |
| VECTOR_HASH | PARTITIONED | 相似向量聚集 |
| LSH | PARTITIONED_VECTOR | VSJoin 专用 |
| CENTROID | PARTITIONED | S3J 专用 |

### 2. 算法-策略兼容性

| 算法 | 必须的配置 |
|-----|-----------|
| VSJOIN | LSH + PARTITIONED_VECTOR + PARTITIONED 索引 |
| S3J | CENTROID + PARTITIONED |
| CLUSTERED_JOIN | CENTROID + PARTITIONED |

### 3. 参数范围检查

| 参数 | 有效范围 |
|-----|---------|
| similarity_threshold | [0.0, 1.0] |
| ivf_nprobes | <= ivf_nlist |
| num_partitions | > 0 |
| hnsw_m | > 0 |
| hnsw_ef_construction | >= hnsw_m |
| hnsw_ef_search | > 0 |
| hdr_projected_dim | > 0, < 原始维度 |

### 4. 依赖检查

| 组件 | 依赖项 |
|-----|-------|
| HDR-Tree | PCA 组件 |
| VSJoin | PartitionCoordinator, AsyncCandidateGenerator, DistanceVerifier |

---

## 接口设计

### JoinConfigValidator

```cpp
#pragma once

#include "operator/join_strategy_config.h"
#include <vector>
#include <string>

namespace sageFlow {

/**
 * @brief 配置验证器
 * 
 * 验证 JoinStrategyConfig 的合法性和一致性
 */
class JoinConfigValidator {
public:
    /**
     * @brief 验证结果
     */
    struct ValidationResult {
        bool valid;                         // 是否有效
        std::vector<std::string> errors;    // 错误信息列表
        std::vector<std::string> warnings;  // 警告信息列表
        
        /**
         * @brief 转换为字符串
         */
        std::string toString() const;
        
        /**
         * @brief 是否有警告
         */
        bool hasWarnings() const { return !warnings.empty(); }
    };
    
    /**
     * @brief 验证配置
     * @param config 策略配置
     * @return 验证结果
     */
    static ValidationResult validate(const JoinStrategyConfig& config);
    
    /**
     * @brief 验证并在无效时抛出异常
     * @param config 策略配置
     * @throws std::runtime_error 如果配置无效
     */
    static void throwIfInvalid(const JoinStrategyConfig& config);
    
    /**
     * @brief 验证配置并打印警告
     * @param config 策略配置
     * @return 是否有效
     */
    static bool validateAndLog(const JoinStrategyConfig& config);

private:
    /**
     * @brief 检查分区-窗口兼容性
     */
    static void checkPartitionWindowCompatibility(
        const JoinStrategyConfig& config, ValidationResult& result);
    
    /**
     * @brief 检查算法-策略兼容性
     */
    static void checkAlgorithmStrategyCompatibility(
        const JoinStrategyConfig& config, ValidationResult& result);
    
    /**
     * @brief 检查参数范围
     */
    static void checkParameterRanges(
        const JoinStrategyConfig& config, ValidationResult& result);
    
    /**
     * @brief 检查组件依赖
     */
    static void checkDependencies(
        const JoinStrategyConfig& config, ValidationResult& result);
    
    /**
     * @brief 检查潜在的性能问题
     */
    static void checkPerformanceHints(
        const JoinStrategyConfig& config, ValidationResult& result);
};

} // namespace sageFlow
```

---

## 实现要点

### JoinConfigValidator 实现

```cpp
#include "operator/join_config_validator.h"
#include "utils/logging.h"
#include <sstream>

namespace sageFlow {

std::string JoinConfigValidator::ValidationResult::toString() const {
    std::ostringstream oss;
    
    if (!valid) {
        oss << "Configuration is INVALID:\n";
        for (const auto& error : errors) {
            oss << "  [ERROR] " << error << "\n";
        }
    } else {
        oss << "Configuration is valid.\n";
    }
    
    if (!warnings.empty()) {
        oss << "Warnings:\n";
        for (const auto& warning : warnings) {
            oss << "  [WARN] " << warning << "\n";
        }
    }
    
    return oss.str();
}

ValidationResult JoinConfigValidator::validate(
    const JoinStrategyConfig& config) {
    
    ValidationResult result;
    result.valid = true;
    
    // 依次执行各项检查
    checkPartitionWindowCompatibility(config, result);
    checkAlgorithmStrategyCompatibility(config, result);
    checkParameterRanges(config, result);
    checkDependencies(config, result);
    checkPerformanceHints(config, result);
    
    return result;
}

void JoinConfigValidator::throwIfInvalid(const JoinStrategyConfig& config) {
    auto result = validate(config);
    if (!result.valid) {
        throw std::runtime_error(result.toString());
    }
}

bool JoinConfigValidator::validateAndLog(const JoinStrategyConfig& config) {
    auto result = validate(config);
    
    if (!result.valid) {
        SAGEFLOW_LOG_ERROR("JoinConfigValidator", "{}", result.toString());
        return false;
    }
    
    if (result.hasWarnings()) {
        for (const auto& warning : result.warnings) {
            SAGEFLOW_LOG_WARN("JoinConfigValidator", "{}", warning);
        }
    }
    
    return true;
}

void JoinConfigValidator::checkPartitionWindowCompatibility(
    const JoinStrategyConfig& config, ValidationResult& result) {
    
    // 规则1: RoundRobin 必须配 SHARED
    if (config.partition_strategy == PartitionStrategy::ROUND_ROBIN &&
        config.window_state_type != WindowStateType::SHARED) {
        result.valid = false;
        result.errors.push_back(
            "RoundRobin partition strategy requires SharedWindowState. "
            "Using PartitionedWindowState with RoundRobin will cause "
            "cross-partition matches to be lost, resulting in reduced recall.");
    }
    
    // 规则2: LSH 需要 PARTITIONED_VECTOR
    if (config.partition_strategy == PartitionStrategy::LSH &&
        config.window_state_type != WindowStateType::PARTITIONED_VECTOR) {
        result.valid = false;
        result.errors.push_back(
            "LSH partition strategy requires PartitionedVectorState.");
    }
    
    // 规则3: CENTROID 需要 PARTITIONED
    if (config.partition_strategy == PartitionStrategy::CENTROID &&
        config.window_state_type == WindowStateType::SHARED) {
        result.valid = false;
        result.errors.push_back(
            "Centroid partition strategy is incompatible with SharedWindowState. "
            "Use PartitionedWindowState instead.");
    }
}

void JoinConfigValidator::checkAlgorithmStrategyCompatibility(
    const JoinStrategyConfig& config, ValidationResult& result) {
    
    // VSJoin 必须配 LSH + PARTITIONED_VECTOR + PARTITIONED 索引
    if (config.algorithm == JoinAlgorithm::VSJOIN) {
        if (config.partition_strategy != PartitionStrategy::LSH) {
            result.valid = false;
            result.errors.push_back(
                "VSJoin algorithm requires LSH partition strategy.");
        }
        if (config.window_state_type != WindowStateType::PARTITIONED_VECTOR) {
            result.valid = false;
            result.errors.push_back(
                "VSJoin algorithm requires PartitionedVectorState.");
        }
        if (config.index_strategy != IndexStrategy::PARTITIONED) {
            result.valid = false;
            result.errors.push_back(
                "VSJoin algorithm requires partitioned index strategy.");
        }
    }
    
    // S3J 必须配 CENTROID
    if (config.algorithm == JoinAlgorithm::S3J) {
        if (config.partition_strategy != PartitionStrategy::CENTROID) {
            result.valid = false;
            result.errors.push_back(
                "S3J algorithm requires Centroid partition strategy.");
        }
        if (config.window_state_type == WindowStateType::SHARED) {
            result.valid = false;
            result.errors.push_back(
                "S3J algorithm is incompatible with SharedWindowState.");
        }
    }
    
    // ClusteredJoin 类似 S3J
    if (config.algorithm == JoinAlgorithm::CLUSTERED_JOIN) {
        if (config.partition_strategy != PartitionStrategy::CENTROID) {
            result.valid = false;
            result.errors.push_back(
                "ClusteredJoin algorithm requires Centroid partition strategy.");
        }
    }
}

void JoinConfigValidator::checkParameterRanges(
    const JoinStrategyConfig& config, ValidationResult& result) {
    
    // similarity_threshold: [0.0, 1.0]
    if (config.similarity_threshold < 0.0 || config.similarity_threshold > 1.0) {
        result.valid = false;
        result.errors.push_back(
            "similarity_threshold must be in range [0.0, 1.0], got: " +
            std::to_string(config.similarity_threshold));
    }
    
    // ivf_nprobes <= ivf_nlist
    if (config.ivf_nprobes > config.ivf_nlist) {
        result.valid = false;
        result.errors.push_back(
            "ivf_nprobes (" + std::to_string(config.ivf_nprobes) + 
            ") cannot exceed ivf_nlist (" + std::to_string(config.ivf_nlist) + ")");
    }
    
    // num_partitions > 0
    if (config.num_partitions <= 0) {
        result.valid = false;
        result.errors.push_back(
            "num_partitions must be positive, got: " +
            std::to_string(config.num_partitions));
    }
    
    // HNSW 参数
    if (config.hnsw_m <= 0) {
        result.valid = false;
        result.errors.push_back(
            "hnsw_m must be positive, got: " + std::to_string(config.hnsw_m));
    }
    
    if (config.hnsw_ef_construction < config.hnsw_m) {
        result.valid = false;
        result.errors.push_back(
            "hnsw_ef_construction should be >= hnsw_m for good recall");
    }
    
    if (config.hnsw_ef_search <= 0) {
        result.valid = false;
        result.errors.push_back(
            "hnsw_ef_search must be positive");
    }
    
    // HDR-Tree 参数
    if (config.hdr_projected_dim <= 0) {
        result.valid = false;
        result.errors.push_back(
            "hdr_projected_dim must be positive");
    }
    
    // window_size_ms > 0
    if (config.window_size_ms <= 0) {
        result.valid = false;
        result.errors.push_back(
            "window_size_ms must be positive");
    }
}

void JoinConfigValidator::checkDependencies(
    const JoinStrategyConfig& config, ValidationResult& result) {
    
    // HDR-Tree 需要 PCA 组件（运行时检查）
    if (config.algorithm == JoinAlgorithm::HDR_TREE) {
        // 这里只能添加警告，实际检查在运行时
        result.warnings.push_back(
            "HDR-Tree requires PCA component to be trained before use. "
            "Make sure to call trainPCA() with sample data.");
    }
    
    // VSJoin 依赖多个组件
    if (config.algorithm == JoinAlgorithm::VSJOIN) {
        result.warnings.push_back(
            "VSJoin requires PartitionCoordinator, AsyncCandidateGenerator, "
            "and DistanceVerifier components. These will be created by "
            "JoinStrategyFactory.");
    }
}

void JoinConfigValidator::checkPerformanceHints(
    const JoinStrategyConfig& config, ValidationResult& result) {
    
    // BruteForce 配 PARTITIONED 会警告
    if (config.algorithm == JoinAlgorithm::BRUTEFORCE &&
        config.window_state_type == WindowStateType::PARTITIONED) {
        result.warnings.push_back(
            "Using BruteForce with PartitionedWindowState may reduce recall "
            "if similar vectors are in different partitions. Consider using "
            "SharedWindowState for 100% recall.");
    }
    
    // 大窗口大小警告
    if (config.window_size_ms > 60000) {  // > 1 minute
        result.warnings.push_back(
            "Large window size (" + std::to_string(config.window_size_ms) + 
            "ms) may cause high memory usage and latency.");
    }
    
    // HNSW ef_search 过小警告
    if (config.algorithm == JoinAlgorithm::HNSW && 
        config.hnsw_ef_search < 50) {
        result.warnings.push_back(
            "Low hnsw_ef_search (" + std::to_string(config.hnsw_ef_search) + 
            ") may result in lower recall. Consider increasing to 50+.");
    }
    
    // IVF nprobes 过小警告
    if (config.algorithm == JoinAlgorithm::IVF && 
        config.ivf_nprobes < 5) {
        result.warnings.push_back(
            "Low ivf_nprobes (" + std::to_string(config.ivf_nprobes) + 
            ") may result in lower recall. Consider increasing to 5+.");
    }
}

} // namespace sageFlow
```

---

## 测试要求

```cpp
TEST(JoinConfigValidatorTest, ValidConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.errors.empty());
}

TEST(JoinConfigValidatorTest, IncompatiblePartitionWindow) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_FALSE(result.valid);
    EXPECT_FALSE(result.errors.empty());
    EXPECT_TRUE(result.errors[0].find("RoundRobin") != std::string::npos);
}

TEST(JoinConfigValidatorTest, VSJoinRequirements) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 错误
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_FALSE(result.valid);
    EXPECT_TRUE(result.errors[0].find("LSH") != std::string::npos);
}

TEST(JoinConfigValidatorTest, S3JRequirements) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::S3J;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;  // 错误
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_FALSE(result.valid);
}

TEST(JoinConfigValidatorTest, InvalidParameterRange) {
    JoinStrategyConfig config;
    config.similarity_threshold = 1.5;  // > 1.0
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_FALSE(result.valid);
}

TEST(JoinConfigValidatorTest, IVFNprobesExceedsNlist) {
    JoinStrategyConfig config;
    config.ivf_nlist = 10;
    config.ivf_nprobes = 20;  // > nlist
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_FALSE(result.valid);
}

TEST(JoinConfigValidatorTest, PerformanceWarnings) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::HNSW;
    config.hnsw_ef_search = 10;  // 较低
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    
    auto result = JoinConfigValidator::validate(config);
    EXPECT_TRUE(result.valid);  // 有效
    EXPECT_TRUE(result.hasWarnings());  // 但有警告
}

TEST(JoinConfigValidatorTest, ThrowIfInvalid) {
    JoinStrategyConfig config;
    config.similarity_threshold = -0.5;  // 无效
    
    EXPECT_THROW(
        JoinConfigValidator::throwIfInvalid(config),
        std::runtime_error
    );
}

TEST(JoinConfigValidatorTest, ToString) {
    JoinStrategyConfig config;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::PARTITIONED;
    
    auto result = JoinConfigValidator::validate(config);
    auto str = result.toString();
    
    EXPECT_FALSE(str.empty());
    EXPECT_TRUE(str.find("INVALID") != std::string::npos);
    EXPECT_TRUE(str.find("ERROR") != std::string::npos);
}
```

---

## 验收标准

1. ✅ 检测所有不兼容配置
2. ✅ 错误信息清晰可操作
3. ✅ 警告信息提示潜在问题
4. ✅ 所有单元测试通过
5. ✅ 代码符合项目编码规范
6. ✅ 与 JoinStrategyFactory 集成

---

## 参考资料

- [TASK_GROUP_C_INTEGRATION.md](../TASK_GROUP_C_INTEGRATION.md) - 主任务文档
- [C02_JoinStrategyFactory.md](./C02_JoinStrategyFactory.md) - 策略工厂

# C-04: 窗口状态自适应选择

**优先级**: 🔴 高  
**预估工时**: 1天  
**依赖**: C-02 (JoinStrategyFactory)  
**状态**: ⬜ 待开始

---

## 任务概述

根据 JoinStrategyConfig 动态选择窗口状态类型。不同的 Join 方法需要不同的窗口状态：

| 窗口状态类型 | 描述 | 适用场景 |
|-------------|------|---------|
| SharedWindowState | 共享状态，所有实例共享 | RoundRobin 分区 |
| PartitionedWindowState | 分区状态，每个 subtask 独立 | 内容分区（Key/Vector） |
| TwoTierWindowState | 双层结构（写友好层+紧凑层） | 高吞吐写入场景 |
| PartitionedVectorState | 向量空间分区状态 | VSJoin 专用 |

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/state/window_state_factory.h` | 窗口状态工厂定义 |
| `src/state/window_state_factory.cpp` | 窗口状态工厂实现 |
| `test/UnitTest/test_window_state_factory.cpp` | 单元测试 |

---

## 接口设计

### WindowStateFactory

```cpp
#pragma once

#include "state/window_state.h"
#include "operator/join_strategy_config.h"
#include <memory>

namespace sageFlow {

// 前向声明
class VectorSpacePartitioner;

/**
 * @brief 窗口状态工厂
 * 
 * 根据配置创建适当的窗口状态实例
 */
class WindowStateFactory {
public:
    /**
     * @brief 创建窗口状态
     * @param type 窗口状态类型
     * @param parallelism 并行度
     * @param config 完整配置（用于获取特定参数）
     * @param partitioner 向量分区器（仅 PARTITIONED_VECTOR 需要）
     * @return 窗口状态实例
     */
    static std::unique_ptr<WindowState> create(
        WindowStateType type,
        size_t parallelism,
        const JoinStrategyConfig& config,
        std::shared_ptr<VectorSpacePartitioner> partitioner = nullptr);
    
    /**
     * @brief 根据配置自动推断并创建窗口状态
     */
    static std::unique_ptr<WindowState> createFromConfig(
        const JoinStrategyConfig& config,
        size_t parallelism,
        std::shared_ptr<VectorSpacePartitioner> partitioner = nullptr);
};

} // namespace sageFlow
```

---

## 实现要点

### WindowStateFactory 实现

```cpp
#include "state/window_state_factory.h"
#include "state/shared_window_state.h"
#include "state/partitioned_window_state.h"
#include "state/two_tier_window_state.h"
#include "state/partitioned_vector_state.h"
#include <stdexcept>

namespace sageFlow {

std::unique_ptr<WindowState> WindowStateFactory::create(
    WindowStateType type,
    size_t parallelism,
    const JoinStrategyConfig& config,
    std::shared_ptr<VectorSpacePartitioner> partitioner) {
    
    switch (type) {
        case WindowStateType::SHARED:
            return std::make_unique<SharedWindowState>();
            
        case WindowStateType::PARTITIONED:
            return std::make_unique<PartitionedWindowState>(parallelism);
            
        case WindowStateType::TWO_TIER:
            // TwoTierWindowState 需要额外的配置参数
            // 如果尚未实现，可以先返回 PartitionedWindowState
            // 或抛出未实现异常
            return std::make_unique<TwoTierWindowState>(
                parallelism,
                config.window_size_ms,
                1000  // compact_threshold
            );
            
        case WindowStateType::PARTITIONED_VECTOR:
            if (!partitioner) {
                throw std::runtime_error(
                    "PartitionedVectorState requires a VectorSpacePartitioner");
            }
            return std::make_unique<PartitionedVectorState>(
                partitioner,
                config.num_partitions
            );
            
        default:
            throw std::runtime_error("Unknown window state type: " + 
                                     toString(type));
    }
}

std::unique_ptr<WindowState> WindowStateFactory::createFromConfig(
    const JoinStrategyConfig& config,
    size_t parallelism,
    std::shared_ptr<VectorSpacePartitioner> partitioner) {
    
    return create(config.window_state_type, parallelism, config, partitioner);
}

} // namespace sageFlow
```

---

## 修改 JoinOperator

需要修改 `JoinOperator::open()` 方法，使用工厂创建窗口状态：

```cpp
// src/operator/join_operator.cpp

void JoinOperator::open(const RuntimeContext& context) {
    // ... 现有代码 ...
    
    // 使用工厂创建窗口状态
    if (strategy_config_.has_value()) {
        left_state_ = WindowStateFactory::createFromConfig(
            *strategy_config_, 
            context.getParallelism(),
            vector_partitioner_);
        right_state_ = WindowStateFactory::createFromConfig(
            *strategy_config_, 
            context.getParallelism(),
            vector_partitioner_);
    } else {
        // 默认使用共享状态
        left_state_ = std::make_unique<SharedWindowState>();
        right_state_ = std::make_unique<SharedWindowState>();
    }
    
    // ... 其他初始化代码 ...
}
```

---

## 测试要求

```cpp
TEST(WindowStateFactoryTest, CreateSharedState) {
    JoinStrategyConfig config;
    auto state = WindowStateFactory::create(
        WindowStateType::SHARED, 4, config);
    
    EXPECT_NE(state, nullptr);
    EXPECT_TRUE(state->isShared());
}

TEST(WindowStateFactoryTest, CreatePartitionedState) {
    JoinStrategyConfig config;
    auto state = WindowStateFactory::create(
        WindowStateType::PARTITIONED, 4, config);
    
    EXPECT_NE(state, nullptr);
    EXPECT_FALSE(state->isShared());
}

TEST(WindowStateFactoryTest, CreatePartitionedVectorStateWithoutPartitioner) {
    JoinStrategyConfig config;
    
    // 没有提供 partitioner 应该抛异常
    EXPECT_THROW(
        WindowStateFactory::create(
            WindowStateType::PARTITIONED_VECTOR, 4, config, nullptr),
        std::runtime_error
    );
}

TEST(WindowStateFactoryTest, CreatePartitionedVectorStateWithPartitioner) {
    JoinStrategyConfig config;
    config.num_partitions = 4;
    
    auto partitioner = std::make_shared<LSHVectorSpacePartitioner>(
        128, 8, 4);
    
    auto state = WindowStateFactory::create(
        WindowStateType::PARTITIONED_VECTOR, 4, config, partitioner);
    
    EXPECT_NE(state, nullptr);
}

TEST(WindowStateFactoryTest, CreateFromConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.inferDefaults();  // 应该设置为 SHARED
    
    auto state = WindowStateFactory::createFromConfig(config, 4);
    EXPECT_TRUE(state->isShared());
}

TEST(WindowStateFactoryTest, CreateFromVSJoinConfig) {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::VSJOIN;
    config.inferDefaults();  // 应该设置为 PARTITIONED_VECTOR
    config.num_partitions = 4;
    
    auto partitioner = std::make_shared<LSHVectorSpacePartitioner>(
        128, 8, 4);
    
    auto state = WindowStateFactory::createFromConfig(config, 4, partitioner);
    EXPECT_FALSE(state->isShared());
}
```

---

## 验收标准

1. ✅ 所有窗口状态类型正确创建
2. ✅ 配置参数正确传递
3. ✅ 与现有 JoinOperator 兼容
4. ✅ 错误情况有明确的异常信息
5. ✅ 所有单元测试通过
6. ✅ 代码符合项目编码规范

---

## 参考资料

- [TASK_GROUP_C_INTEGRATION.md](../TASK_GROUP_C_INTEGRATION.md) - 主任务文档
- [WindowState 接口定义](../../../include/state/window_state.h)

# 添加新的 Join 方法指南

本文档详细说明如何在 SageFlow 中添加新的 Join 算法。新增一个 Join 方法需要修改多个文件，请按照本指南逐步完成。

## 概述

添加新的 Join 方法需要完成以下步骤：

1. **定义枚举值** - 在配置枚举中添加新算法
2. **实现算法类** - 继承 `BaseMethod` 实现核心逻辑
3. **配置验证** - 添加参数验证规则
4. **工厂集成** - 注册到 `JoinStrategyFactory`
5. **测试配置** - 添加 TOML 配置和测试用例
6. **文档更新** - 更新相关文档

---

## 步骤 1：定义枚举值

### 1.1 添加 JoinAlgorithm 枚举

**文件**: `include/operator/join_strategy_config.h`

```cpp
enum class JoinAlgorithm {
    BRUTEFORCE,
    IVF,
    HNSW,
    HDR_TREE,
    CLUSTERED_JOIN,
    S3J,
    VSJOIN,
    MY_NEW_JOIN    // <-- 添加新算法
};
```

### 1.2 添加 JoinMethodType 枚举

**文件**: `include/operator/join_operator_methods/base_method.h`

```cpp
enum class JoinMethodType {
    BRUTEFORCE,
    IVF,
    HNSW,
    HDR_TREE,
    CLUSTERED_JOIN,
    S3J,
    MY_NEW_JOIN    // <-- 添加新算法
};
```

### 1.3 添加字符串转换函数

**文件**: `src/operator/join_strategy_config.cpp`

```cpp
std::string toString(JoinAlgorithm algo) {
    switch (algo) {
        // ... 现有 case
        case JoinAlgorithm::MY_NEW_JOIN: return "my_new_join";
        default: return "unknown";
    }
}

JoinAlgorithm parseJoinAlgorithm(const std::string& s) {
    // ... 现有映射
    if (s == "my_new_join" || s == "MY_NEW_JOIN") return JoinAlgorithm::MY_NEW_JOIN;
    // ...
}
```

---

## 步骤 2：实现算法类

### 2.1 创建头文件

**文件**: `include/operator/join_operator_methods/my_new_join_method.h`

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include <memory>

namespace sageFlow {

/**
 * @brief MyNewJoin 方法实现
 * 
 * 描述你的算法特点、适用场景和复杂度分析。
 */
class MyNewJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param similarity_threshold 相似度阈值
     * @param my_param1 算法特有参数1
     * @param my_param2 算法特有参数2
     */
    MyNewJoinMethod(double similarity_threshold, 
                    int my_param1, 
                    double my_param2);
    
    ~MyNewJoinMethod() override = default;
    
    // ==================== 核心接口 ====================
    
    /**
     * @brief 执行 Eager 模式的 Join 查询
     * 
     * @param query_record 查询记录
     * @param query_slot 查询来源 slot（0=左流，1=右流）
     * @return 满足阈值的候选向量列表
     */
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot) override;
    
    // ==================== 状态管理 ====================
    
    /**
     * @brief 设置窗口状态（必须）
     * @param left_state 左侧窗口状态
     * @param right_state 右侧窗口状态
     */
    void setWindowStates(std::shared_ptr<WindowState> left_state,
                         std::shared_ptr<WindowState> right_state);
    
    /**
     * @brief 设置索引管理器（如果需要索引）
     * @param manager 并发管理器
     * @param index_id 索引 ID
     */
    void setIndexManager(std::shared_ptr<ConcurrencyManager> manager,
                         int index_id);
    
    // ==================== 生命周期钩子 ====================
    
    /**
     * @brief 初始化方法（在流水线启动时调用）
     */
    void initialize();
    
    /**
     * @brief 处理新记录添加到窗口
     * @param record 新记录
     * @param slot 来源 slot
     */
    void onRecordAdded(const VectorRecord& record, int slot);
    
    /**
     * @brief 处理窗口驱逐
     * @param evicted_uids 被驱逐的记录 UID 列表
     */
    void onWindowEviction(const std::vector<uint64_t>& evicted_uids);
    
private:
    // 算法特有参数
    int my_param1_;
    double my_param2_;
    
    // 状态引用
    std::shared_ptr<WindowState> left_state_;
    std::shared_ptr<WindowState> right_state_;
    
    // 索引相关（如果需要）
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    int index_id_ = -1;
    
    // 内部数据结构
    // ... 根据算法需要添加
};

}  // namespace sageFlow
```

### 2.2 创建实现文件

**文件**: `src/operator/join_operator_methods/my_new_join_method.cpp`

```cpp
#include "operator/join_operator_methods/my_new_join_method.h"
#include "utils/logging.h"
#include "compute_engine/distance_function.h"

namespace sageFlow {

MyNewJoinMethod::MyNewJoinMethod(double similarity_threshold, 
                                  int my_param1, 
                                  double my_param2)
    : BaseMethod(similarity_threshold)
    , my_param1_(my_param1)
    , my_param2_(my_param2) {
    SAGEFLOW_LOG_INFO("MyNewJoin", "Created with threshold={}, param1={}, param2={}",
                      similarity_threshold, my_param1, my_param2);
}

void MyNewJoinMethod::setWindowStates(std::shared_ptr<WindowState> left_state,
                                       std::shared_ptr<WindowState> right_state) {
    left_state_ = std::move(left_state);
    right_state_ = std::move(right_state);
}

void MyNewJoinMethod::setIndexManager(std::shared_ptr<ConcurrencyManager> manager,
                                       int index_id) {
    concurrency_manager_ = std::move(manager);
    index_id_ = index_id;
}

void MyNewJoinMethod::initialize() {
    // 初始化算法所需的数据结构
    SAGEFLOW_LOG_DEBUG("MyNewJoin", "Initializing...");
}

std::vector<std::unique_ptr<VectorRecord>> MyNewJoinMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 获取对侧窗口状态
    auto& target_state = (query_slot == 0) ? right_state_ : left_state_;
    
    // 获取窗口中的记录（根据状态类型选择合适的方法）
    const auto& records = target_state->getRecords(0);  // subtask_index
    
    // 实现你的算法逻辑
    for (const auto& record : records) {
        // 计算相似度
        double similarity = Cosine(query_record.vector, record->vector);
        
        if (similarity >= join_similarity_threshold_) {
            // 创建结果记录
            auto result = std::make_unique<VectorRecord>(*record);
            results.push_back(std::move(result));
        }
    }
    
    SAGEFLOW_LOG_DEBUG("MyNewJoin", "Query found {} candidates", results.size());
    return results;
}

void MyNewJoinMethod::onRecordAdded(const VectorRecord& record, int slot) {
    // 可选：维护增量索引或统计信息
}

void MyNewJoinMethod::onWindowEviction(const std::vector<uint64_t>& evicted_uids) {
    // 可选：清理过期数据
}

}  // namespace sageFlow
```

### 2.3 更新 CMakeLists.txt

**文件**: `src/operator/CMakeLists.txt`

```cmake
set(OPERATOR_SOURCES
    # ... 现有文件
    join_operator_methods/my_new_join_method.cpp
)
```

---

## 步骤 3：配置验证

### 3.1 添加配置参数

**文件**: `include/operator/join_strategy_config.h`

在 `JoinStrategyConfig` 结构体中添加：

```cpp
struct JoinStrategyConfig {
    // ... 现有参数
    
    // ==================== MyNewJoin 参数 ====================
    int my_new_param1 = 100;        ///< 参数1说明
    double my_new_param2 = 0.5;     ///< 参数2说明
    bool my_new_enable_xxx = true;  ///< 是否启用xxx特性
};
```

### 3.2 添加验证逻辑

**文件**: `src/operator/join_config_validator.cpp`

```cpp
std::vector<std::string> JoinConfigValidator::validateAlgorithmSpecificParams(
    const JoinStrategyConfig& config) {
    std::vector<std::string> errors;
    
    // ... 现有验证
    
    // MyNewJoin 参数验证
    if (config.algorithm == JoinAlgorithm::MY_NEW_JOIN) {
        if (config.my_new_param1 <= 0) {
            errors.push_back("my_new_param1 must be positive");
        }
        if (config.my_new_param2 < 0.0 || config.my_new_param2 > 1.0) {
            errors.push_back("my_new_param2 must be in [0.0, 1.0]");
        }
    }
    
    return errors;
}

// 更新推荐策略
void JoinStrategyConfig::inferDefaults() {
    switch (algorithm) {
        // ... 现有 case
        
        case JoinAlgorithm::MY_NEW_JOIN:
            // 设置推荐的分区策略和窗口状态类型
            partition_strategy = PartitionStrategy::VECTOR_HASH;  // 或其他合适策略
            window_state_type = WindowStateType::PARTITIONED;
            index_strategy = IndexStrategy::PARTITIONED;
            break;
    }
}
```

### 3.3 添加 TOML 解析

**文件**: `src/operator/join_strategy_config.cpp`

在 `loadJoinStrategyConfig` 函数中添加：

```cpp
// MyNewJoin 参数解析
if (auto my_new = root["my_new_join"].as_table()) {
    if (auto v = (*my_new)["param1"].value<int>()) config.my_new_param1 = *v;
    if (auto v = (*my_new)["param2"].value<double>()) config.my_new_param2 = *v;
    if (auto v = (*my_new)["enable_xxx"].value<bool>()) config.my_new_enable_xxx = *v;
}
```

---

## 步骤 4：工厂集成

### 4.1 更新 JoinStrategyFactory

**文件**: `src/operator/join_strategy_factory.cpp`

```cpp
std::unique_ptr<BaseMethod> JoinStrategyFactory::createMethod(
    const JoinStrategyConfig& config) {
    
    switch (config.algorithm) {
        // ... 现有 case
        
        case JoinAlgorithm::MY_NEW_JOIN:
            return std::make_unique<MyNewJoinMethod>(
                config.similarity_threshold,
                config.my_new_param1,
                config.my_new_param2
            );
        
        default:
            throw std::runtime_error("Unknown join algorithm");
    }
}
```

### 4.2 更新 JoinOperator（如需特殊处理）

**文件**: `src/operator/join_operator.cpp`

如果新方法需要特殊的初始化或生命周期管理，在 `initializeWithStrategyConfig()` 中添加：

```cpp
void JoinOperator::initializeWithStrategyConfig() {
    // ... 现有逻辑
    
    // MyNewJoin 特殊处理
    if (strategy_config_.algorithm == JoinAlgorithm::MY_NEW_JOIN) {
        // 设置特殊的索引类型或状态管理
        auto* my_method = dynamic_cast<MyNewJoinMethod*>(join_method_.get());
        if (my_method) {
            my_method->setWindowStates(left_window_state_, right_window_state_);
            // 其他初始化...
        }
    }
}
```

---

## 步骤 5：测试配置

### 5.1 添加策略配置

**文件**: `config/join_strategies.toml`

```toml
[strategies.my_new_join_baseline]
name = "my_new_join_baseline"
join_algorithm = "my_new_join"
partition_strategy = "VectorHash"
window_state_type = "Partitioned"
similarity_threshold = 0.75
window_time_ms = 60000

[strategies.my_new_join_baseline.my_new_join]
param1 = 100
param2 = 0.5
enable_xxx = true
```

### 5.2 添加集成测试用例

**文件**: `config/integration_test_cases.toml`

```toml
[test_cases.my_new_join_small]
name = "my_new_join_small"
description = "MyNewJoin with small dataset"
enabled = true

[test_cases.my_new_join_small.strategy]
join_algorithm = "my_new_join"
partition_strategy = "VectorHash"
window_state_type = "Partitioned"
similarity_threshold = 0.75
window_time_ms = 60000

[test_cases.my_new_join_small.strategy.my_new_join]
param1 = 100
param2 = 0.5

[test_cases.my_new_join_small.data_generation]
positive_pairs = 500
near_threshold_pairs = 50
negative_pairs = 500
random_tail = 2000
seed = 42

[test_cases.my_new_join_small.validation]
expected_min_recall = 0.80
expected_min_precision = 0.75
compare_with_ground_truth = true
```

### 5.3 编写单元测试

**文件**: `test/UnitTest/test_my_new_join_method.cpp`

```cpp
#include <gtest/gtest.h>
#include "operator/join_operator_methods/my_new_join_method.h"
#include "common/vector_record.h"

using namespace sageFlow;

class MyNewJoinMethodTest : public ::testing::Test {
protected:
    void SetUp() override {
        method_ = std::make_unique<MyNewJoinMethod>(0.75, 100, 0.5);
    }
    
    std::unique_ptr<MyNewJoinMethod> method_;
};

TEST_F(MyNewJoinMethodTest, Construction) {
    EXPECT_NE(method_, nullptr);
}

TEST_F(MyNewJoinMethodTest, ExecuteEager_EmptyState) {
    VectorRecord query;
    query.vector = {1.0f, 0.0f, 0.0f};
    
    // 设置空窗口状态
    auto left_state = std::make_shared<SharedWindowState>();
    auto right_state = std::make_shared<SharedWindowState>();
    method_->setWindowStates(left_state, right_state);
    
    auto results = method_->ExecuteEager(query, 0);
    EXPECT_TRUE(results.empty());
}

TEST_F(MyNewJoinMethodTest, ExecuteEager_FindsMatches) {
    // 创建窗口状态并添加测试数据
    auto left_state = std::make_shared<SharedWindowState>();
    auto right_state = std::make_shared<SharedWindowState>();
    
    // 添加测试向量到 right_state...
    
    method_->setWindowStates(left_state, right_state);
    
    VectorRecord query;
    query.vector = {1.0f, 0.0f, 0.0f};
    
    auto results = method_->ExecuteEager(query, 0);
    
    // 验证结果...
}
```

### 5.4 更新 CMakeLists.txt

**文件**: `test/UnitTest/CMakeLists.txt`

```cmake
add_executable(test_my_new_join_method test_my_new_join_method.cpp)
target_link_libraries(test_my_new_join_method gtest_main sageflow_lib)
gtest_discover_tests(test_my_new_join_method PROPERTIES LABELS "UNIT")
```

---

## 步骤 6：文档更新

### 6.1 更新 copilot-instructions.md

**文件**: `.github/copilot-instructions.md`

在 Join 策略兼容性表中添加新算法：

```markdown
| 分区策略 | 兼容的窗口状态 | 说明 |
|---------|---------------|------|
| ... | ... | ... |
| VectorHash | Partitioned | MyNewJoin 推荐配置 |
```

### 6.2 更新 JOIN_PIPELINE_GUIDE.md

**文件**: `docs/JOIN_PIPELINE_GUIDE.md`

添加新算法的描述：

```markdown
### MyNewJoin

**适用场景**：描述算法最适合的使用场景

**复杂度**：
- 构建：O(...)
- 查询：O(...)

**参数配置**：
| 参数 | 默认值 | 说明 |
|------|--------|------|
| param1 | 100 | ... |
| param2 | 0.5 | ... |

**推荐配置**：
- 分区策略：VectorHash
- 窗口状态：Partitioned
- 索引策略：Partitioned
```

---

## 核心检查清单

### 必须修改的文件

- [ ] `include/operator/join_strategy_config.h` - 添加枚举和参数
- [ ] `include/operator/join_operator_methods/base_method.h` - 添加方法类型枚举
- [ ] `include/operator/join_operator_methods/my_new_join_method.h` - 新建算法头文件
- [ ] `src/operator/join_operator_methods/my_new_join_method.cpp` - 实现算法
- [ ] `src/operator/join_strategy_config.cpp` - 添加字符串转换和 TOML 解析
- [ ] `src/operator/join_config_validator.cpp` - 添加参数验证
- [ ] `src/operator/join_strategy_factory.cpp` - 注册到工厂
- [ ] `src/operator/CMakeLists.txt` - 添加源文件

### 必须新增的文件

- [ ] `include/operator/join_operator_methods/my_new_join_method.h`
- [ ] `src/operator/join_operator_methods/my_new_join_method.cpp`
- [ ] `test/UnitTest/test_my_new_join_method.cpp`

### 必须更新的配置

- [ ] `config/join_strategies.toml` - 添加策略配置
- [ ] `config/integration_test_cases.toml` - 添加测试用例

### 必须更新的文档

- [ ] `.github/copilot-instructions.md`
- [ ] `docs/JOIN_PIPELINE_GUIDE.md`
- [ ] `docs/TEST_TOOLS_GUIDE.md`（如果影响测试工具）

---

## 验证步骤

1. **编译测试**
   ```bash
   cmake --build build -j$(nproc)
   ```

2. **运行单元测试**
   ```bash
   ./build/bin/test_my_new_join_method
   ```

3. **运行配置加载测试**
   ```bash
   ./build/bin/test_join_config_loader
   ```

4. **运行集成测试**
   ```bash
   ./build/bin/test_join_baseline_integration --gtest_filter="*my_new_join*"
   ```

5. **运行多并行度测试**（关键！）
   ```bash
   ./build/bin/test_join_datasource_modes
   ```
   这个测试会验证新算法在不同并行度下的正确性。

---

## 常见问题

### Q1: 我的算法需要特殊的索引结构怎么办？

**A**: 可以在 `include/index/` 下创建新的索引类型，继承 `Index` 基类，然后通过 `ConcurrencyManager::register_index()` 注册。参考 `PartitionedIndex` 的实现。

### Q2: 如何支持异步候选生成？

**A**: 参考 `AsyncCandidateGenerator` 的实现，在你的方法中集成异步逻辑。

### Q3: 测试时召回率很低怎么调试？

**A**: 
1. 检查 `partition_strategy` 和 `window_state_type` 的兼容性
2. 确保窗口大小足够包含所有相关记录
3. 使用 `JoinMetricsCollector` 收集详细指标
4. 降低并行度到 1 进行单线程调试

### Q4: 如何让我的算法支持 Lazy 模式？

**A**: Lazy 模式已被弃用，所有新算法都应该使用 Eager 模式。如果确实需要批量处理，可以在 `ExecuteEager` 中实现内部批处理逻辑。

---

## 参考实现

- **简单实现**: `bruteforce_baseline.h/cpp` - 最简单的 baseline
- **索引实现**: `ivf_method.h/cpp` - 使用 IVF 索引
- **复杂实现**: `s3j_method.h/cpp` - 包含自适应分区等高级特性
- **分区实现**: `clustered_join_method.h/cpp` - 基于聚类的分区方法

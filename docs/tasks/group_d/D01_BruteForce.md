# D-01: BruteForce Ground Truth

**优先级**: 🔴 高  
**预估工时**: 1天  
**依赖**: 无  
**状态**: ⬜ 待开始

---

## 任务概述

实现 BruteForce 精确匹配方法作为 Ground Truth 基准。该实现用于验证其他近似算法的正确性，以及在小规模测试中作为参考答案。

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_operator_methods/bruteforce_baseline.h` | BruteForce 方法定义 |
| `src/operator/join_operator_methods/bruteforce_baseline.cpp` | BruteForce 方法实现 |
| `test/UnitTest/test_bruteforce_baseline.cpp` | 单元测试 |

---

## 算法描述

### 核心思想

对于每个查询向量，遍历窗口内所有记录，计算相似度并返回满足阈值的结果。

### 复杂度分析

- **时间复杂度**: $O(N \cdot M \cdot D)$
  - N: 查询数量
  - M: 窗口内记录数量
  - D: 向量维度
- **空间复杂度**: $O(M \cdot D)$ (仅存储窗口内记录)

---

## 推荐配置

```toml
[baseline.bruteforce]
algorithm = "bruteforce"
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "none"  # 无需索引
similarity_threshold = 0.8
```

### 配置说明

| 参数 | 值 | 说明 |
|-----|---|------|
| partition_strategy | round_robin | 负载均衡分发 |
| window_state_type | shared | 共享状态保证 100% 召回 |
| index_strategy | none | 精确匹配无需索引 |

---

## 接口设计

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include <memory>
#include <vector>
#include <deque>

namespace sageFlow {

/**
 * @brief BruteForce Ground Truth 方法
 * 
 * 精确匹配实现，用于：
 * 1. 作为 Ground Truth 验证其他方法
 * 2. 小规模数据的精确 Join
 */
class BruteForceBaseline : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值
     */
    explicit BruteForceBaseline(double threshold);
    
    ~BruteForceBaseline() override = default;
    
    /**
     * @brief 获取方法名称
     */
    std::string getName() const override { return "BruteForce"; }
    
    /**
     * @brief 获取推荐配置
     */
    JoinStrategyConfig getRecommendedConfig() const override;
    
    /**
     * @brief 初始化
     */
    void open(const RuntimeContext& context, 
              JoinOperatorState* state) override;
    
    /**
     * @brief Eager 模式：对单个查询执行匹配
     * @param query 查询向量
     * @param slot 输入槽位
     * @return 匹配结果列表
     */
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    /**
     * @brief Lazy 模式：批量查询执行匹配
     * @param queries 查询向量队列
     * @param slot 输入槽位
     * @return 匹配结果列表
     */
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries,
        int slot) override;
    
    /**
     * @brief 关闭
     */
    void close() override;
    
    /**
     * @brief 获取阈值
     */
    double getThreshold() const { return threshold_; }
    
    /**
     * @brief 设置阈值
     */
    void setThreshold(double threshold) { threshold_ = threshold; }

private:
    double threshold_;
    JoinOperatorState* state_ = nullptr;
    size_t subtask_index_ = 0;
    
    /**
     * @brief 计算余弦相似度
     */
    double computeCosineSimilarity(
        const std::vector<float>& a, 
        const std::vector<float>& b) const;
    
    /**
     * @brief 在给定记录集中搜索匹配
     */
    std::vector<std::unique_ptr<VectorRecord>> searchInRecords(
        const VectorRecord& query,
        const std::deque<std::unique_ptr<VectorRecord>>& records) const;
};

// 自动注册
REGISTER_JOIN_METHOD(BruteForceBaseline, "bruteforce");

} // namespace sageFlow
```

---

## 实现要点

```cpp
#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "compute_engine/cosine_similarity.h"
#include <algorithm>
#include <cmath>

namespace sageFlow {

BruteForceBaseline::BruteForceBaseline(double threshold)
    : threshold_(threshold) {}

JoinStrategyConfig BruteForceBaseline::getRecommendedConfig() const {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::BRUTEFORCE;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.index_strategy = IndexStrategy::NONE;
    config.similarity_threshold = threshold_;
    return config;
}

void BruteForceBaseline::open(
    const RuntimeContext& context, 
    JoinOperatorState* state) {
    state_ = state;
    subtask_index_ = context.getSubtaskIndex();
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::ExecuteEager(
    const VectorRecord& query, int slot) {
    
    if (!state_) {
        return {};
    }
    
    // 获取对侧窗口的记录
    int other_slot = (slot == 0) ? 1 : 0;
    const auto& records = state_->getWindowRecords(other_slot);
    
    return searchInRecords(query, records);
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::ExecuteLazy(
    const std::deque<std::unique_ptr<VectorRecord>>& queries,
    int slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> all_results;
    
    for (const auto& query : queries) {
        auto matches = ExecuteEager(*query, slot);
        for (auto& match : matches) {
            all_results.push_back(std::move(match));
        }
    }
    
    return all_results;
}

void BruteForceBaseline::close() {
    state_ = nullptr;
}

double BruteForceBaseline::computeCosineSimilarity(
    const std::vector<float>& a, 
    const std::vector<float>& b) const {
    
    if (a.size() != b.size()) {
        return 0.0;
    }
    
    double dot = 0.0;
    double norm_a = 0.0;
    double norm_b = 0.0;
    
    for (size_t i = 0; i < a.size(); ++i) {
        dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        norm_a += static_cast<double>(a[i]) * static_cast<double>(a[i]);
        norm_b += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }
    
    double denom = std::sqrt(norm_a) * std::sqrt(norm_b);
    if (denom < 1e-10) {
        return 0.0;
    }
    
    return dot / denom;
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceBaseline::searchInRecords(
    const VectorRecord& query,
    const std::deque<std::unique_ptr<VectorRecord>>& records) const {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    for (const auto& record : records) {
        double similarity = computeCosineSimilarity(
            query.getVector(), record->getVector());
        
        if (similarity >= threshold_) {
            // 创建结果副本
            results.push_back(std::make_unique<VectorRecord>(*record));
        }
    }
    
    return results;
}

} // namespace sageFlow
```

---

## 测试要求

```cpp
TEST(BruteForceBaselineTest, BasicMatching) {
    BruteForceBaseline method(0.9);
    
    // 创建测试数据
    std::vector<float> vec1(128, 1.0f);
    std::vector<float> vec2(128, 1.0f);  // 相同向量
    
    auto query = std::make_unique<VectorRecord>(1, vec1, 0);
    auto record = std::make_unique<VectorRecord>(2, vec2, 0);
    
    // 相同向量相似度应为 1.0
    // 验证匹配逻辑正确
}

TEST(BruteForceBaselineTest, ThresholdFiltering) {
    BruteForceBaseline method(0.8);
    
    // 测试阈值过滤
    // 低于阈值的不应返回
}

TEST(BruteForceBaselineTest, EmptyWindow) {
    BruteForceBaseline method(0.8);
    
    // 空窗口应返回空结果
}

TEST(BruteForceBaselineTest, RecommendedConfig) {
    BruteForceBaseline method(0.8);
    auto config = method.getRecommendedConfig();
    
    EXPECT_EQ(config.algorithm, JoinAlgorithm::BRUTEFORCE);
    EXPECT_EQ(config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(config.window_state_type, WindowStateType::SHARED);
    EXPECT_EQ(config.index_strategy, IndexStrategy::NONE);
}

TEST(BruteForceBaselineTest, Registration) {
    auto registry = JoinMethodRegistry::getInstance();
    EXPECT_TRUE(registry->hasMethod("bruteforce"));
}
```

---

## 验收标准

1. ✅ 100% 召回率（Ground Truth）
2. ✅ 通过所有单元测试
3. ✅ 作为其他方法的验证基准
4. ✅ 代码符合项目编码规范
5. ✅ 正确实现自动注册

---

## 使用场景

1. **Ground Truth 验证**: 验证其他近似算法的召回率
2. **小规模测试**: 窗口大小 < 1000 时的精确匹配
3. **调试基准**: 排查其他方法问题时的对照组

---

## 参考资料

- [GROUP_D_README.md](./README.md) - Group D 总览
- [TASK_GROUP_C_BASELINES.md](../TASK_GROUP_C_BASELINES.md) - Baseline 主任务

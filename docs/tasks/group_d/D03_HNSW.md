# D-03: HNSW Enhanced Baseline

**优先级**: 🔴 高  
**预估工时**: 2天  
**依赖**: D-01 (Ground Truth)  
**状态**: ✅ 完成

---

## 任务概述

基于现有 HNSW 索引实现增强版 Join 方法，利用 Hierarchical Navigable Small World 图结构进行高效的近似最近邻搜索，后续考虑接入faiss库或其他开源库的高性能HNSW，可留出相关接口待后续实现。

---

## 论文依据

### 主要论文

| 标题 | 期刊/来源 | 年份 | DOI/链接 |
|-----|----------|-----|----------|
| Efficient and Robust Approximate Nearest Neighbor Search Using Hierarchical Navigable Small World Graphs | IEEE TPAMI | 2018 | 10.1109/TPAMI.2018.2889473 |
| Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs | arXiv | 2016 | arXiv:1603.09320 |

### 核心作者

- Yu. A. Malkov
- D. A. Yashunin

### 参考实现

- **hnswlib**: https://github.com/nmslib/hnswlib

### 算法要点

1. **层次化结构**: 多层图结构，上层稀疏用于快速跳转，下层稠密用于精确搜索
2. **Navigable Small World**: 每层维护 small world 属性，保证 $O(\log N)$ 搜索复杂度
3. **增量插入**: 支持高效增量插入，不需要完全重建
4. **可调参数**: M (最大邻居数)、efConstruction (构建质量)、efSearch (搜索质量)

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_operator_methods/hnsw.h` | HNSW Join 方法定义 |
| `src/operator/join_operator_methods/hnsw.cpp` | HNSW Join 方法实现 |
| `test/UnitTest/test_join_hnsw.cpp` | 单元测试 |

---

## 推荐配置

```toml
[baseline.hnsw]
algorithm = "hnsw"
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "hnsw"
similarity_threshold = 0.8

[baseline.hnsw.params]
m = 16                    # 每层最大邻居数
ef_construction = 200     # 构建时的候选集大小
ef_search = 100           # 搜索时的候选集大小
use_existing_index = true # 复用已有 HNSW 索引
```

### 参数调优指南

| 参数 | 典型范围 | 影响 |
|-----|---------|------|
| M | 8-64 | 越大召回越高，但构建慢、内存大 |
| efConstruction | 100-500 | 越大图质量越高，构建越慢 |
| efSearch | 50-200 | 越大召回越高，查询越慢 |

---

## 接口设计

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "index/hnsw.h"
#include "concurrency/concurrency_manager.h"

namespace sageFlow {

/**
 * @brief HNSW Join 方法
 * 
 * 基于 HNSW 图索引的近似 Join 实现
 */
class HNSWMethod : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int m = 16;                   // 每层最大邻居数
        int ef_construction = 200;    // 构建候选集大小
        int ef_search = 100;          // 搜索候选集大小
        bool use_existing_index = true;
    };
    
    explicit HNSWMethod(const Config& config);
    ~HNSWMethod() override = default;
    
    std::string getName() const override { return "HNSW"; }
    JoinStrategyConfig getRecommendedConfig() const override;
    
    void open(const RuntimeContext& context, 
              JoinOperatorState* state) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries,
        int slot) override;
    
    void close() override;
    
    /**
     * @brief 设置搜索扩展因子
     */
    void setEfSearch(int ef_search);
    
    /**
     * @brief 获取当前索引统计信息
     */
    struct IndexStats {
        size_t num_elements;
        size_t num_layers;
        size_t memory_usage;
    };
    IndexStats getStats() const;

private:
    Config config_;
    JoinOperatorState* state_ = nullptr;
    size_t subtask_index_ = 0;
    ConcurrencyManager* concurrency_manager_ = nullptr;
    int32_t index_id_ = -1;
    
    /**
     * @brief 将余弦相似度转换为距离阈值
     */
    float similarityToDistance(double similarity) const;
    
    /**
     * @brief 范围搜索 HNSW
     */
    std::vector<std::shared_ptr<const VectorRecord>> rangeSearch(
        const VectorRecord& query, double threshold);
};

// 自动注册
REGISTER_JOIN_METHOD(HNSWMethod, "hnsw");

} // namespace sageFlow
```

---

## 实现要点

```cpp
#include "operator/join_operator_methods/hnsw_method.h"
#include <cmath>

namespace sageFlow {

HNSWMethod::HNSWMethod(const Config& config)
    : config_(config) {}

JoinStrategyConfig HNSWMethod::getRecommendedConfig() const {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::HNSW;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.index_strategy = IndexStrategy::HNSW;
    config.similarity_threshold = config_.similarity_threshold;
    config.hnsw_m = config_.m;
    config.hnsw_ef_construction = config_.ef_construction;
    config.hnsw_ef_search = config_.ef_search;
    return config;
}

void HNSWMethod::open(
    const RuntimeContext& context, 
    JoinOperatorState* state) {
    
    state_ = state;
    subtask_index_ = context.getSubtaskIndex();
    
    // 获取或创建 HNSW 索引
    concurrency_manager_ = state->getConcurrencyManager();
    
    if (config_.use_existing_index) {
        // 尝试获取已有索引
        index_id_ = concurrency_manager_->getIndexId("hnsw_join_index");
    }
    
    if (index_id_ < 0) {
        // 创建新索引
        std::map<std::string, std::string> params;
        params["m"] = std::to_string(config_.m);
        params["ef_construction"] = std::to_string(config_.ef_construction);
        
        index_id_ = concurrency_manager_->create_index(
            "hnsw_join_index",
            IndexType::HNSW,
            state->getVectorDimension(),
            params
        );
    }
}

std::vector<std::unique_ptr<VectorRecord>> HNSWMethod::ExecuteEager(
    const VectorRecord& query, int slot) {
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 使用 HNSW 进行范围搜索
    auto candidates = rangeSearch(query, config_.similarity_threshold);
    
    for (const auto& candidate : candidates) {
        results.push_back(std::make_unique<VectorRecord>(*candidate));
    }
    
    return results;
}

std::vector<std::unique_ptr<VectorRecord>> HNSWMethod::ExecuteLazy(
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

void HNSWMethod::close() {
    state_ = nullptr;
    concurrency_manager_ = nullptr;
}

float HNSWMethod::similarityToDistance(double similarity) const {
    // 对于余弦相似度，距离 = 1 - 相似度
    // 或者使用内积距离: distance = 1 - inner_product / (norm_a * norm_b)
    return static_cast<float>(1.0 - similarity);
}

std::vector<std::shared_ptr<const VectorRecord>> HNSWMethod::rangeSearch(
    const VectorRecord& query, double threshold) {
    
    // HNSW 原生不支持范围搜索，需要通过 k-NN 模拟
    // 策略：
    // 1. 先用较大的 k 搜索
    // 2. 然后过滤满足阈值的结果
    
    int k = config_.ef_search;  // 初始 k 值
    float distance_threshold = similarityToDistance(threshold);
    
    std::vector<std::shared_ptr<const VectorRecord>> results;
    
    // 执行 k-NN 查询
    auto candidates = concurrency_manager_->query(index_id_, query, k);
    
    // 过滤满足阈值的结果
    for (const auto& candidate : candidates) {
        double similarity = computeCosineSimilarity(
            query.getVector(), candidate->getVector());
        
        if (similarity >= threshold) {
            results.push_back(candidate);
        }
    }
    
    return results;
}

void HNSWMethod::setEfSearch(int ef_search) {
    config_.ef_search = ef_search;
    // 更新索引的 efSearch 参数
    if (concurrency_manager_ && index_id_ >= 0) {
        // 通过 ConcurrencyManager 更新参数
    }
}

HNSWMethod::IndexStats HNSWMethod::getStats() const {
    IndexStats stats;
    // 获取索引统计信息
    return stats;
}

} // namespace sageFlow
```

---

## 算法流程

```
┌─────────────────────────────────────────────────────────────┐
│                      HNSW Join 流程                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   索引构建阶段                        │    │
│  │  1. 确定入层: l = floor(-ln(uniform(0,1)) * mL)     │    │
│  │  2. 从最高层向下搜索最近邻                           │    │
│  │  3. 在每层建立双向边                                 │    │
│  │  4. 维护每层最大邻居数 M                             │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   查询处理阶段                        │    │
│  │  1. 从入口点开始搜索                                 │    │
│  │  2. 在最高层找到最近邻                               │    │
│  │  3. 逐层向下，每层扩展候选集                         │    │
│  │  4. 最底层返回 top-k 结果                            │    │
│  │  5. 过滤满足阈值的结果                               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   增量更新阶段                        │    │
│  │  1. 新记录直接插入（同构建阶段）                     │    │
│  │  2. 过期记录标记删除（懒删除）                       │    │
│  │  3. 定期压缩重建（可选）                             │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 核心公式

### 层数分配

每个节点的最大层数由指数分布决定：

$$l = \lfloor -\ln(u) \cdot m_L \rfloor$$

其中 $u \sim U(0,1)$，$m_L = 1 / \ln(M)$

### 搜索复杂度

- **平均**: $O(\log N)$
- **最坏**: $O(N)$（但极少发生）

### 空间复杂度

$$O(N \cdot M \cdot L_{avg})$$

其中 $L_{avg} \approx \ln(N) / \ln(M)$

---

## 测试要求

```cpp
TEST(HNSWMethodTest, BasicFunctionality) {
    HNSWMethod::Config config;
    config.similarity_threshold = 0.8;
    config.m = 16;
    config.ef_construction = 100;
    config.ef_search = 50;
    
    HNSWMethod method(config);
    
    // 基本功能测试
}

TEST(HNSWMethodTest, RecommendedConfig) {
    HNSWMethod::Config config;
    HNSWMethod method(config);
    auto rec_config = method.getRecommendedConfig();
    
    EXPECT_EQ(rec_config.algorithm, JoinAlgorithm::HNSW);
    EXPECT_EQ(rec_config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(rec_config.window_state_type, WindowStateType::SHARED);
}

TEST(HNSWMethodTest, RecallVsBruteForce) {
    // 与 BruteForce 对比召回率
    // 要求: 召回率 >= 95% @ threshold=0.8
}

TEST(HNSWMethodTest, EfSearchTuning) {
    // 测试不同 efSearch 对召回率的影响
}

TEST(HNSWMethodTest, IncrementalInsert) {
    // 测试增量插入功能
}

TEST(HNSWMethodTest, Registration) {
    auto registry = JoinMethodRegistry::getInstance();
    EXPECT_TRUE(registry->hasMethod("hnsw"));
}
```

---

## 性能目标

| 指标 | 目标值 | 说明 |
|-----|-------|------|
| Recall@0.8 | ≥ 95% | 相似度阈值 0.8 时的召回率 |
| 索引构建 | < 0.5ms/record | 单条记录索引时间 |
| 查询延迟 | < 1ms | 单次查询平均延迟 |
| 吞吐量 | > 10000 QPS | 查询吞吐量 |

---

## 验收标准

1. ✅ 正确封装现有 HNSW 索引
2. ✅ 范围搜索功能正确
3. ✅ 召回率达到目标
4. ✅ 参数可配置
5. ✅ 所有单元测试通过
6. ✅ 代码符合项目编码规范
7. ✅ 正确实现自动注册

---

## 与现有代码集成

本任务主要是封装现有的 `include/index/hnsw.h` 实现：

1. 复用 `HNSW` 类的核心功能
2. 通过 `ConcurrencyManager` 进行线程安全访问
3. 添加范围搜索（阈值过滤）支持
4. 适配 `BaseMethod` 接口

---

## 参考资料

### 论文链接

- [IEEE TPAMI 2018](https://doi.org/10.1109/TPAMI.2018.2889473)
- [arXiv 2016](https://arxiv.org/abs/1603.09320)

### 代码参考

- hnswlib: https://github.com/nmslib/hnswlib
- 项目现有 HNSW: `include/index/hnsw.h`

### 相关文档

- [GROUP_D_README.md](./README.md) - Group D 总览
- [D01_BruteForce.md](./D01_BruteForce.md) - Ground Truth

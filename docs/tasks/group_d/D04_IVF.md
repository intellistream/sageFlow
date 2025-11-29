# D-04: IVF Enhanced Baseline

**优先级**: 🔴 高  
**预估工时**: 2天  
**依赖**: D-01 (Ground Truth)  
**状态**: ⬜ 待开始

---

## 任务概述

基于现有 IVF (Inverted File) 索引实现增强版 Join 方法，利用聚类分区进行高效的近似最近邻搜索，后续考虑接入faiss库的IVF，可留出相关接口待后续实现。

---

## 论文依据

### 主要参考

| 来源 | 描述 | 链接 |
|-----|------|-----|
| Faiss | Facebook AI 相似性搜索库 | https://github.com/facebookresearch/faiss |
| IEEE TBD 2017 | Billion-scale similarity search with GPUs | 10.1109/TBDATA.2019.2921572 |

### 算法要点

1. **聚类分区**: 使用 k-means 将向量空间划分为 nlist 个簇
2. **倒排索引**: 每个簇维护一个倒排列表，存储属于该簇的向量
3. **多路搜索**: 查询时搜索 nprobe 个最近的簇
4. **精确计算**: 在候选集中计算精确距离

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_operator_methods/ivf_method.h` | IVF Join 方法定义 |
| `src/operator/join_operator_methods/ivf_method.cpp` | IVF Join 方法实现 |
| `test/UnitTest/test_ivf_method.cpp` | 单元测试 |

---

## 推荐配置

```toml
[baseline.ivf]
algorithm = "ivf"
partition_strategy = "round_robin"
window_state_type = "shared"
index_strategy = "ivf"
similarity_threshold = 0.8

[baseline.ivf.params]
nlist = 100               # 聚类数量
nprobes = 10              # 搜索的簇数量
rebuild_threshold = 0.2   # 重建阈值（数据变化比例）
use_residual = false      # 是否使用残差量化
```

### 参数调优指南

| 参数 | 典型范围 | 影响 |
|-----|---------|------|
| nlist | 4√N ~ 16√N | 越大精度越高，但聚类开销大 |
| nprobes | 1 ~ nlist | 越大召回越高，但查询越慢 |
| rebuild_threshold | 0.1 ~ 0.5 | 触发聚类重建的数据变化比例 |

---

## 接口设计

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "index/ivf.h"
#include "concurrency/concurrency_manager.h"

namespace sageFlow {

/**
 * @brief IVF Join 方法
 * 
 * 基于 IVF 倒排索引的近似 Join 实现
 */
class IVFMethod : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int nlist = 100;              // 聚类数量
        int nprobes = 10;             // 搜索的簇数量
        double rebuild_threshold = 0.2;
        bool use_residual = false;    // 是否使用残差量化
    };
    
    explicit IVFMethod(const Config& config);
    ~IVFMethod() override = default;
    
    std::string getName() const override { return "IVF"; }
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
     * @brief 触发聚类重建
     */
    void rebuildClusters();
    
    /**
     * @brief 设置 nprobes
     */
    void setNprobes(int nprobes);
    
    /**
     * @brief 获取索引统计信息
     */
    struct IndexStats {
        size_t num_elements;
        size_t num_clusters;
        std::vector<size_t> cluster_sizes;
        double cluster_balance;  // 簇大小均衡度
    };
    IndexStats getStats() const;

private:
    Config config_;
    JoinOperatorState* state_ = nullptr;
    size_t subtask_index_ = 0;
    ConcurrencyManager* concurrency_manager_ = nullptr;
    int32_t index_id_ = -1;
    
    // 用于跟踪数据变化
    size_t last_rebuild_size_ = 0;
    size_t current_size_ = 0;
    
    /**
     * @brief 检查是否需要重建
     */
    bool needsRebuild() const;
    
    /**
     * @brief 范围搜索
     */
    std::vector<std::shared_ptr<const VectorRecord>> rangeSearch(
        const VectorRecord& query, double threshold);
};

// 自动注册
REGISTER_JOIN_METHOD(IVFMethod, "ivf");

} // namespace sageFlow
```

---

## 实现要点

```cpp
#include "operator/join_operator_methods/ivf_method.h"
#include <algorithm>
#include <cmath>

namespace sageFlow {

IVFMethod::IVFMethod(const Config& config)
    : config_(config) {}

JoinStrategyConfig IVFMethod::getRecommendedConfig() const {
    JoinStrategyConfig config;
    config.algorithm = JoinAlgorithm::IVF;
    config.partition_strategy = PartitionStrategy::ROUND_ROBIN;
    config.window_state_type = WindowStateType::SHARED;
    config.index_strategy = IndexStrategy::IVF;
    config.similarity_threshold = config_.similarity_threshold;
    config.ivf_nlist = config_.nlist;
    config.ivf_nprobes = config_.nprobes;
    return config;
}

void IVFMethod::open(
    const RuntimeContext& context, 
    JoinOperatorState* state) {
    
    state_ = state;
    subtask_index_ = context.getSubtaskIndex();
    
    concurrency_manager_ = state->getConcurrencyManager();
    
    // 创建 IVF 索引
    std::map<std::string, std::string> params;
    params["nlist"] = std::to_string(config_.nlist);
    params["nprobes"] = std::to_string(config_.nprobes);
    params["rebuild_threshold"] = std::to_string(config_.rebuild_threshold);
    
    index_id_ = concurrency_manager_->create_index(
        "ivf_join_index",
        IndexType::IVF,
        state->getVectorDimension(),
        params
    );
    
    last_rebuild_size_ = 0;
    current_size_ = 0;
}

std::vector<std::unique_ptr<VectorRecord>> IVFMethod::ExecuteEager(
    const VectorRecord& query, int slot) {
    
    // 检查是否需要重建
    if (needsRebuild()) {
        rebuildClusters();
    }
    
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 范围搜索
    auto candidates = rangeSearch(query, config_.similarity_threshold);
    
    for (const auto& candidate : candidates) {
        results.push_back(std::make_unique<VectorRecord>(*candidate));
    }
    
    return results;
}

std::vector<std::unique_ptr<VectorRecord>> IVFMethod::ExecuteLazy(
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

void IVFMethod::close() {
    state_ = nullptr;
    concurrency_manager_ = nullptr;
}

bool IVFMethod::needsRebuild() const {
    if (last_rebuild_size_ == 0) {
        return current_size_ >= config_.nlist;  // 首次构建
    }
    
    double change_ratio = static_cast<double>(current_size_ - last_rebuild_size_) 
                         / static_cast<double>(last_rebuild_size_);
    
    return change_ratio >= config_.rebuild_threshold;
}

void IVFMethod::rebuildClusters() {
    // 触发索引重建
    // 实际实现中通过 ConcurrencyManager 调用
    last_rebuild_size_ = current_size_;
}

void IVFMethod::setNprobes(int nprobes) {
    config_.nprobes = nprobes;
    // 更新索引的 nprobes 参数
}

std::vector<std::shared_ptr<const VectorRecord>> IVFMethod::rangeSearch(
    const VectorRecord& query, double threshold) {
    
    // IVF 原生不支持范围搜索，通过 k-NN + 过滤实现
    int k = config_.nprobes * 10;  // 估计的候选数量
    
    auto candidates = concurrency_manager_->query(index_id_, query, k);
    
    std::vector<std::shared_ptr<const VectorRecord>> results;
    
    for (const auto& candidate : candidates) {
        double similarity = computeCosineSimilarity(
            query.getVector(), candidate->getVector());
        
        if (similarity >= threshold) {
            results.push_back(candidate);
        }
    }
    
    return results;
}

IVFMethod::IndexStats IVFMethod::getStats() const {
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
│                      IVF Join 流程                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   聚类训练阶段                        │    │
│  │  1. 收集训练样本                                     │    │
│  │  2. k-means 聚类生成 nlist 个质心                    │    │
│  │  3. 初始化 nlist 个倒排列表                          │    │
│  └─────────────────────────────────────────────────────┘    │
│         │                                                    │
│         ↓                                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   索引构建阶段                        │    │
│  │  1. 计算向量到各质心的距离                           │    │
│  │  2. 分配到最近的簇                                   │    │
│  │  3. 添加到对应倒排列表                               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   查询处理阶段                        │    │
│  │  1. 计算查询向量到所有质心的距离                     │    │
│  │  2. 选择最近的 nprobes 个簇                          │    │
│  │  3. 在选中的簇中计算精确距离                         │    │
│  │  4. 过滤满足阈值的结果                               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   增量维护阶段                        │    │
│  │  1. 新向量直接插入最近的簇                           │    │
│  │  2. 定期检查簇平衡度                                 │    │
│  │  3. 超过阈值时触发重聚类                             │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 核心公式

### 聚类数量选择

经验公式：

$$nlist = \sqrt{N}$$

或更保守的：

$$nlist = 4\sqrt{N} \sim 16\sqrt{N}$$

### 召回率估计

在均匀分布假设下，搜索 $p$ 个簇的召回率约为：

$$Recall \approx \frac{p}{nlist}$$

实际数据通常更好，因为相似向量倾向于聚集在同一簇。

### 查询复杂度

$$O(D \cdot nlist + D \cdot nprobes \cdot \frac{N}{nlist})$$

- 第一项：质心距离计算
- 第二项：候选向量精确距离计算

---

## 测试要求

```cpp
TEST(IVFMethodTest, BasicFunctionality) {
    IVFMethod::Config config;
    config.similarity_threshold = 0.8;
    config.nlist = 16;
    config.nprobes = 4;
    
    IVFMethod method(config);
    
    // 基本功能测试
}

TEST(IVFMethodTest, RecommendedConfig) {
    IVFMethod::Config config;
    IVFMethod method(config);
    auto rec_config = method.getRecommendedConfig();
    
    EXPECT_EQ(rec_config.algorithm, JoinAlgorithm::IVF);
    EXPECT_EQ(rec_config.partition_strategy, PartitionStrategy::ROUND_ROBIN);
    EXPECT_EQ(rec_config.window_state_type, WindowStateType::SHARED);
}

TEST(IVFMethodTest, RecallVsBruteForce) {
    // 与 BruteForce 对比召回率
    // 要求: 召回率 >= 90% @ threshold=0.8, nprobes=10
}

TEST(IVFMethodTest, NprobesTuning) {
    // 测试不同 nprobes 对召回率的影响
}

TEST(IVFMethodTest, ClusterRebuild) {
    // 测试聚类重建功能
}

TEST(IVFMethodTest, ClusterBalance) {
    // 测试簇平衡度
}

TEST(IVFMethodTest, Registration) {
    auto registry = JoinMethodRegistry::getInstance();
    EXPECT_TRUE(registry->hasMethod("ivf"));
}
```

---

## 性能目标

| 指标 | 目标值 | 说明 |
|-----|-------|------|
| Recall@0.8 | ≥ 90% | 相似度阈值 0.8, nprobes=10 |
| 索引构建 | < 0.2ms/record | 不含聚类训练 |
| 聚类训练 | < 1s/10000 | 10000 样本训练时间 |
| 查询延迟 | < 2ms | 单次查询平均延迟 |
| 吞吐量 | > 5000 QPS | 查询吞吐量 |

---

## 验收标准

1. ✅ 正确封装现有 IVF 索引
2. ✅ 聚类训练功能正确
3. ✅ 增量插入和重建功能
4. ✅ 召回率达到目标
5. ✅ 所有单元测试通过
6. ✅ 代码符合项目编码规范
7. ✅ 正确实现自动注册

---

## 与现有代码集成

本任务主要是封装现有的 `include/index/ivf.h` 实现：

1. 复用 `Ivf` 类的核心功能
2. 通过 `ConcurrencyManager` 进行线程安全访问
3. 添加范围搜索（阈值过滤）支持
4. 添加自动重建机制
5. 适配 `BaseMethod` 接口

---

## 参考资料

### 论文/库链接

- [Faiss GitHub](https://github.com/facebookresearch/faiss)
- [Faiss Wiki - IVF](https://github.com/facebookresearch/faiss/wiki/Faiss-indexes#cell-probe-methods-indexivf-indexes)
- [IEEE TBD 2017](https://doi.org/10.1109/TBDATA.2019.2921572)

### 代码参考

- 项目现有 IVF: `include/index/ivf.h`
- Faiss Python 接口示例

### 相关文档

- [GROUP_D_README.md](./README.md) - Group D 总览
- [D01_BruteForce.md](./D01_BruteForce.md) - Ground Truth
- [D03_HNSW.md](./D03_HNSW.md) - HNSW 对比

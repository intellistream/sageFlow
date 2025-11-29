# D-02: HDR-Tree Baseline

**优先级**: 🟡 中  
**预估工时**: 3天  
**依赖**: D-01 (Ground Truth)  
**状态**: ⬜ 待开始

---

## 任务概述

实现 HDR-Tree (High-Dimensional R-Tree) 方法，基于 PCA 降维和双索引结构进行高效的高维向量 Join。

---

## 论文依据

### 主要论文

| 标题 | 会议/期刊 | 年份 | DOI |
|-----|----------|-----|-----|
| Efficient kNN Join over Dynamic High-Dimensional Data | ADC 2022 (LNCS 13459) | 2022 | 10.1007/978-3-031-15512-3_5 |
| Efficient kNN Join over Dynamic High-Dimensional Data (Extended) | World Wide Web Journal | 2023 | 10.1007/s11280-023-01204-9 |
| High-Dimensional kNN Joins with Incremental Updates | ICDM | 2014 | - |

### 核心作者

- Nimish Ukey
- Zhengyi Yang
- Guangjian Zhang
- Binghao Li
- Wei Wang

### 算法要点

1. **PCA 降维**: 将高维向量投影到低维空间
2. **双索引结构**: 
   - R-Tree 用于低维空间快速剪枝
   - 原始高维数据用于精确验证
3. **增量更新**: 支持动态数据的高效更新
4. **阈值估计**: 基于降维距离估计原始距离上下界

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/index/hdr_tree.h` | HDR-Tree 索引定义 |
| `src/index/hdr_tree.cpp` | HDR-Tree 索引实现 |
| `include/operator/join_operator_methods/hdr_tree_method.h` | Join 方法定义 |
| `src/operator/join_operator_methods/hdr_tree_method.cpp` | Join 方法实现 |
| `include/utils/pca_projector.h` | PCA 投影器 |
| `src/utils/pca_projector.cpp` | PCA 投影器实现 |
| `test/UnitTest/test_hdr_tree.cpp` | 单元测试 |

---

## 推荐配置

```toml
[baseline.hdr_tree]
algorithm = "hdr_tree"
partition_strategy = "key_hash"
window_state_type = "partitioned"
index_strategy = "hdr_tree"
similarity_threshold = 0.8

[baseline.hdr_tree.params]
projected_dim = 16          # 降维后维度
pca_sample_size = 10000     # PCA 训练样本数
distance_bound_ratio = 1.2  # 距离上下界比例
rtree_min_entries = 4       # R-Tree 最小条目
rtree_max_entries = 16      # R-Tree 最大条目
```

---

## 接口设计

### PCA 投影器

```cpp
#pragma once

#include <vector>
#include <Eigen/Dense>

namespace sageFlow {

/**
 * @brief PCA 投影器
 * 
 * 使用 PCA 将高维向量投影到低维空间
 */
class PCAProjector {
public:
    /**
     * @brief 构造函数
     * @param original_dim 原始维度
     * @param projected_dim 目标维度
     */
    PCAProjector(int original_dim, int projected_dim);
    
    /**
     * @brief 使用样本数据训练 PCA
     * @param samples 样本向量集合
     */
    void train(const std::vector<std::vector<float>>& samples);
    
    /**
     * @brief 投影单个向量
     * @param vec 原始向量
     * @return 降维后的向量
     */
    std::vector<float> project(const std::vector<float>& vec) const;
    
    /**
     * @brief 批量投影
     * @param vecs 原始向量集合
     * @return 降维后的向量集合
     */
    std::vector<std::vector<float>> projectBatch(
        const std::vector<std::vector<float>>& vecs) const;
    
    /**
     * @brief 检查是否已训练
     */
    bool isTrained() const { return trained_; }
    
    /**
     * @brief 获取投影矩阵
     */
    const Eigen::MatrixXf& getProjectionMatrix() const;
    
    /**
     * @brief 估计原始距离上下界
     * @param projected_dist 投影空间距离
     * @return {下界, 上界}
     */
    std::pair<float, float> estimateDistanceBounds(
        float projected_dist) const;

private:
    int original_dim_;
    int projected_dim_;
    bool trained_ = false;
    Eigen::MatrixXf projection_matrix_;  // projected_dim x original_dim
    Eigen::VectorXf mean_;               // 均值向量
    Eigen::VectorXf singular_values_;    // 奇异值（用于距离估计）
};

} // namespace sageFlow
```

### HDR-Tree 索引

```cpp
#pragma once

#include "index/index.h"
#include "utils/pca_projector.h"
#include <memory>

namespace sageFlow {

/**
 * @brief HDR-Tree 索引
 * 
 * 基于 PCA 降维和 R-Tree 的高维向量索引
 */
class HDRTree : public Index {
public:
    struct Config {
        int projected_dim = 16;
        int rtree_min_entries = 4;
        int rtree_max_entries = 16;
        int pca_sample_size = 10000;
        float distance_bound_ratio = 1.2f;
    };
    
    /**
     * @brief 构造函数
     * @param dimension 原始向量维度
     * @param config 配置
     */
    HDRTree(int dimension, const Config& config);
    
    ~HDRTree() override = default;
    
    /**
     * @brief 训练 PCA 投影器
     */
    void trainPCA(const std::vector<std::vector<float>>& samples);
    
    /**
     * @brief 插入向量
     */
    bool insert(uint64_t uid) override;
    
    /**
     * @brief 删除向量
     */
    bool erase(uint64_t uid) override;
    
    /**
     * @brief 范围查询（基于阈值）
     */
    std::vector<std::shared_ptr<const VectorRecord>> queryForJoin(
        const VectorRecord& query, double threshold);
    
    /**
     * @brief k-NN 查询
     */
    std::vector<std::shared_ptr<const VectorRecord>> query(
        const VectorRecord& query, int k) const override;
    
    /**
     * @brief 获取索引类型
     */
    IndexType getType() const override { return IndexType::HDRTree; }

private:
    Config config_;
    std::unique_ptr<PCAProjector> pca_projector_;
    
    // R-Tree 结构（简化表示）
    struct RTreeNode {
        std::vector<float> mbr_low;   // 最小边界
        std::vector<float> mbr_high;  // 最大边界
        std::vector<uint64_t> entries;
        std::vector<std::unique_ptr<RTreeNode>> children;
        bool is_leaf = true;
    };
    std::unique_ptr<RTreeNode> rtree_root_;
    
    /**
     * @brief 在 R-Tree 中搜索候选
     */
    std::vector<uint64_t> searchRTree(
        const std::vector<float>& projected_query,
        float threshold) const;
    
    /**
     * @brief 验证候选
     */
    std::vector<std::shared_ptr<const VectorRecord>> verifyCandidates(
        const VectorRecord& query,
        const std::vector<uint64_t>& candidates,
        double threshold) const;
};

} // namespace sageFlow
```

### HDR-Tree Join 方法

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "index/hdr_tree.h"

namespace sageFlow {

/**
 * @brief HDR-Tree Join 方法
 */
class HDRTreeMethod : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int projected_dim = 16;
        int pca_sample_size = 10000;
    };
    
    explicit HDRTreeMethod(const Config& config);
    ~HDRTreeMethod() override = default;
    
    std::string getName() const override { return "HDR-Tree"; }
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
     * @brief 训练 PCA（需要在第一批数据到达后调用）
     */
    void trainPCAFromSamples(
        const std::vector<std::vector<float>>& samples);

private:
    Config config_;
    std::shared_ptr<HDRTree> index_;
    JoinOperatorState* state_ = nullptr;
    size_t subtask_index_ = 0;
    bool pca_trained_ = false;
    
    // 采样缓冲区
    std::vector<std::vector<float>> sample_buffer_;
};

// 自动注册
REGISTER_JOIN_METHOD(HDRTreeMethod, "hdr_tree");

} // namespace sageFlow
```

---

## 算法流程

```
┌─────────────────────────────────────────────────────────────┐
│                      HDR-Tree Join 流程                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐   PCA训练    ┌─────────────────────────┐   │
│  │  样本采集   │ ─────────→  │   训练 PCA 投影矩阵     │   │
│  └─────────────┘              └─────────────────────────┘   │
│         │                                                    │
│         ↓                                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   索引构建阶段                        │    │
│  │  1. 投影向量: v' = P * (v - mean)                   │    │
│  │  2. 插入 R-Tree                                      │    │
│  │  3. 维护原始向量引用                                 │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   查询处理阶段                        │    │
│  │  1. 投影查询: q' = P * (q - mean)                   │    │
│  │  2. R-Tree 范围搜索（估计阈值）                      │    │
│  │  3. 候选验证（原始空间精确距离）                     │    │
│  │  4. 返回满足阈值的结果                               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 距离估计公式

论文中的核心公式：

设 $d(v, q)$ 为原始空间距离，$d'(v', q')$ 为投影空间距离，则：

$$d'(v', q') \leq d(v, q) \leq \alpha \cdot d'(v', q')$$

其中 $\alpha$ 由奇异值分布决定：

$$\alpha = \sqrt{\frac{\sum_{i=1}^{D} \sigma_i^2}{\sum_{i=1}^{d} \sigma_i^2}}$$

- $D$: 原始维度
- $d$: 降维后维度
- $\sigma_i$: 第 $i$ 个奇异值

---

## 测试要求

```cpp
TEST(PCAProjectorTest, TrainAndProject) {
    PCAProjector projector(128, 16);
    
    // 生成随机样本
    std::vector<std::vector<float>> samples(1000);
    for (auto& s : samples) {
        s.resize(128);
        // 填充随机值
    }
    
    projector.train(samples);
    EXPECT_TRUE(projector.isTrained());
    
    // 测试投影
    std::vector<float> vec(128, 1.0f);
    auto projected = projector.project(vec);
    EXPECT_EQ(projected.size(), 16);
}

TEST(HDRTreeTest, InsertAndQuery) {
    HDRTree::Config config;
    config.projected_dim = 8;
    HDRTree tree(128, config);
    
    // 训练 PCA
    // 插入向量
    // 查询验证
}

TEST(HDRTreeMethodTest, Integration) {
    HDRTreeMethod::Config config;
    config.similarity_threshold = 0.8;
    HDRTreeMethod method(config);
    
    // 集成测试
}

TEST(HDRTreeMethodTest, RecallVsBruteForce) {
    // 与 BruteForce 对比召回率
    // 要求: 召回率 >= 90% @ threshold=0.8
}

TEST(HDRTreeMethodTest, Registration) {
    auto registry = JoinMethodRegistry::getInstance();
    EXPECT_TRUE(registry->hasMethod("hdr_tree"));
}
```

---

## 性能目标

| 指标 | 目标值 | 说明 |
|-----|-------|------|
| Recall@0.8 | ≥ 90% | 相似度阈值 0.8 时的召回率 |
| 索引构建 | < 1ms/record | 单条记录索引时间 |
| 查询延迟 | < 5ms | 单次查询平均延迟 |
| 内存开销 | < 2x | 相比仅存储原始向量 |

---

## 验收标准

1. ✅ PCA 投影器正确实现
2. ✅ R-Tree 索引功能正确
3. ✅ 距离估计公式正确
4. ✅ 召回率达到目标
5. ✅ 所有单元测试通过
6. ✅ 代码符合项目编码规范
7. ✅ 正确实现自动注册

---

## 实现注意事项

1. **Eigen 依赖**: 需要在 CMakeLists.txt 中添加 Eigen 依赖
2. **PCA 训练**: 需要足够样本才能训练有效的投影矩阵
3. **R-Tree 实现**: 可考虑使用 boost::geometry 的 R-Tree 实现
4. **增量更新**: 论文支持增量更新，但初版可简化为批量重建

---

## 参考资料

### 论文链接

- [ADC 2022 Paper](https://doi.org/10.1007/978-3-031-15512-3_5)
- [WWW Journal Extended](https://doi.org/10.1007/s11280-023-01204-9)

### 代码参考

- Eigen 文档: https://eigen.tuxfamily.org/
- Boost.Geometry R-Tree: https://www.boost.org/doc/libs/release/libs/geometry/

### 相关文档

- [GROUP_D_README.md](./README.md) - Group D 总览
- [D01_BruteForce.md](./D01_BruteForce.md) - Ground Truth

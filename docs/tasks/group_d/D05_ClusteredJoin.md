# D-05: ClusteredJoin Baseline

**优先级**: 🟡 中  
**预估工时**: 3天  
**依赖**: D-01 (Ground Truth), D-04 (IVF)  
**状态**: ⬜ 待开始

---

## 任务概述

实现 ClusteredJoin 方法，这是 VectraFlow 项目的内部设计，基于聚类的分布式向量 Join 策略。

---

## 参考来源

| 来源 | 描述 |
|-----|------|
| VectraFlow | 内部设计文档 |
| SageFlow | 现有索引实现 |

### 算法要点

1. **Centroid Partitioner**: 基于质心的向量空间分区
2. **两阶段查询**: 先确定候选分区，再在分区内精确搜索
3. **边界处理**: 处理跨分区边界的相似向量
4. **负载均衡**: 动态调整分区以平衡负载

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/operator/join_operator_methods/clustered_join_method.h` | ClusteredJoin 方法定义 |
| `src/operator/join_operator_methods/clustered_join_method.cpp` | ClusteredJoin 方法实现 |
| `include/stream/partitioner/centroid_partitioner.h` | 质心分区器定义 |
| `src/stream/partitioner/centroid_partitioner.cpp` | 质心分区器实现 |
| `test/UnitTest/test_clustered_join.cpp` | 单元测试 |

---

## 推荐配置

```toml
[baseline.clustered_join]
algorithm = "clustered_join"
partition_strategy = "centroid"
window_state_type = "partitioned"
index_strategy = "ivf"
similarity_threshold = 0.8

[baseline.clustered_join.params]
num_partitions = 16           # 分区数量
overlap_ratio = 0.1           # 边界重叠比例
rebalance_threshold = 0.3     # 重平衡阈值
centroid_init = "kmeans++"    # 质心初始化方法
```

---

## 接口设计

### CentroidPartitioner

```cpp
#pragma once

#include "stream/partitioner/i_partitioner.h"
#include <vector>
#include <memory>

namespace sageFlow {

/**
 * @brief 质心分区器
 * 
 * 基于 k-means 聚类的向量空间分区策略
 */
class CentroidPartitioner : public IPartitioner {
public:
    struct Config {
        int num_partitions = 16;       // 分区数量
        double overlap_ratio = 0.1;    // 边界重叠比例
        int max_iterations = 100;      // k-means 最大迭代次数
        std::string init_method = "kmeans++";  // 初始化方法
    };
    
    explicit CentroidPartitioner(const Config& config);
    ~CentroidPartitioner() override = default;
    
    /**
     * @brief 训练质心
     * @param samples 训练样本
     */
    void train(const std::vector<std::vector<float>>& samples);
    
    /**
     * @brief 获取向量的分区
     * @param record 向量记录
     * @return 分区索引列表（可能返回多个，用于边界处理）
     */
    std::vector<int> getPartitions(const VectorRecord& record) const;
    
    /**
     * @brief IPartitioner 接口实现
     */
    int partition(const Response& data, int num_partitions) override;
    
    /**
     * @brief 获取质心
     */
    const std::vector<std::vector<float>>& getCentroids() const;
    
    /**
     * @brief 更新质心（增量学习）
     */
    void updateCentroids(const std::vector<std::vector<float>>& new_samples);
    
    /**
     * @brief 检查是否需要重平衡
     */
    bool needsRebalance(const std::vector<size_t>& partition_sizes) const;
    
    /**
     * @brief 获取分区统计
     */
    struct PartitionStats {
        std::vector<size_t> sizes;
        double balance_score;  // 0-1, 1 表示完美均衡
    };
    PartitionStats getStats() const;

private:
    Config config_;
    std::vector<std::vector<float>> centroids_;
    bool trained_ = false;
    
    /**
     * @brief k-means++ 初始化
     */
    void initKMeansPlusPlus(
        const std::vector<std::vector<float>>& samples);
    
    /**
     * @brief 计算向量到质心的距离
     */
    std::vector<float> computeDistances(
        const std::vector<float>& vec) const;
    
    /**
     * @brief 获取边界分区
     */
    std::vector<int> getBorderPartitions(
        const std::vector<float>& distances) const;
};

} // namespace sageFlow
```

### ClusteredJoinMethod

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "stream/partitioner/centroid_partitioner.h"
#include "index/ivf.h"

namespace sageFlow {

/**
 * @brief ClusteredJoin 方法
 * 
 * 基于质心分区的分布式 Join 实现
 */
class ClusteredJoinMethod : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int num_partitions = 16;
        double overlap_ratio = 0.1;
        double rebalance_threshold = 0.3;
        bool use_border_replication = true;  // 边界向量复制
    };
    
    explicit ClusteredJoinMethod(const Config& config);
    ~ClusteredJoinMethod() override = default;
    
    std::string getName() const override { return "ClusteredJoin"; }
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
     * @brief 触发重平衡
     */
    void rebalance();
    
    /**
     * @brief 获取分区器
     */
    std::shared_ptr<CentroidPartitioner> getPartitioner() const {
        return partitioner_;
    }

private:
    Config config_;
    JoinOperatorState* state_ = nullptr;
    size_t subtask_index_ = 0;
    size_t parallelism_ = 0;
    
    std::shared_ptr<CentroidPartitioner> partitioner_;
    ConcurrencyManager* concurrency_manager_ = nullptr;
    int32_t index_id_ = -1;
    
    // 分区状态
    std::vector<size_t> partition_sizes_;
    
    /**
     * @brief 在本地分区内搜索
     */
    std::vector<std::shared_ptr<const VectorRecord>> searchLocalPartition(
        const VectorRecord& query, double threshold);
    
    /**
     * @brief 处理边界查询
     */
    std::vector<std::shared_ptr<const VectorRecord>> searchBorderPartitions(
        const VectorRecord& query, 
        const std::vector<int>& partitions,
        double threshold);
    
    /**
     * @brief 合并去重结果
     */
    void deduplicateResults(
        std::vector<std::shared_ptr<const VectorRecord>>& results);
};

// 自动注册
REGISTER_JOIN_METHOD(ClusteredJoinMethod, "clustered_join");

} // namespace sageFlow
```

---

## 算法流程

```
┌─────────────────────────────────────────────────────────────┐
│                   ClusteredJoin 流程                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   分区初始化阶段                      │    │
│  │  1. 收集训练样本                                     │    │
│  │  2. k-means++ 初始化质心                             │    │
│  │  3. 迭代优化质心位置                                 │    │
│  │  4. 确定边界区域                                     │    │
│  └─────────────────────────────────────────────────────┘    │
│         │                                                    │
│         ↓                                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   数据分区阶段                        │    │
│  │  1. 计算向量到所有质心的距离                         │    │
│  │  2. 分配到最近的分区                                 │    │
│  │  3. 边界向量复制到相邻分区（可选）                   │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   查询处理阶段                        │    │
│  │  1. 确定查询向量的主分区                             │    │
│  │  2. 在主分区内搜索候选                               │    │
│  │  3. 如果在边界区域，搜索相邻分区                     │    │
│  │  4. 合并去重返回结果                                 │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   动态重平衡阶段                      │    │
│  │  1. 监控各分区大小                                   │    │
│  │  2. 检测不平衡（方差超过阈值）                       │    │
│  │  3. 触发重聚类                                       │    │
│  │  4. 数据迁移                                         │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 边界处理策略

### 问题描述

相似向量可能被分配到不同分区，导致跨分区匹配丢失。

### 解决方案

1. **边界复制** (Border Replication)
   - 靠近分区边界的向量复制到相邻分区
   - 复制比例由 `overlap_ratio` 控制
   - 查询时需要去重

2. **多分区查询** (Multi-Partition Query)
   - 如果查询向量靠近边界，同时查询多个分区
   - 通过距离阈值判断是否需要扩展查询

### 边界判定公式

设 $d_1$ 为到最近质心的距离，$d_2$ 为到次近质心的距离：

$$\text{isBorder} = \frac{d_2 - d_1}{d_1} < \text{overlap\_ratio}$$

---

## 测试要求

```cpp
TEST(CentroidPartitionerTest, TrainAndPartition) {
    CentroidPartitioner::Config config;
    config.num_partitions = 4;
    CentroidPartitioner partitioner(config);
    
    // 生成训练样本
    std::vector<std::vector<float>> samples(1000);
    // 填充...
    
    partitioner.train(samples);
    
    // 测试分区
    std::vector<float> vec(128, 1.0f);
    VectorRecord record(1, vec, 0);
    auto partitions = partitioner.getPartitions(record);
    
    EXPECT_FALSE(partitions.empty());
    EXPECT_LT(partitions[0], config.num_partitions);
}

TEST(CentroidPartitionerTest, BorderDetection) {
    // 测试边界检测
}

TEST(ClusteredJoinMethodTest, BasicFunctionality) {
    ClusteredJoinMethod::Config config;
    config.similarity_threshold = 0.8;
    config.num_partitions = 4;
    
    ClusteredJoinMethod method(config);
    // 基本功能测试
}

TEST(ClusteredJoinMethodTest, RecommendedConfig) {
    ClusteredJoinMethod::Config config;
    ClusteredJoinMethod method(config);
    auto rec_config = method.getRecommendedConfig();
    
    EXPECT_EQ(rec_config.algorithm, JoinAlgorithm::CLUSTERED_JOIN);
    EXPECT_EQ(rec_config.partition_strategy, PartitionStrategy::CENTROID);
    EXPECT_EQ(rec_config.window_state_type, WindowStateType::PARTITIONED);
}

TEST(ClusteredJoinMethodTest, RecallWithBorderHandling) {
    // 测试边界处理对召回率的影响
    // 要求: 召回率 >= 90% @ threshold=0.8
}

TEST(ClusteredJoinMethodTest, LoadBalance) {
    // 测试负载均衡
}

TEST(ClusteredJoinMethodTest, Rebalance) {
    // 测试重平衡功能
}

TEST(ClusteredJoinMethodTest, Registration) {
    auto registry = JoinMethodRegistry::getInstance();
    EXPECT_TRUE(registry->hasMethod("clustered_join"));
}
```

---

## 性能目标

| 指标 | 目标值 | 说明 |
|-----|-------|------|
| Recall@0.8 | ≥ 90% | 含边界处理 |
| 分区均衡度 | > 0.8 | balance_score |
| 查询延迟 | < 3ms | 含边界查询 |
| 重平衡开销 | < 5% | 额外延迟 |

---

## 验收标准

1. ✅ CentroidPartitioner 正确实现
2. ✅ 边界处理策略有效
3. ✅ 召回率达到目标
4. ✅ 负载均衡功能正常
5. ✅ 重平衡机制可用
6. ✅ 所有单元测试通过
7. ✅ 代码符合项目编码规范
8. ✅ 正确实现自动注册

---

## 与 S3J 的关系

ClusteredJoin 和 S3J (D-06) 都使用质心分区，但有以下区别：

| 特性 | ClusteredJoin | S3J |
|-----|---------------|-----|
| 边界处理 | 复制/多查询 | 自适应调整 |
| 重平衡 | 阈值触发 | 持续自适应 |
| 索引 | 分区内 IVF | 分区内多种 |
| 来源 | VectraFlow | DEBS'23 论文 |

---

## 参考资料

### 相关文档

- [GROUP_D_README.md](./README.md) - Group D 总览
- [D04_IVF.md](./D04_IVF.md) - IVF 基础
- [D06_S3J.md](./D06_S3J.md) - S3J 对比
- [C03_PartitionerFactory.md](../group_c/C03_PartitionerFactory.md) - 分区器工厂

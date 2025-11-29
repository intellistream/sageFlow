# C-03: 分区策略自适应选择

**优先级**: 🔴 高  
**预估工时**: 1-2天  
**依赖**: C-02 (JoinStrategyFactory)  
**状态**: ⬜ 待开始

---

## 任务概述

根据 JoinStrategyConfig 动态选择分区器，并集成到连接策略中。不同的 Join Baseline 方法需要不同的分区策略：

| Join 方法 | 分区策略 | 窗口状态 |
|----------|---------|---------|
| BruteForce/IVF/HNSW | RoundRobin | SharedWindowState |
| S3J | CentroidPartitioner | PartitionedWindowState |
| VSJoin | LSHPartitioner | PartitionedVectorState |

---

## 输出文件

| 文件路径 | 描述 |
|---------|------|
| `include/execution/partitioner_factory.h` | 分区器工厂定义 |
| `src/execution/partitioner_factory.cpp` | 分区器工厂实现 |
| `include/execution/lsh_partitioner.h` | LSH 分区器定义 |
| `src/execution/lsh_partitioner.cpp` | LSH 分区器实现 |
| `include/execution/centroid_partitioner.h` | 质心分区器定义 |
| `src/execution/centroid_partitioner.cpp` | 质心分区器实现 |
| `test/UnitTest/test_partitioner_factory.cpp` | 单元测试 |

---

## 接口设计

### PartitionerFactory

```cpp
#pragma once

#include "execution/partitioner.h"
#include "operator/join_strategy_config.h"
#include <memory>

namespace sageFlow {

/**
 * @brief 分区器工厂
 * 
 * 根据配置创建适当的分区器实例
 */
class PartitionerFactory {
public:
    /**
     * @brief 创建分区器
     * @param strategy 分区策略类型
     * @param dimension 向量维度
     * @param num_partitions 分区数量
     * @param config 完整配置（用于获取算法特定参数）
     * @return 分区器实例
     */
    static std::unique_ptr<IPartitioner> create(
        PartitionStrategy strategy,
        int dimension,
        int num_partitions,
        const JoinStrategyConfig& config);
};

} // namespace sageFlow
```

### LSHPartitioner

```cpp
#pragma once

#include "execution/partitioner.h"
#include <vector>
#include <random>

namespace sageFlow {

/**
 * @brief LSH 分区器
 * 
 * 使用局部敏感哈希（Locality Sensitive Hashing）进行分区。
 * 相似的向量有更高概率被分配到相同的分区。
 * 
 * 核心思想：
 * - 生成多个随机投影向量
 * - 计算向量与投影向量的点积
 * - 根据点积符号生成哈希码
 * - 相似向量产生相同哈希码的概率较高
 */
class LSHPartitioner : public IPartitioner {
public:
    /**
     * @brief 构造函数
     * @param dimension 向量维度
     * @param num_hash_functions 哈希函数数量
     * @param num_partitions 分区数量
     * @param seed 随机种子（用于可重复性）
     */
    LSHPartitioner(int dimension, int num_hash_functions, int num_partitions,
                   unsigned seed = 42);
    
    /**
     * @brief 分区函数
     * @param record 待分区的记录
     * @param num_channels 下游通道数
     * @return 分区 ID
     */
    int partition(const Response& record, int num_channels) override;
    
    /**
     * @brief 重置随机投影（使用新种子）
     */
    void reset(unsigned seed);
    
    /**
     * @brief 获取向量的 LSH 签名
     * @param vec 输入向量
     * @return 二进制签名
     */
    std::vector<bool> getSignature(const std::vector<float>& vec) const;

private:
    int dimension_;
    int num_hash_functions_;
    int num_partitions_;
    
    // 随机投影向量
    std::vector<std::vector<float>> random_projections_;
    
    /**
     * @brief 计算 LSH 哈希值
     */
    uint32_t computeLSHHash(const std::vector<float>& vec) const;
    
    /**
     * @brief 初始化随机投影
     */
    void initRandomProjections(unsigned seed);
};

} // namespace sageFlow
```

### CentroidPartitioner

```cpp
#pragma once

#include "execution/partitioner.h"
#include <vector>
#include <shared_mutex>

namespace sageFlow {

/**
 * @brief 质心分区器
 * 
 * 基于 K-means 聚类中心进行分区，用于 S3J 算法。
 * 每个向量被分配到最近的质心对应的分区。
 */
class CentroidPartitioner : public IPartitioner {
public:
    /**
     * @brief 构造函数
     * @param num_centroids 质心数量
     */
    explicit CentroidPartitioner(int num_centroids);
    
    /**
     * @brief 分区函数
     */
    int partition(const Response& record, int num_channels) override;
    
    /**
     * @brief 初始化质心
     * @param samples 样本数据
     */
    void initCentroids(const std::vector<std::vector<float>>& samples);
    
    /**
     * @brief 使用 K-means++ 初始化质心
     */
    void initCentroidsKMeansPP(const std::vector<std::vector<float>>& samples);
    
    /**
     * @brief 更新质心（在线学习）
     */
    void updateCentroids(const std::vector<std::vector<float>>& new_centroids);
    
    /**
     * @brief 获取当前质心
     */
    const std::vector<std::vector<float>>& getCentroids() const;
    
    /**
     * @brief 检查是否已初始化
     */
    bool isInitialized() const;
    
    /**
     * @brief 获取向量到最近质心的距离
     */
    double getDistanceToNearestCentroid(const std::vector<float>& vec) const;

private:
    int num_centroids_;
    std::vector<std::vector<float>> centroids_;
    mutable std::shared_mutex mutex_;
    bool initialized_ = false;
    
    /**
     * @brief 找到最近的质心
     */
    int findNearestCentroid(const std::vector<float>& vec) const;
};

} // namespace sageFlow
```

---

## 实现要点

### 1. PartitionerFactory 实现

```cpp
std::unique_ptr<IPartitioner> PartitionerFactory::create(
    PartitionStrategy strategy,
    int dimension,
    int num_partitions,
    const JoinStrategyConfig& config) {
    
    switch (strategy) {
        case PartitionStrategy::ROUND_ROBIN:
            return std::make_unique<RoundRobinPartitioner>();
            
        case PartitionStrategy::KEY_HASH:
            return std::make_unique<KeyPartitioner>();
            
        case PartitionStrategy::VECTOR_HASH:
            return std::make_unique<VectorHashPartitioner>(dimension);
            
        case PartitionStrategy::LSH:
            return std::make_unique<LSHPartitioner>(
                dimension,
                config.vsjoin_num_hash_functions,
                num_partitions);
                
        case PartitionStrategy::CENTROID:
            return std::make_unique<CentroidPartitioner>(num_partitions);
            
        default:
            throw std::runtime_error("Unknown partition strategy");
    }
}
```

### 2. LSHPartitioner 实现

```cpp
LSHPartitioner::LSHPartitioner(int dimension, int num_hash_functions, 
                               int num_partitions, unsigned seed)
    : dimension_(dimension)
    , num_hash_functions_(num_hash_functions)
    , num_partitions_(num_partitions) {
    initRandomProjections(seed);
}

void LSHPartitioner::initRandomProjections(unsigned seed) {
    std::mt19937 gen(seed);
    std::normal_distribution<float> dist(0.0f, 1.0f);
    
    random_projections_.resize(num_hash_functions_);
    for (auto& proj : random_projections_) {
        proj.resize(dimension_);
        for (float& val : proj) {
            val = dist(gen);
        }
        // 归一化
        float norm = 0.0f;
        for (float val : proj) norm += val * val;
        norm = std::sqrt(norm);
        for (float& val : proj) val /= norm;
    }
}

uint32_t LSHPartitioner::computeLSHHash(const std::vector<float>& vec) const {
    uint32_t hash = 0;
    for (int i = 0; i < num_hash_functions_; ++i) {
        float dot = 0.0f;
        for (int j = 0; j < dimension_; ++j) {
            dot += vec[j] * random_projections_[i][j];
        }
        if (dot > 0) {
            hash |= (1u << i);
        }
    }
    return hash;
}

int LSHPartitioner::partition(const Response& record, int num_channels) {
    if (!record.record_) return 0;
    
    const auto& vec = record.record_->getVector();
    uint32_t hash = computeLSHHash(vec);
    return static_cast<int>(hash % num_partitions_);
}
```

### 3. CentroidPartitioner 实现

```cpp
CentroidPartitioner::CentroidPartitioner(int num_centroids)
    : num_centroids_(num_centroids) {}

void CentroidPartitioner::initCentroidsKMeansPP(
    const std::vector<std::vector<float>>& samples) {
    
    std::unique_lock lock(mutex_);
    
    if (samples.empty()) return;
    
    centroids_.clear();
    centroids_.reserve(num_centroids_);
    
    // K-means++ 初始化
    std::mt19937 gen(42);
    
    // 1. 随机选择第一个质心
    std::uniform_int_distribution<size_t> dist(0, samples.size() - 1);
    centroids_.push_back(samples[dist(gen)]);
    
    // 2. 选择剩余质心
    std::vector<double> min_distances(samples.size(), 
                                      std::numeric_limits<double>::max());
    
    for (int k = 1; k < num_centroids_; ++k) {
        // 更新每个样本到最近质心的距离
        double total_dist = 0.0;
        for (size_t i = 0; i < samples.size(); ++i) {
            double dist = euclideanDistance(samples[i], centroids_.back());
            min_distances[i] = std::min(min_distances[i], dist);
            total_dist += min_distances[i] * min_distances[i];
        }
        
        // 按距离平方概率选择下一个质心
        std::uniform_real_distribution<double> prob_dist(0.0, total_dist);
        double threshold = prob_dist(gen);
        double cumsum = 0.0;
        for (size_t i = 0; i < samples.size(); ++i) {
            cumsum += min_distances[i] * min_distances[i];
            if (cumsum >= threshold) {
                centroids_.push_back(samples[i]);
                break;
            }
        }
    }
    
    initialized_ = true;
}

int CentroidPartitioner::findNearestCentroid(
    const std::vector<float>& vec) const {
    
    int nearest = 0;
    double min_dist = std::numeric_limits<double>::max();
    
    for (size_t i = 0; i < centroids_.size(); ++i) {
        double dist = euclideanDistance(vec, centroids_[i]);
        if (dist < min_dist) {
            min_dist = dist;
            nearest = static_cast<int>(i);
        }
    }
    
    return nearest;
}

int CentroidPartitioner::partition(const Response& record, int num_channels) {
    std::shared_lock lock(mutex_);
    
    if (!initialized_ || !record.record_) return 0;
    
    const auto& vec = record.record_->getVector();
    return findNearestCentroid(vec);
}
```

---

## 修改现有代码

### 修改 PartitionedConnectionStrategy

```cpp
// include/execution/partitioned_connection_strategy.h

class PartitionedConnectionStrategy : public IConnectionStrategy {
public:
    // 新增：设置自定义分区器
    void setPartitioner(std::unique_ptr<IPartitioner> partitioner) {
        custom_partitioner_ = std::move(partitioner);
    }
    
    void setupResultPartition(...) override {
        if (custom_partitioner_) {
            result_partition->setup(std::move(custom_partitioner_), ...);
        } else {
            // 默认 RoundRobin
            result_partition->setup(
                std::make_unique<RoundRobinPartitioner>(), ...);
        }
    }

private:
    std::unique_ptr<IPartitioner> custom_partitioner_;
};
```

---

## 测试要求

```cpp
TEST(PartitionerFactoryTest, CreateRoundRobin) {
    JoinStrategyConfig config;
    auto partitioner = PartitionerFactory::create(
        PartitionStrategy::ROUND_ROBIN, 128, 4, config);
    EXPECT_NE(partitioner, nullptr);
}

TEST(PartitionerFactoryTest, CreateLSH) {
    JoinStrategyConfig config;
    config.vsjoin_num_hash_functions = 8;
    auto partitioner = PartitionerFactory::create(
        PartitionStrategy::LSH, 128, 4, config);
    EXPECT_NE(partitioner, nullptr);
}

TEST(LSHPartitionerTest, LocalityPreservation) {
    LSHPartitioner partitioner(128, 8, 4);
    
    // 创建两个相似向量
    std::vector<float> v1(128, 0.5f);
    std::vector<float> v2(128, 0.5f);
    v2[0] += 0.01f;  // 微小差异
    
    // 相似向量应该有较高概率分到同一分区
    // 测试多次，统计同分区率
    int same_partition_count = 0;
    for (int i = 0; i < 100; ++i) {
        partitioner.reset(i);
        Response r1, r2;
        // ... 设置 record
        if (partitioner.partition(r1, 4) == partitioner.partition(r2, 4)) {
            same_partition_count++;
        }
    }
    EXPECT_GT(same_partition_count, 50);  // 至少 50% 相同
}

TEST(CentroidPartitionerTest, Initialization) {
    CentroidPartitioner partitioner(4);
    EXPECT_FALSE(partitioner.isInitialized());
    
    std::vector<std::vector<float>> samples = {
        {1.0f, 0.0f}, {0.0f, 1.0f}, {-1.0f, 0.0f}, {0.0f, -1.0f}
    };
    partitioner.initCentroidsKMeansPP(samples);
    EXPECT_TRUE(partitioner.isInitialized());
}

TEST(CentroidPartitionerTest, PartitionAssignment) {
    CentroidPartitioner partitioner(2);
    std::vector<std::vector<float>> samples = {
        {1.0f, 0.0f}, {-1.0f, 0.0f}
    };
    partitioner.initCentroids(samples);
    
    // 测试分区分配
    Response r1, r2;
    // ... 设置 record 为 (0.9, 0.1) 和 (-0.9, 0.1)
    
    EXPECT_NE(partitioner.partition(r1, 2), partitioner.partition(r2, 2));
}
```

---

## 验收标准

1. ✅ 不同策略创建正确的分区器
2. ✅ LSH 分区保证相似向量局部性
3. ✅ CentroidPartitioner 支持 K-means++ 初始化
4. ✅ 与现有连接策略兼容
5. ✅ 所有单元测试通过
6. ✅ 代码符合项目编码规范

---

## 参考资料

- [TASK_GROUP_C_INTEGRATION.md](../TASK_GROUP_C_INTEGRATION.md) - 主任务文档
- Locality Sensitive Hashing 论文
- K-means++ 初始化算法

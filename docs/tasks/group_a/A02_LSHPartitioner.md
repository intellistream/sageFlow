# Task A-02: LSHPartitioner 局部敏感哈希分区器

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: 无  
**输出文件**:
- `include/execution/vector_space_partitioner.h`
- `src/execution/vector_space_partitioner.cpp`
- `test/UnitTest/test_vector_space_partitioner.cpp`

---

## 任务描述

实现基于局部敏感哈希（LSH）的向量空间分区器，确保相似向量大概率被分配到同一分区。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 LSHPartitioner 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护

## 背景
当前 VectorHashPartitioner 仅使用向量前8维的简单哈希，无法保证相似向量的局部性。
VSJoin 需要基于向量空间的分区策略，使相似向量大概率分配到同一分区。

## 任务目标
实现基于随机投影的 LSH 分区器：
1. 使用多个随机超平面将向量空间划分
2. 相似向量具有高概率获得相同的哈希码
3. 支持查询时返回候选分区列表

## 文件位置
- 头文件: include/execution/vector_space_partitioner.h
- 实现文件: src/execution/vector_space_partitioner.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include <vector>
#include <random>
#include <cstdint>

namespace sageFlow {

/**
 * @brief 向量空间分区器基类
 */
class VectorSpacePartitioner {
public:
    virtual ~VectorSpacePartitioner() = default;
    
    /**
     * @brief 计算向量所属分区
     * @param record 向量记录
     * @param num_partitions 分区总数
     * @return 分区ID
     */
    virtual size_t partition(const VectorRecord& record, size_t num_partitions) = 0;
    
    /**
     * @brief 获取查询时需要检查的候选分区（包含邻近分区）
     * @param query 查询向量
     * @param num_partitions 分区总数
     * @param num_probes 探测数量（1=仅主分区）
     * @return 候选分区列表
     */
    virtual std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) = 0;
    
    /**
     * @brief 判断向量是否靠近分区边界
     * @param record 向量记录
     * @param num_partitions 分区总数
     * @return 是否为边界向量
     */
    virtual bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) = 0;
};

/**
 * @brief 基于局部敏感哈希的分区器
 * 
 * 使用随机超平面将向量空间划分，相似向量有高概率获得相同哈希码。
 * 适用于欧氏距离和角距离场景。
 */
class LSHPartitioner : public VectorSpacePartitioner {
public:
    /**
     * @brief 构造函数
     * @param dimension 向量维度
     * @param num_hash_functions 哈希函数数量（影响分区粒度）
     * @param seed 随机种子
     * @param boundary_threshold 边界判定阈值（与超平面距离的比例）
     */
    LSHPartitioner(int dimension, int num_hash_functions = 8, 
                   int seed = 42, double boundary_threshold = 0.1);
    
    size_t partition(const VectorRecord& record, size_t num_partitions) override;
    
    std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) override;
    
    bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;
    
    /**
     * @brief 获取向量的原始 LSH 哈希码（用于调试）
     */
    uint64_t getHashCode(const VectorRecord& record) const;

private:
    int dimension_;
    int num_hash_functions_;
    double boundary_threshold_;
    
    // 随机投影向量 (num_hash_functions x dimension)
    std::vector<std::vector<float>> random_projections_;
    
    /**
     * @brief 计算 LSH 哈希码
     * @param record 向量记录
     * @return 二进制哈希码
     */
    uint64_t computeHashCode(const VectorRecord& record) const;
    
    /**
     * @brief 计算向量到各超平面的有符号距离
     * @param record 向量记录
     * @return 各超平面的距离（正=超平面一侧，负=另一侧）
     */
    std::vector<float> computeDistancesToHyperplanes(const VectorRecord& record) const;
    
    /**
     * @brief 初始化随机投影向量
     * @param seed 随机种子
     */
    void initRandomProjections(int seed);
};

/**
 * @brief 基于 K-Means 的分区器（备选方案）
 */
class KMeansPartitioner : public VectorSpacePartitioner {
public:
    KMeansPartitioner(int dimension, int num_clusters, int seed = 42);
    
    /**
     * @brief 使用样本数据初始化质心
     * @param samples 样本向量
     * @param max_iterations 最大迭代次数
     */
    void initCentroids(const std::vector<const VectorRecord*>& samples, 
                       int max_iterations = 100);
    
    /**
     * @brief 在线更新质心（增量 K-Means）
     * @param record 新向量
     * @param learning_rate 学习率
     */
    void updateCentroids(const VectorRecord& record, double learning_rate = 0.01);
    
    size_t partition(const VectorRecord& record, size_t num_partitions) override;
    std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) override;
    bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;

private:
    int dimension_;
    int num_clusters_;
    std::vector<std::vector<float>> centroids_;
    
    size_t findNearestCentroid(const VectorRecord& record) const;
};

} // namespace sageFlow
```

## 实现要点

1. **initRandomProjections()**:
   - 使用标准正态分布初始化 num_hash_functions 个随机向量
   - 每个向量维度为 dimension
   - 归一化为单位向量

2. **computeHashCode()**:
   - 对每个投影向量计算与输入向量的点积
   - 点积 > 0 则对应位为 1，否则为 0
   - 组合成 uint64_t 哈希码

3. **partition()**:
   - hashCode % num_partitions

4. **getCandidatePartitions()**:
   - 返回主分区
   - 如果 num_probes > 1，翻转距离超平面最近的 bit 位，获取邻近分区
   - 使用 computeDistancesToHyperplanes() 确定哪些 bit 最容易翻转

5. **isBoundaryVector()**:
   - 检查是否有任何超平面距离小于 boundary_threshold * 向量模长
   - 距离小说明向量靠近分区边界

## 参考资料
- 现有分区器: include/execution/partitioner.h
- LSH 理论: Locality-Sensitive Hashing Scheme Based on p-Stable Distributions

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "execution/vector_space_partitioner.h"

class LSHPartitionerTest : public ::testing::Test {
protected:
    void SetUp() override {
        partitioner_ = std::make_unique<LSHPartitioner>(128, 8, 42);
    }
    std::unique_ptr<LSHPartitioner> partitioner_;
};

// 一致性测试
TEST_F(LSHPartitionerTest, SameVectorSamePartition) {
    // 相同向量应该分配到相同分区
}

// 局部性测试
TEST_F(LSHPartitionerTest, SimilarVectorsSamePartitionHighProbability) {
    // 相似向量有高概率分配到同一分区（统计测试）
    // 生成100对相似向量，检查同分区比例 > 70%
}

// 候选分区测试
TEST_F(LSHPartitionerTest, CandidatePartitionsIncludesMainPartition) {
    // getCandidatePartitions 结果应包含主分区
}

TEST_F(LSHPartitionerTest, MoreProbesMeansMoreCandidates) {
    // num_probes 增加时，候选分区数应增加
}

// 边界向量测试
TEST_F(LSHPartitionerTest, BoundaryVectorDetection) {
    // 构造靠近超平面的向量，验证被标记为边界向量
}
```

## 验收标准
1. 所有单元测试通过
2. 相似向量同分区率 > 70%（在测试数据集上）
3. 代码通过 clang-tidy 检查
```

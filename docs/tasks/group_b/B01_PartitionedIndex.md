# Task B-01: PartitionedIndex 分区索引

**状态**: ✅ 已完成  
**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: A-02 (LSHPartitioner) ✅ 已完成  
**输出文件**:
- `include/index/partitioned_index.h` ✅
- `src/index/partitioned_index.cpp` ✅
- `test/UnitTest/test_partitioned_index.cpp` ✅

---

## 任务描述

实现分区索引结构，每个向量空间分区维护独立的 IVF 索引，支持分区级别的插入/删除/查询。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionedIndex 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase (如 PartitionedIndex)
- 方法名: camelBack (如 insertToPartition, queryPartition)
- 成员变量: lower_case_ 带尾部下划线 (如 partition_indexes_, num_partitions_)
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
当前索引是全局共享的，所有线程竞争同一个索引。
分区索引让每个分区拥有独立的索引，减少锁竞争，提高并行效率。

## 任务目标
实现分区索引：
1. 每个分区维护独立的 IVF 索引
2. 支持分区级别的插入/删除/查询
3. 支持跨分区查询

## 依赖
- LSHPartitioner (A-02): 用于确定向量所属分区，已在 include/execution/vector_space_partitioner.h 中实现

## 文件位置
- 头文件: include/index/partitioned_index.h
- 实现文件: src/index/partitioned_index.cpp

## 接口要求

```cpp
#pragma once

#include "index/index.h"
#include "index/ivf.h"
#include "execution/vector_space_partitioner.h"
#include <vector>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace sageFlow {

/**
 * @brief 分区索引
 * 
 * 将向量空间分区，每个分区维护独立的 IVF 索引。
 * 支持分区级别的并发操作，减少全局锁竞争。
 */
class PartitionedIndex : public Index {
public:
    /**
     * @brief 构造函数
     * @param num_partitions 分区数量
     * @param dimension 向量维度
     * @param partitioner 向量空间分区器
     * @param nlist 每个分区 IVF 的聚类数
     * @param nprobes 查询时探测的聚类数
     */
    PartitionedIndex(size_t num_partitions, int dimension,
                     std::shared_ptr<VectorSpacePartitioner> partitioner,
                     int nlist = 100, int nprobes = 10);
    
    // Index 接口实现
    int insert(std::unique_ptr<VectorRecord> record) override;
    bool erase(uint64_t uid) override;
    
    std::vector<std::shared_ptr<const VectorRecord>> 
        query(const VectorRecord& query, int k) override;
    
    std::vector<std::shared_ptr<const VectorRecord>>
        queryForJoin(const VectorRecord& query, double threshold) override;
    
    size_t size() const override;
    
    // 分区特定操作
    
    /**
     * @brief 插入到指定分区
     * @param partition_id 分区ID
     * @param record 向量记录
     * @return 成功返回1，失败返回-1
     */
    int insertToPartition(size_t partition_id, std::unique_ptr<VectorRecord> record);
    
    /**
     * @brief 查询指定分区
     * @param partition_id 分区ID
     * @param query 查询向量
     * @param k 返回数量
     * @return 查询结果
     */
    std::vector<std::shared_ptr<const VectorRecord>>
        queryPartition(size_t partition_id, const VectorRecord& query, int k);
    
    /**
     * @brief 跨分区查询
     * @param query 查询向量
     * @param k 返回数量
     * @param num_probes 探测分区数
     * @return 合并去重的结果
     */
    std::vector<std::shared_ptr<const VectorRecord>>
        queryMultiPartition(const VectorRecord& query, int k, size_t num_probes = 2);
    
    /**
     * @brief 获取分区数量
     */
    size_t getNumPartitions() const { return num_partitions_; }
    
    /**
     * @brief 获取分区大小
     */
    size_t getPartitionSize(size_t partition_id) const;
    
    /**
     * @brief 获取分区负载统计
     */
    std::vector<size_t> getPartitionSizes() const;

private:
    size_t num_partitions_;
    int dimension_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    int nlist_;
    int nprobes_;
    
    // 每个分区的索引
    std::vector<std::unique_ptr<Ivf>> partition_indexes_;
    
    // uid -> partition_id 映射，用于删除时定位分区
    std::unordered_map<uint64_t, size_t> uid_partition_map_;
    mutable std::shared_mutex map_mutex_;
    
    // 分区级别的锁
    std::vector<std::unique_ptr<std::shared_mutex>> partition_mutexes_;
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 创建 num_partitions 个独立的 Ivf 实例
   - 每个分区使用相同的 nlist 和 nprobes
   - 初始化分区锁

2. **insert()**:
   - 使用 partitioner_->partition() 确定分区
   - 获取分区写锁
   - 更新 uid_partition_map_
   - 调用对应分区索引的 insert

3. **erase()**:
   - 从 uid_partition_map_ 查找分区
   - 获取分区写锁
   - 调用对应分区的 erase
   - 移除映射

4. **queryMultiPartition()**:
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 获取各分区读锁
   - 并行查询多个分区
   - 合并去重结果，按距离排序取 top-k

5. **线程安全**:
   - uid_partition_map_ 使用 map_mutex_ 保护
   - 每个分区索引使用独立的 partition_mutexes_[i] 保护
   - 读操作使用 shared_lock，写操作使用 unique_lock

## 参考文件
- include/index/index.h (接口定义)
- include/index/ivf.h (IVF 索引实现)
- include/execution/vector_space_partitioner.h (分区器)

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "index/partitioned_index.h"
#include "execution/vector_space_partitioner.h"

class PartitionedIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        partitioner_ = std::make_shared<LSHPartitioner>(128, 8, 42);
        index_ = std::make_unique<PartitionedIndex>(
            4, 128, partitioner_, 10, 2);
    }
    
    std::shared_ptr<LSHPartitioner> partitioner_;
    std::unique_ptr<PartitionedIndex> index_;
    
    std::unique_ptr<VectorRecord> createRandomRecord(uint64_t uid);
};

// 基础功能测试
TEST_F(PartitionedIndexTest, InsertRouting) {
    // 测试插入路由到正确分区
}

TEST_F(PartitionedIndexTest, InsertAndQuery) {
    // 测试插入后能正确查询
}

TEST_F(PartitionedIndexTest, EraseCorrectness) {
    // 测试删除操作正确性
}

TEST_F(PartitionedIndexTest, EraseNonExistent) {
    // 测试删除不存在的记录
}

// 分区查询测试
TEST_F(PartitionedIndexTest, SinglePartitionQuery) {
    // 测试单分区查询
}

TEST_F(PartitionedIndexTest, MultiPartitionQuery) {
    // 测试跨分区查询召回率
}

TEST_F(PartitionedIndexTest, QueryForJoin) {
    // 测试阈值查询
}

// 并发测试
TEST_F(PartitionedIndexTest, ConcurrentInsert) {
    // 测试并发插入
}

TEST_F(PartitionedIndexTest, ConcurrentQueryAndInsert) {
    // 测试并发查询和插入
}

// 负载统计测试
TEST_F(PartitionedIndexTest, PartitionSizes) {
    // 测试分区大小统计
}

TEST_F(PartitionedIndexTest, LoadBalance) {
    // 测试分区负载均衡
}
```

## 验收标准
1. 所有单元测试通过
2. 跨分区查询召回率 > 95%
3. 并发测试无数据竞争
4. 代码通过 clang-tidy 检查
```

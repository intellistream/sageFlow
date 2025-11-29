# Group C: Baseline 实现

本文档包含 VSJoin 论文中相关工作的 Baseline 实现任务。

---

## D-01: BruteForce Baseline (Ground Truth)

**优先级**: 🟡 中  
**预估工时**: 1-2 天  
**依赖**: 无  
**输出文件**:
- `include/operator/join_operator_methods/bruteforce_join_method.h`
- `src/operator/join_operator_methods/bruteforce_join_method.cpp`
- `test/UnitTest/test_bruteforce_join_method.cpp`
- `config/baseline_bruteforce.toml`

### 任务描述

实现 BruteForce Join 方法，作为绝对准确性基准（Ground Truth）。使用时间分区 + 暴力扫描策略。此方法用于验证其他近似算法的召回率。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 BruteForce Baseline。

## 背景
需要一个绝对准确的基准方法来验证其他近似算法的召回率。
- 仅按时间分区（滑动窗口）
- 使用暴力扫描进行相似度匹配
- 理论召回率 100%

## 任务目标
实现 BruteForceJoinMethod 类。

## 文件位置
- 头文件: include/operator/join_operator_methods/bruteforce_join_method.h
- 实现文件: src/operator/join_operator_methods/bruteforce_join_method.cpp

## 接口要求

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"
#include <deque>
#include <memory>

namespace sageFlow {

/**
 * @brief BruteForce Baseline 实现
 * 
 * 使用时间滑动窗口和暴力扫描。
 * 作为 Ground Truth 使用。
 */
class BruteForceJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值
     * @param window_size 窗口大小（毫秒）
     */
    BruteForceJoinMethod(double threshold, int64_t window_size);
    
    // BaseMethod 接口实现
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, int slot) override;
    
    void updateState(std::unique_ptr<VectorRecord> record, int slot) override;
    
    void evictExpired(int64_t current_timestamp) override;
    
    /**
     * @brief 获取窗口内的记录数
     */
    size_t getLeftWindowSize() const;
    size_t getRightWindowSize() const;

private:
    double threshold_;
    int64_t window_size_;
    
    // 左右流的滑动窗口
    std::deque<std::unique_ptr<VectorRecord>> left_window_;
    std::deque<std::unique_ptr<VectorRecord>> right_window_;
    
    /**
     * @brief 暴力扫描匹配
     * @param query 查询向量
     * @param window 目标窗口
     * @return 匹配结果
     */
    std::vector<std::unique_ptr<VectorRecord>> bruteForceMatch(
        const VectorRecord& query,
        const std::deque<std::unique_ptr<VectorRecord>>& window);
};

} // namespace sageFlow
```

## 实现要点

1. **ExecuteEager()**:
   - 对于左流记录（slot=0），在右窗口暴力扫描
   - 对于右流记录（slot=1），在左窗口暴力扫描
   - 计算余弦相似度，返回超过阈值的记录

2. **evictExpired()**:
   - 从左右窗口移除过期记录
   - 使用 VectorRecord::getTimestamp() 判断

3. **bruteForceMatch()**:
   ```cpp
   std::vector<std::unique_ptr<VectorRecord>> bruteForceMatch(
       const VectorRecord& query,
       const std::deque<std::unique_ptr<VectorRecord>>& window) {
       
       std::vector<std::unique_ptr<VectorRecord>> results;
       for (const auto& record : window) {
           double similarity = CosineSimilarity(query.getVector(), 
                                                record->getVector());
           if (similarity >= threshold_) {
               results.push_back(record->clone());
           }
       }
       return results;
   }
   ```

## 配置文件

```toml
# config/baseline_bruteforce.toml
[baseline.bruteforce]
threshold = 0.8
window_size = 10000  # 10 seconds

[test]
left_stream_size = 10000
right_stream_size = 10000
dimension = 128
```

## 测试要求

```cpp
TEST(BruteForceJoinMethodTest, EagerExecution) {
    // 测试即时执行模式
}

TEST(BruteForceJoinMethodTest, WindowEviction) {
    // 测试窗口过期清理
}

TEST(BruteForceJoinMethodTest, CorrectnessVerification) {
    // 验证结果正确性
}
```

## 验收标准
1. 所有单元测试通过
2. 结果正确性 100%
```

---

## D-02: HDR-Tree Baseline

**优先级**: 🟡 中  
**预估工时**: 3-4 天  
**依赖**: A-06 (PCA)  
**输出文件**:
- `include/index/hdr_tree.h`
- `src/index/hdr_tree.cpp`
- `include/operator/join_operator_methods/hdr_tree_join_method.h`
- `src/operator/join_operator_methods/hdr_tree_join_method.cpp`
- `test/UnitTest/test_hdr_tree.cpp`
- `config/baseline_hdr_tree.toml`

### 任务描述

实现 HDR-Tree (High-Dimensional Range Tree) Baseline。
*注：Roadmap 中引用的是 ICDM 2014 的版本，但本任务描述采用了更适合流式场景的优化版本（参考 ADC 2022），包含延迟更新和批量操作优化。*

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 HDR-Tree Baseline。

## 背景
HDR-Tree 是一种面向增量更新场景的向量索引：
- 使用 PCA 将高维向量投影到低维
- 使用类 R-Tree 结构进行空间划分
- 专门针对插入/删除场景优化:
  1. 延迟更新机制: 使用 delta buffer 暂存更新，批量合并到主树
  2. 批量 R-Tree 操作: 批量插入/批量删除，减少树结构调整次数
  3. 优化的删除策略: 标记删除 + 延迟重建，避免频繁 rebalance

## 任务目标
实现 HDRTree 索引和对应的 Join 方法。

## 文件位置
- 索引头文件: include/index/hdr_tree.h
- 索引实现文件: src/index/hdr_tree.cpp
- Join 方法头文件: include/operator/join_operator_methods/hdr_tree_join_method.h
- Join 方法实现文件: src/operator/join_operator_methods/hdr_tree_join_method.cpp

## HDRTree 接口要求

```cpp
#pragma once

#include "index/index.h"
#include "utils/pca.h"
#include <vector>
#include <memory>
#include <unordered_set>

namespace sageFlow {

/**
 * @brief HDR-Tree 节点
 */
struct HDRTreeNode {
    bool is_leaf;
    std::vector<float> mbb_min;  // Minimum bounding box - lower bounds
    std::vector<float> mbb_max;  // Minimum bounding box - upper bounds
    
    // 叶子节点
    std::vector<std::shared_ptr<const VectorRecord>> records;
    
    // 内部节点
    std::vector<std::unique_ptr<HDRTreeNode>> children;
};

/**
 * @brief Delta Buffer 条目
 */
struct DeltaEntry {
    enum class Type { INSERT, DELETE };
    Type type;
    std::unique_ptr<VectorRecord> record;  // for INSERT
    uint64_t uid;                          // for DELETE
};

/**
 * @brief HDR-Tree 索引
 * 
 * 面向增量更新场景的高维向量索引。
 * 使用 PCA 降维 + R-Tree 空间划分 + 延迟更新优化。
 */
class HDRTree : public Index {
public:
    /**
     * @brief 构造函数
     * @param original_dim 原始向量维度
     * @param projected_dim 投影后维度
     * @param max_node_size 叶节点最大记录数
     * @param min_node_size 叶节点最小记录数 (分裂时使用)
     * @param delta_buffer_size Delta buffer 大小阈值（触发合并）
     * @param lazy_deletion_threshold 延迟删除阈值（删除标记数占比）
     */
    HDRTree(int original_dim, int projected_dim = 8,
            int max_node_size = 100, int min_node_size = 40,
            size_t delta_buffer_size = 1000,
            double lazy_deletion_threshold = 0.3);
    
    // Index 接口实现
    int insert(std::unique_ptr<VectorRecord> record) override;
    bool erase(uint64_t uid) override;
    std::vector<std::shared_ptr<const VectorRecord>> 
        query(const VectorRecord& query, int k) override;
    std::vector<std::shared_ptr<const VectorRecord>>
        queryForJoin(const VectorRecord& query, double threshold) override;
    size_t size() const override;
    
    /**
     * @brief 训练 PCA 投影矩阵
     * @param training_data 训练数据
     */
    void trainPCA(const std::vector<std::vector<float>>& training_data);
    
    /**
     * @brief 强制刷新 delta buffer
     */
    void flushDeltaBuffer();
    
    /**
     * @brief 强制重建（清理删除标记）
     */
    void rebuild();
    
    /**
     * @brief 获取统计信息
     */
    struct Stats {
        size_t total_records;
        size_t deleted_marks;     // 删除标记数
        size_t delta_buffer_size; // 当前 buffer 大小
        size_t tree_depth;
        size_t flush_count;       // 刷新次数
        size_t rebuild_count;     // 重建次数
    };
    Stats getStats() const;

private:
    int original_dim_;
    int projected_dim_;
    int max_node_size_;
    int min_node_size_;
    size_t delta_buffer_threshold_;
    double lazy_deletion_threshold_;
    
    std::unique_ptr<PCA> pca_;
    bool pca_trained_;
    
    std::unique_ptr<HDRTreeNode> root_;
    
    // Delta buffer (延迟更新)
    std::vector<DeltaEntry> delta_buffer_;
    mutable std::mutex delta_mutex_;
    
    // 删除标记集合
    std::unordered_set<uint64_t> deleted_uids_;
    mutable std::shared_mutex delete_mutex_;
    
    // uid -> record 映射
    std::unordered_map<uint64_t, std::shared_ptr<const VectorRecord>> record_map_;
    
    /**
     * @brief 投影向量到低维
     */
    std::vector<float> projectVector(const std::vector<float>& vec) const;
    
    /**
     * @brief 批量插入到树
     */
    void bulkInsert(std::vector<std::unique_ptr<VectorRecord>>& records);
    
    /**
     * @brief 批量删除（应用删除标记）
     */
    void bulkDelete(const std::vector<uint64_t>& uids);
    
    /**
     * @brief 检查是否需要刷新 buffer
     */
    void checkAndFlush();
    
    /**
     * @brief 检查是否需要重建
     */
    void checkAndRebuild();
    
    /**
     * @brief 范围查询（内部实现）
     */
    void rangeQuery(const HDRTreeNode* node,
                    const std::vector<float>& query_proj,
                    double radius,
                    std::vector<std::shared_ptr<const VectorRecord>>& results) const;
};

} // namespace sageFlow
```

## HDRTreeJoinMethod 接口要求

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "index/hdr_tree.h"
#include <memory>

namespace sageFlow {

/**
 * @brief HDR-Tree Join 方法
 */
class HDRTreeJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值
     * @param original_dim 原始向量维度
     * @param projected_dim 投影维度
     */
    HDRTreeJoinMethod(double threshold, int original_dim, int projected_dim = 8);
    
    // BaseMethod 接口
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, int slot) override;
    
    void updateState(std::unique_ptr<VectorRecord> record, int slot) override;
    void evictExpired(int64_t current_timestamp) override;
    
    /**
     * @brief 训练 PCA（需要在处理前调用）
     */
    void trainPCA(const std::vector<std::vector<float>>& training_data);

private:
    double threshold_;
    std::unique_ptr<HDRTree> left_index_;
    std::unique_ptr<HDRTree> right_index_;
};

} // namespace sageFlow
```

## 实现要点

1. **延迟更新**:
   - insert() 先加入 delta_buffer_
   - 当 buffer 达到阈值时，调用 bulkInsert()
   - bulkInsert() 使用 R-Tree 的批量插入算法

2. **延迟删除**:
   - erase() 只标记 deleted_uids_
   - 查询时过滤删除标记
   - 当删除标记占比超过阈值时触发 rebuild()

3. **范围查询**:
   - 先投影查询向量
   - 使用投影空间的距离作为下界剪枝
   - 对候选结果在原始空间验证

## 配置文件

```toml
# config/baseline_hdr_tree.toml
[baseline.hdr_tree]
projected_dim = 8
max_node_size = 100
min_node_size = 40
delta_buffer_size = 1000
lazy_deletion_threshold = 0.3

[test]
threshold = 0.8
dimension = 128
```

## 测试要求

```cpp
TEST(HDRTreeTest, PCAProjection) {
    // 测试 PCA 投影正确性
}

TEST(HDRTreeTest, LazyDeletion) {
    // 测试延迟删除机制
}

TEST(HDRTreeTest, DeltaBufferFlush) {
    // 测试 delta buffer 刷新
}

TEST(HDRTreeTest, RangeQuery) {
    // 测试范围查询正确性
}

TEST(HDRTreeTest, IncrementalUpdate) {
    // 测试增量更新场景性能
}
```

## 验收标准
1. 所有单元测试通过
2. 召回率 > 95%
3. 增量更新性能优于简单 rebuild
```

---

## D-03: HNSW Enhanced Baseline

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/operator/join_operator_methods/hnsw_join_method.h`
- `src/operator/join_operator_methods/hnsw_join_method.cpp`
- `test/UnitTest/test_hnsw_join_method.cpp`
- `config/baseline_hnsw.toml`

### 任务描述

基于现有 HNSW 索引实现 Join 方法封装，作为 Baseline B3。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 HNSW Join Method Baseline。

## 背景
HNSW 是目前最流行的近似最近邻索引之一，作为 VSJoin 的重要 Baseline。

## 任务目标
封装现有 HNSW 索引，实现 BaseMethod 接口。

## 文件位置
- 头文件: include/operator/join_operator_methods/hnsw_join_method.h
- 实现文件: src/operator/join_operator_methods/hnsw_join_method.cpp

## 接口要求

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "index/hnsw.h"
#include "concurrency/concurrency_manager.h"
#include <memory>

namespace sageFlow {

/**
 * @brief HNSW Join 方法 (Baseline B3)
 */
class HNSWJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值
     * @param dimension 向量维度
     * @param m HNSW 的 M 参数
     * @param ef_construction 构建时的 ef
     * @param ef_search 查询时的 ef
     */
    HNSWJoinMethod(double threshold, int dimension,
                   int m = 16, int ef_construction = 200, int ef_search = 50);
    
    // BaseMethod 接口
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, int slot) override;
    
    void updateState(std::unique_ptr<VectorRecord> record, int slot) override;
    void evictExpired(int64_t current_timestamp) override;
    
    /**
     * @brief 设置查询时的 ef 参数
     */
    void setEfSearch(int ef_search);
    
    /**
     * @brief 获取索引大小
     */
    size_t getLeftIndexSize() const;
    size_t getRightIndexSize() const;

private:
    double threshold_;
    int dimension_;
    
    // 使用 ConcurrencyManager 管理索引
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    int left_index_id_;
    int right_index_id_;
    
    // uid -> timestamp 映射，用于过期清理
    std::unordered_map<uint64_t, int64_t> left_timestamps_;
    std::unordered_map<uint64_t, int64_t> right_timestamps_;
    mutable std::shared_mutex timestamp_mutex_;
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 创建 ConcurrencyManager
   - 创建左右两个 HNSW 索引

2. **ExecuteEager()**:
   - 使用 concurrency_manager_->query_for_join() 查询
   - 阈值过滤

3. **evictExpired()**:
   - 遍历 timestamps_ 找到过期记录
   - 调用 concurrency_manager_->erase() 删除
   - 注意：HNSW 删除可能影响性能，记录统计

## 配置文件

```toml
# config/baseline_hnsw.toml
[baseline.hnsw]
threshold = 0.8
m = 16
ef_construction = 200
ef_search = 50

[test]
dimension = 128
```

## 测试要求

```cpp
TEST(HNSWJoinMethodTest, BasicFunctionality) {
    // 测试基本功能
}

TEST(HNSWJoinMethodTest, RecallRate) {
    // 测试召回率
}

TEST(HNSWJoinMethodTest, Deletion) {
    // 测试删除后的性能和正确性
}
```

## 验收标准
1. 所有单元测试通过
2. 召回率 > 95%
3. 性能数据记录
```

---

## D-04: IVF Enhanced Baseline

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**输出文件**:
- `include/operator/join_operator_methods/ivf_join_method.h`
- `src/operator/join_operator_methods/ivf_join_method.cpp`
- `test/UnitTest/test_ivf_join_method.cpp`
- `config/baseline_ivf.toml`

### 任务描述

基于现有 IVF 索引实现 Join 方法封装，作为 Baseline B4。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 IVF Join Method Baseline。

## 背景
IVF 是经典的近似最近邻索引，适合大规模向量检索。

## 任务目标
封装现有 IVF 索引，实现 BaseMethod 接口。

## 文件位置
- 头文件: include/operator/join_operator_methods/ivf_join_method.h
- 实现文件: src/operator/join_operator_methods/ivf_join_method.cpp

## 接口要求

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "index/ivf.h"
#include "concurrency/concurrency_manager.h"
#include <memory>

namespace sageFlow {

/**
 * @brief IVF Join 方法 (Baseline B4)
 */
class IVFJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值
     * @param dimension 向量维度
     * @param nlist 聚类数
     * @param nprobes 查询时探测的聚类数
     * @param rebuild_threshold 重建阈值
     */
    IVFJoinMethod(double threshold, int dimension,
                  int nlist = 100, int nprobes = 10,
                  double rebuild_threshold = 0.3);
    
    // BaseMethod 接口
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, int slot) override;
    
    void updateState(std::unique_ptr<VectorRecord> record, int slot) override;
    void evictExpired(int64_t current_timestamp) override;
    
    /**
     * @brief 设置 nprobes 参数
     */
    void setNprobes(int nprobes);
    
    /**
     * @brief 强制重新训练聚类
     */
    void retrain();

private:
    double threshold_;
    int dimension_;
    int nlist_;
    int nprobes_;
    
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    int left_index_id_;
    int right_index_id_;
    
    std::unordered_map<uint64_t, int64_t> left_timestamps_;
    std::unordered_map<uint64_t, int64_t> right_timestamps_;
    mutable std::shared_mutex timestamp_mutex_;
};

} // namespace sageFlow
```

## 实现要点

类似 D-03，但使用 IVF 索引。

## 配置文件

```toml
# config/baseline_ivf.toml
[baseline.ivf]
threshold = 0.8
nlist = 100
nprobes = 10
rebuild_threshold = 0.3

[test]
dimension = 128
```

## 测试要求

类似 D-03。

## 验收标准
1. 所有单元测试通过
2. 召回率 > 90%
3. 性能数据记录
```

---

## D-05: ClusteredJoin VectraFlow Baseline

**优先级**: 🟢 低  
**预估工时**: 3-4 天  
**依赖**: 无  
**输出文件**:
- `include/operator/join_operator_methods/clustered_join_method.h`
- `src/operator/join_operator_methods/clustered_join_method.cpp`
- `test/UnitTest/test_clustered_join_method.cpp`
- `config/baseline_clustered_join.toml`

### 任务描述

实现 VectraFlow 的 ClusteredJoin 算法，作为 Baseline B5。这是一种基于聚类的流式向量连接方法。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现 ClusteredJoin Baseline。

## 背景
VectraFlow 论文提出的 ClusteredJoin 方法：
- 使用在线 k-means 对向量进行聚类
- 只在相似聚类之间进行连接
- 维护聚类中心的索引用于快速路由

## 任务目标
实现 ClusteredJoinMethod 类。

## 文件位置
- 头文件: include/operator/join_operator_methods/clustered_join_method.h
- 实现文件: src/operator/join_operator_methods/clustered_join_method.cpp

## 接口要求

```cpp
#pragma once

#include "operator/join_operator_methods/base_method.h"
#include <vector>
#include <memory>
#include <unordered_map>

namespace sageFlow {

/**
 * @brief 聚类信息
 */
struct Cluster {
    size_t id;
    std::vector<float> centroid;           // 聚类中心
    size_t count;                          // 成员数量
    std::vector<float> sum;                // 向量和（用于增量更新中心）
    std::deque<std::unique_ptr<VectorRecord>> members; // 成员记录
};

/**
 * @brief ClusteredJoin 方法 (VectraFlow Baseline)
 */
class ClusteredJoinMethod : public BaseMethod {
public:
    /**
     * @brief 构造函数
     * @param threshold 相似度阈值
     * @param dimension 向量维度
     * @param num_clusters 聚类数量
     * @param centroid_threshold 聚类中心相似度阈值
     */
    ClusteredJoinMethod(double threshold, int dimension,
                        int num_clusters = 16,
                        double centroid_threshold = 0.7);
    
    // BaseMethod 接口
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query, int slot) override;
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
        const std::deque<std::unique_ptr<VectorRecord>>& queries, int slot) override;
    
    void updateState(std::unique_ptr<VectorRecord> record, int slot) override;
    void evictExpired(int64_t current_timestamp) override;
    
    /**
     * @brief 获取聚类统计
     */
    struct ClusterStats {
        int num_clusters;
        size_t min_size;
        size_t max_size;
        double avg_size;
    };
    ClusterStats getLeftClusterStats() const;
    ClusterStats getRightClusterStats() const;

private:
    double threshold_;
    int dimension_;
    int num_clusters_;
    double centroid_threshold_;
    
    // 左右流的聚类
    std::vector<Cluster> left_clusters_;
    std::vector<Cluster> right_clusters_;
    mutable std::shared_mutex cluster_mutex_;
    
    // 聚类 ID 计数器
    size_t next_cluster_id_{0};
    
    /**
     * @brief 找到最近的聚类
     */
    size_t findNearestCluster(const std::vector<float>& vec,
                               const std::vector<Cluster>& clusters);
    
    /**
     * @brief 更新聚类中心（在线 k-means）
     */
    void updateCentroid(Cluster& cluster, const std::vector<float>& vec);
    
    /**
     * @brief 查找相似的聚类对
     */
    std::vector<std::pair<size_t, size_t>> findSimilarClusterPairs(
        const std::vector<Cluster>& clusters1,
        const std::vector<Cluster>& clusters2);
    
    /**
     * @brief 在聚类内搜索匹配
     */
    std::vector<std::unique_ptr<VectorRecord>> searchInCluster(
        const VectorRecord& query,
        const Cluster& cluster);
};

} // namespace sageFlow
```

## 实现要点

1. **updateState()**:
   - 找到最近聚类
   - 如果距离太远，创建新聚类
   - 更新聚类中心（增量式）

2. **ExecuteEager()**:
   - 找到查询所属聚类
   - 找到对侧相似的聚类
   - 在相似聚类中搜索

3. **evictExpired()**:
   - 从聚类成员列表中移除过期记录
   - 更新聚类中心
   - 如果聚类为空，考虑删除

## 配置文件

```toml
# config/baseline_clustered_join.toml
[baseline.clustered_join]
threshold = 0.8
num_clusters = 16
centroid_threshold = 0.7

[test]
dimension = 128
```

## 测试要求

```cpp
TEST(ClusteredJoinMethodTest, ClusterFormation) {
    // 测试聚类形成
}

TEST(ClusteredJoinMethodTest, OnlineUpdate) {
    // 测试在线更新
}

TEST(ClusteredJoinMethodTest, CrossClusterJoin) {
    // 测试跨聚类连接
}

TEST(ClusteredJoinMethodTest, RecallRate) {
    // 测试召回率
}
```

## 验收标准
1. 所有单元测试通过
2. 召回率 > 90%
3. 聚类负载相对均衡
```

---

## D-06: S3J/DEBS'23 Baseline (Full Implementation)

**优先级**: 🟡 中  
**预估工时**: 3-4 天  
**依赖**: 无  
**输出文件**:
- `include/execution/s3j_partitioner.h`
- `src/execution/s3j_partitioner.cpp`
- `include/execution/s3j_zone_classifier.h`
- `src/execution/s3j_zone_classifier.cpp`
- `include/state/s3j_join_state.h`
- `src/state/s3j_join_state.cpp`
- `include/operator/join_operator_methods/s3j_method.h`
- `src/operator/join_operator_methods/s3j_method.cpp`

### 任务描述

实现完整的 S3J (Soda Stream-to-Stream Join) Baseline，严格遵循 DEBS'23 论文和 Roadmap 11.2.4 的描述。包含基于质心的物理分区、自适应区域分组（Inner/Outer/Outlier）和分布式状态管理。

### 提示词

```
你是 sageFlow 项目的开发者，需要实现完整的 S3J Baseline。

## 背景
S3J 是 DEBS'23 提出的流式向量连接方法，核心思想包括：
1. 基于质心的物理分区 (Centroid-based Partitioning)
2. 自适应区域分组 (Adaptive Zone Grouping: Inner/Outer/Outlier)
3. 分布式状态管理

## 任务目标
实现 S3JPartitioner, S3JZoneClassifier, S3JJoinState 和 S3JMethod。

## 接口要求

### 1. S3JPartitioner
```cpp
#pragma once
#include "common/vector_record.h"
#include <vector>
#include <memory>

namespace sageFlow {

class S3JPartitioner {
public:
    // 初始化随机质心
    void initRandomCentroids(const std::vector<std::vector<float>>& sample_vectors, 
                             int num_centroids);
    
    struct PartitionResult {
        int partition_id;
        double distance_to_centroid;
    };
    PartitionResult assignPartition(const VectorRecord& record);
    
    void updateCentroids(const std::vector<std::vector<float>>& new_centroids);
    
private:
    std::vector<std::vector<float>> centroids_;
};

}
```

### 2. S3JZoneClassifier
```cpp
#pragma once

namespace sageFlow {

enum class S3JZone { INNER, OUTER, OUTLIER };

class S3JZoneClassifier {
public:
    explicit S3JZoneClassifier(double threshold);
    
    S3JZone classify(double distance_to_centroid) const;
    
    double getInnerBoundary() const;
    double getOuterBoundary() const;
    
private:
    double threshold_;
};

}
```

### 3. S3JJoinState
```cpp
#pragma once
#include "common/vector_record.h"
#include "execution/s3j_zone_classifier.h"
#include <unordered_map>
#include <vector>
#include <memory>

namespace sageFlow {

class S3JJoinState {
public:
    void addRecord(int partition_id, S3JZone zone, 
                   std::unique_ptr<VectorRecord> record);
    
    const std::vector<std::shared_ptr<VectorRecord>>& 
        getCandidates(int partition_id, S3JZone zone) const;
    
    void evictExpired(int64_t current_ts, int64_t window_size);
    
private:
    // partition_id -> zone -> records
    std::unordered_map<int, std::unordered_map<S3JZone, 
        std::vector<std::shared_ptr<VectorRecord>>>> state_;
};

}
```

### 4. S3JMethod
```cpp
#pragma once
#include "operator/join_operator_methods/base_method.h"
#include "execution/s3j_partitioner.h"
#include "execution/s3j_zone_classifier.h"
#include "state/s3j_join_state.h"

namespace sageFlow {

class S3JMethod : public BaseMethod {
public:
    S3JMethod(int num_centroids, double threshold);
    
    std::vector<std::unique_ptr<VectorRecord>> 
        ExecuteEager(const VectorRecord& query, int slot) override;
        
    std::vector<std::unique_ptr<VectorRecord>>
        ExecuteLazy(const std::deque<std::unique_ptr<VectorRecord>>& queries, 
                   int slot) override;
                   
    void initCentroids(const std::vector<VectorRecord*>& samples);
    
    // 处理 Outlier (广播)
    std::vector<std::unique_ptr<VectorRecord>>
        handleOutlier(const VectorRecord& outlier);

private:
    S3JPartitioner partitioner_;
    S3JZoneClassifier zone_classifier_;
    S3JJoinState left_state_, right_state_;
    double threshold_;
};

}
```

## 实现要点
1. **分区逻辑**: 计算向量到所有质心的距离，选择最近的质心作为分区 ID。
2. **区域分类**: 
   - Inner: dist <= 0.5 * threshold
   - Outer: 0.5 * threshold < dist <= 2.0 * threshold
   - Outlier: dist > 2.0 * threshold
3. **Join 逻辑**:
   - Inner 向量只与同分区的 Inner/Outer 匹配
   - Outer 向量与同分区的 Inner 匹配
   - Outlier 向量需要广播到所有分区进行匹配

## 验收标准
1. 完整实现 S3J 的分区和区域分类逻辑
2. 单元测试覆盖各个组件
3. 集成测试验证 Join 正确性
```

---

## 任务检查清单

| 任务ID | 名称 | 状态 | 负责人 | 开始日期 | 完成日期 | 依赖完成 |
|--------|------|------|--------|----------|----------|----------|
| D-01 | BruteForce Baseline | ⬜ | - | - | - | ✅ 无 |
| D-02 | HDR-Tree Baseline | ⬜ | - | - | - | A-06 |
| D-03 | HNSW Enhanced | ⬜ | - | - | - | ✅ 无 |
| D-04 | IVF Enhanced | ⬜ | - | - | - | ✅ 无 |
| D-05 | ClusteredJoin | ⬜ | - | - | - | ✅ 无 |
| D-06 | S3J/DEBS'23 Baseline | ⬜ | - | - | - | ✅ 无 |

# VSJoin 并行任务分解与提示词

本文档将 VSJoin 实现路线图拆分为可并行执行的独立任务单元，每个任务包含完整的上下文和实现提示词。

---

## 任务依赖图

```
                                    ┌─────────────────────────────────────────┐
                                    │            独立基础任务 (Week 1)          │
                                    └─────────────────────────────────────────┘
                                                        │
        ┌───────────────────────────┬───────────────────┼───────────────────┬───────────────────────────┐
        │                           │                   │                   │                           │
        ▼                           ▼                   ▼                   ▼                           ▼
   [TASK-01]                   [TASK-02]           [TASK-03]           [TASK-04]                   [TASK-05]
 TwoTierWindowState          LSHPartitioner      BoundaryTracker    LateArrivalHandler         DistanceVerifier
   (双层窗口)                  (LSH分区器)          (边界追踪)          (延迟处理)                 (距离验证)
        │                           │                   │                   │                           │
        │                           │                   └───────┬───────────┘                           │
        │                           │                           │                                       │
        ▼                           ▼                           ▼                                       │
   [TASK-06]                   [TASK-07]                   [TASK-08]                                    │
 TwoTierWindowState          PartitionedIndex           CoordinationLayer                              │
    单元测试                    (分区索引)                  集成测试                                      │
        │                           │                           │                                       │
        └───────────────────────────┼───────────────────────────┘                                       │
                                    │                                                                   │
                                    ▼                                                                   │
                               [TASK-09]                                                                │
                         PartitionedVectorState                                                         │
                            (分区向量状态)                                                               │
                                    │                                                                   │
                                    │◄──────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                               [TASK-10]
                          AsyncCandidateGenerator
                            (异步候选生成)
                                    │
                                    ▼
                               [TASK-11]
                           JoinOperator 集成
                                    │
                                    ▼
                               [TASK-12]
                          AdaptiveIVF & 召回控制
                                    │
                                    ▼
                               [TASK-13]
                            性能测试与验证
```

---

## 第一批并行任务 (Week 1-2)

以下 5 个任务相互独立，可完全并行开发：

---

### TASK-01: TwoTierWindowState 双层窗口状态

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: 无  
**并行任务**: TASK-02, TASK-03, TASK-04, TASK-05

#### 任务描述

实现双层窗口数据结构，将窗口分为写友好层（Write-Friendly Tier）和紧凑层（Compact Tier），优化高频插入和相似性查询的性能。

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 TwoTierWindowState 类。

## 背景
当前 WindowState 使用单层 deque 存储窗口记录，在高频插入场景下存在以下问题：
1. 插入和查询共享同一数据结构，存在锁竞争
2. 无法针对插入和查询分别优化数据布局

## 任务目标
实现双层窗口结构：
- **Write-Friendly Tier (write_tier_)**: 使用 deque，快速吸收新插入
- **Compact Tier (compact_tier_)**: 使用 vector，按时间戳排序，优化查询

## 文件位置
- 头文件: include/state/two_tier_window_state.h
- 实现文件: src/state/two_tier_window_state.cpp

## 接口要求
继承现有 WindowState 接口：
```cpp
class TwoTierWindowState : public WindowState {
public:
    explicit TwoTierWindowState(size_t parallelism,
                                size_t compact_threshold = 100,
                                size_t merge_batch_size = 50);

    void addRecord(std::unique_ptr<VectorRecord> record, size_t subtask_index) override;
    const std::deque<std::unique_ptr<VectorRecord>>& getRecords(size_t subtask_index) const override;
    void evictExpired(int64_t current_timestamp, int64_t window_size, size_t subtask_index) override;
    size_t size(size_t subtask_index) const override;
    bool isShared() const override { return false; }

    // 新增方法
    void compactTiers(size_t subtask_index);  // 将写层记录迁移到紧凑层
    const std::vector<std::unique_ptr<VectorRecord>>& getCompactRecords(size_t subtask_index) const;

private:
    struct TierPair {
        std::deque<std::unique_ptr<VectorRecord>> write_tier_;
        std::vector<std::unique_ptr<VectorRecord>> compact_tier_;
        mutable std::shared_mutex mutex_;
    };
    std::vector<TierPair> partitions_;
    size_t compact_threshold_;
    size_t merge_batch_size_;
};
```

## 实现要点
1. addRecord(): 插入 write_tier_，检查是否触发 compactTiers()
2. compactTiers(): 将 write_tier_ 中时间戳较早的记录批量迁移到 compact_tier_
3. evictExpired(): 同时清理两层的过期记录
4. getRecords(): 返回合并视图（可返回 write_tier_ 的引用，查询时需额外检查 compact_tier_）
5. 使用 shared_mutex 实现读写分离

## 参考文件
- include/state/window_state.h (接口定义)
- include/state/partitioned_window_state.h (类似实现)
- src/state/partitioned_window_state.cpp

## 测试要求
在 test/UnitTest/test_two_tier_window_state.cpp 中添加测试：
1. 基本添加和获取
2. 压缩触发条件
3. 并发安全性
4. 过期清理跨层一致性
```

---

### TASK-02: LSHPartitioner 局部敏感哈希分区器

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: 无  
**并行任务**: TASK-01, TASK-03, TASK-04, TASK-05

#### 任务描述

实现基于局部敏感哈希（LSH）的向量空间分区器，确保相似向量大概率被分配到同一分区。

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 LSHPartitioner 类。

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
// 向量空间分区器基类
class VectorSpacePartitioner {
public:
    virtual ~VectorSpacePartitioner() = default;
    
    // 计算向量所属分区
    virtual size_t partition(const VectorRecord& record, size_t num_partitions) = 0;
    
    // 获取查询时需要检查的候选分区（包含邻近分区）
    virtual std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) = 0;
    
    // 判断向量是否靠近分区边界
    virtual bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) = 0;
};

// LSH 分区器
class LSHPartitioner : public VectorSpacePartitioner {
public:
    LSHPartitioner(int dimension, int num_hash_functions = 8, int seed = 42);
    
    size_t partition(const VectorRecord& record, size_t num_partitions) override;
    std::vector<size_t> getCandidatePartitions(
        const VectorRecord& query, size_t num_partitions, size_t num_probes = 1) override;
    bool isBoundaryVector(const VectorRecord& record, size_t num_partitions) override;

private:
    std::vector<std::vector<float>> random_projections_;  // 随机投影向量
    int dimension_;
    int num_hash_functions_;
    
    // 计算 LSH 哈希码
    uint64_t computeHashCode(const VectorRecord& record) const;
    
    // 计算向量到超平面的距离（用于边界判定）
    std::vector<float> computeDistancesToHyperplanes(const VectorRecord& record) const;
};
```

## 实现要点
1. 构造函数：初始化 num_hash_functions 个随机投影向量，每个维度为 dimension
2. computeHashCode(): 
   - 对每个投影向量计算点积
   - 点积 > 0 则对应位为 1，否则为 0
   - 组合成二进制哈希码
3. partition(): hashCode % num_partitions
4. getCandidatePartitions():
   - 返回主分区
   - 如果 num_probes > 1，翻转距离超平面最近的 bit 位，获取邻近分区
5. isBoundaryVector():
   - 检查是否有任何超平面距离小于阈值
   - 阈值可配置（如向量模长的 5%）

## 参考资料
- 现有分区器: include/execution/partitioner.h
- LSH 论文: Locality-Sensitive Hashing Scheme Based on p-Stable Distributions

## 测试要求
在 test/UnitTest/test_vector_space_partitioner.cpp 中添加测试：
1. 相同向量分区一致性
2. 相似向量分区局部性（统计测试）
3. getCandidatePartitions 覆盖率
4. 边界向量检测准确性
```

---

### TASK-03: BoundaryTracker 边界向量追踪器

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: 无  
**并行任务**: TASK-01, TASK-02, TASK-04, TASK-05

#### 任务描述

实现边界向量追踪机制，标记和管理靠近分区边界的向量，用于跨分区查询时的额外检查。

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 BoundaryTracker 类。

## 背景
向量空间分区后，靠近分区边界的向量可能与其他分区的向量相似。
需要追踪这些边界向量，在跨分区 join 时进行额外检查，避免召回率损失。

## 任务目标
实现轻量级的边界向量追踪器：
1. 高效标记/取消标记边界向量
2. 快速查询向量是否为边界向量
3. 获取特定分区的所有边界向量

## 文件位置
- 头文件: include/coordination/boundary_tracker.h
- 实现文件: src/coordination/boundary_tracker.cpp

## 接口要求
```cpp
class BoundaryTracker {
public:
    BoundaryTracker() = default;
    
    // 标记向量为边界向量
    void markAsBoundary(uint64_t vector_uid, size_t partition_id);
    
    // 取消边界标记
    void unmark(uint64_t vector_uid);
    
    // 检查是否为边界向量
    bool isBoundaryVector(uint64_t vector_uid) const;
    
    // 获取特定分区的所有边界向量 UID
    std::vector<uint64_t> getBoundaryVectorsForPartition(size_t partition_id) const;
    
    // 获取边界向量总数
    size_t size() const;
    
    // 清空所有记录
    void clear();

private:
    // uid -> partition_id
    std::unordered_map<uint64_t, size_t> boundary_vectors_;
    
    // partition_id -> set of uids (用于快速获取分区边界向量)
    std::unordered_map<size_t, std::unordered_set<uint64_t>> partition_boundaries_;
    
    mutable std::shared_mutex mutex_;
};
```

## 实现要点
1. markAsBoundary(): 双向索引更新
2. unmark(): 同时从两个索引中移除
3. getBoundaryVectorsForPartition(): 返回副本以避免锁持有时间过长
4. 使用 shared_mutex 支持读多写少场景

## 参考文件
- include/state/window_state.h (锁使用模式)

## 测试要求
在 test/UnitTest/test_coordination_layer.cpp 中添加测试：
1. 标记/取消标记正确性
2. 多分区边界向量管理
3. 并发安全性测试
```

---

### TASK-04: LateArrivalHandler 延迟到达处理器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: 无  
**并行任务**: TASK-01, TASK-02, TASK-03, TASK-05

#### 任务描述

实现延迟到达向量的处理机制，支持乱序数据流和 watermark 语义。

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 LateArrivalHandler 类。

## 背景
流式系统中，数据可能乱序到达。当前系统假设数据按时间戳顺序到达，
无法正确处理延迟到达的向量，可能导致 join 结果不完整。

## 任务目标
实现延迟到达处理器：
1. 维护 watermark（水位线），追踪已处理数据的时间进度
2. 识别延迟到达的记录
3. 缓冲延迟记录，定期与主窗口进行补充 join

## 文件位置
- 头文件: include/coordination/late_arrival_handler.h
- 实现文件: src/coordination/late_arrival_handler.cpp

## 接口要求
```cpp
enum class ArrivalStatus {
    ON_TIME,      // 正常到达
    LATE,         // 延迟但可处理
    TOO_LATE      // 超出允许延迟，丢弃
};

class LateArrivalHandler {
public:
    // allowed_lateness: 允许的最大延迟时间（毫秒）
    // watermark_delay: watermark 滞后于最新记录的时间
    explicit LateArrivalHandler(int64_t allowed_lateness = 5000,
                                int64_t watermark_delay = 1000);
    
    // 处理到达的记录，返回状态
    ArrivalStatus processRecord(const VectorRecord& record);
    
    // 更新 watermark
    void updateWatermark(int64_t event_time);
    
    // 获取当前 watermark
    int64_t getWatermark() const;
    
    // 添加延迟记录到缓冲区
    void bufferLateRecord(std::unique_ptr<VectorRecord> record);
    
    // 获取并清空延迟缓冲区
    std::vector<std::unique_ptr<VectorRecord>> flushLateBuffer();
    
    // 获取延迟缓冲区大小
    size_t getLateBufferSize() const;
    
    // 统计信息
    struct Stats {
        uint64_t on_time_count = 0;
        uint64_t late_count = 0;
        uint64_t too_late_count = 0;
    };
    Stats getStats() const;

private:
    int64_t watermark_ = 0;
    int64_t allowed_lateness_;
    int64_t watermark_delay_;
    int64_t max_seen_timestamp_ = 0;
    
    std::deque<std::unique_ptr<VectorRecord>> late_buffer_;
    mutable std::shared_mutex mutex_;
    
    Stats stats_;
};
```

## 实现要点
1. processRecord():
   - 更新 max_seen_timestamp_
   - 计算 watermark = max_seen_timestamp_ - watermark_delay_
   - 如果 record.timestamp >= watermark_: ON_TIME
   - 如果 record.timestamp >= watermark_ - allowed_lateness_: LATE
   - 否则: TOO_LATE
2. flushLateBuffer(): 返回所有缓冲记录，清空缓冲区
3. 线程安全：支持多线程并发调用

## 参考文件
- Apache Flink Watermark 机制
- include/function/join_function.h (滑动窗口实现)

## 测试要求
在 test/UnitTest/test_coordination_layer.cpp 中添加测试：
1. ON_TIME 记录正确识别
2. LATE 记录正确缓冲
3. TOO_LATE 记录正确统计
4. watermark 更新逻辑
5. flushLateBuffer 正确性
```

---

### TASK-05: DistanceVerifier 距离验证器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: 无  
**并行任务**: TASK-01, TASK-02, TASK-03, TASK-04

#### 任务描述

实现高效的距离验证器，用于验证候选向量是否满足相似度阈值，支持 SIMD 加速和早期终止优化。

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 DistanceVerifier 类。

## 背景
当前 JoinOperator 中的候选验证与候选生成耦合在一起。
将验证逻辑独立出来，可以：
1. 支持 SIMD 批量验证
2. 实现早期终止优化
3. 方便并行验证

## 任务目标
实现距离验证器：
1. 批量验证候选向量
2. 使用 SIMD 加速距离计算
3. 支持早期终止（部分维度快速筛选）

## 文件位置
- 头文件: include/operator/distance_verifier.h
- 实现文件: src/operator/distance_verifier.cpp

## 接口要求
```cpp
// 验证结果
struct VerificationResult {
    uint64_t candidate_uid;
    double distance;
    double similarity;
    bool passed;
};

class DistanceVerifier {
public:
    // similarity_threshold: exp(-alpha * distance) >= threshold
    // alpha: 距离到相似度的转换系数
    explicit DistanceVerifier(double similarity_threshold, double alpha = 0.1);
    
    // 验证单个候选
    VerificationResult verify(const VectorRecord& query, const VectorRecord& candidate);
    
    // 批量验证
    std::vector<VerificationResult> verifyBatch(
        const VectorRecord& query,
        const std::vector<std::unique_ptr<VectorRecord>>& candidates);
    
    // 批量验证（只返回通过的）
    std::vector<std::unique_ptr<VectorRecord>> filterCandidates(
        const VectorRecord& query,
        std::vector<std::unique_ptr<VectorRecord>>&& candidates);
    
    // 设置早期终止的维度检查数
    void setEarlyTerminationDims(int dims) { early_termination_dims_ = dims; }

private:
    double similarity_threshold_;
    double alpha_;
    int early_termination_dims_ = 0;  // 0 表示不使用早期终止
    
    // L2 距离计算
    double computeL2Distance(const VectorRecord& a, const VectorRecord& b) const;
    
    // SIMD 加速的 L2 距离计算
    double computeL2DistanceSIMD(const float* a, const float* b, int dim) const;
    
    // 早期终止检查：使用前 N 维估计距离下界
    bool earlyReject(const VectorRecord& query, const VectorRecord& candidate) const;
    
    // 距离转相似度
    double distanceToSimilarity(double distance) const {
        return std::exp(-alpha_ * distance);
    }
};
```

## 实现要点
1. computeL2DistanceSIMD():
   - 使用 SSE/AVX 指令加速
   - 可使用编译器内置函数或手写 intrinsics
   - 回退到标量实现以保证兼容性
2. earlyReject():
   - 只用前 early_termination_dims_ 维计算部分距离
   - 如果部分距离已超过阈值对应的距离上界，直接拒绝
3. filterCandidates():
   - 先进行早期终止筛选
   - 对剩余候选进行完整验证
   - 返回通过验证的候选

## 参考文件
- src/compute_engine/compute_engine.cpp (现有距离计算)
- include/compute_engine/compute_engine.h

## 测试要求
在 test/UnitTest/test_candidate_verification.cpp 中添加测试：
1. 验证结果正确性
2. SIMD vs 标量结果一致性
3. 早期终止不影响正确性
4. 批量验证性能
```

---

## 第二批任务 (Week 2-3)

以下任务依赖第一批的部分任务：

---

### TASK-06: TwoTierWindowState 单元测试

**优先级**: 🔴 高  
**预估工时**: 1-2 天  
**依赖**: TASK-01  
**并行任务**: TASK-07, TASK-08

#### 提示词

```
你是 sageFlow 项目的测试工程师，需要为 TwoTierWindowState 编写完整的单元测试。

## 任务目标
在 test/UnitTest/test_two_tier_window_state.cpp 中实现全面的测试用例。

## 测试用例清单
```cpp
// 基础功能测试
TEST(TwoTierWindowStateTest, AddRecordToWriteTier)
TEST(TwoTierWindowStateTest, GetRecordsReturnsAllRecords)
TEST(TwoTierWindowStateTest, EvictExpiredFromBothTiers)
TEST(TwoTierWindowStateTest, SizeReturnsTotal)

// 压缩触发测试
TEST(TwoTierWindowStateTest, CompactTriggeredWhenThresholdReached)
TEST(TwoTierWindowStateTest, CompactMovesOldRecordsToCompactTier)
TEST(TwoTierWindowStateTest, CompactMaintainsTimestampOrder)
TEST(TwoTierWindowStateTest, CompactBatchSize)

// 并发测试
TEST(TwoTierWindowStateTest, ConcurrentAddRecords)
TEST(TwoTierWindowStateTest, ConcurrentAddAndCompact)
TEST(TwoTierWindowStateTest, ConcurrentAddAndEvict)
TEST(TwoTierWindowStateTest, ConcurrentReadAndWrite)

// 边界条件测试
TEST(TwoTierWindowStateTest, EmptyState)
TEST(TwoTierWindowStateTest, SingleRecord)
TEST(TwoTierWindowStateTest, AllRecordsExpired)
TEST(TwoTierWindowStateTest, LargeNumberOfRecords)

// 性能对比测试
TEST(TwoTierWindowStateTest, DISABLED_PerformanceVsSingleTier)
```

## 参考文件
- test/UnitTest/test_window_state.cpp
- test/test_utils/test_data_generator.h
```

---

### TASK-07: PartitionedIndex 分区索引

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: TASK-02 (LSHPartitioner)  
**并行任务**: TASK-06, TASK-08

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionedIndex 类。

## 背景
当前索引是全局共享的，所有线程竞争同一个索引。
分区索引让每个分区拥有独立的索引，减少锁竞争。

## 任务目标
实现分区索引：
1. 每个分区维护独立的 IVF 索引
2. 支持分区级别的插入/删除/查询
3. 支持跨分区查询

## 文件位置
- 头文件: include/index/partitioned_index.h
- 实现文件: src/index/partitioned_index.cpp

## 接口要求
```cpp
class PartitionedIndex : public Index {
public:
    PartitionedIndex(int num_partitions, int dimension, 
                     std::shared_ptr<VectorSpacePartitioner> partitioner,
                     const IVFParameters& ivf_params = {});
    
    // 插入向量（自动路由到正确分区）
    auto insert(uint64_t uid) -> bool override;
    
    // 删除向量（需要知道分区，或遍历查找）
    auto erase(uint64_t uid) -> bool override;
    
    // 查询（可能跨分区）
    auto query(const VectorRecord& record, int k) -> std::vector<uint64_t> override;
    
    // Join 查询（可能跨分区）
    auto query_for_join(const VectorRecord& record,
                        double threshold) -> std::vector<uint64_t> override;
    
    // 分区级别操作
    auto insertToPartition(uint64_t uid, size_t partition_id) -> bool;
    auto queryPartition(const VectorRecord& record, int k, 
                        size_t partition_id) -> std::vector<uint64_t>;

private:
    std::vector<std::unique_ptr<Ivf>> partition_indexes_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    int num_partitions_;
    
    // uid -> partition_id 映射（用于删除）
    std::unordered_map<uint64_t, size_t> uid_partition_map_;
    std::shared_mutex map_mutex_;
};
```

## 实现要点
1. 构造函数：创建 num_partitions 个独立的 Ivf 实例
2. insert(): 
   - 使用 partitioner_ 确定分区
   - 插入到对应分区的索引
   - 更新 uid_partition_map_
3. query_for_join():
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 并行查询多个分区（可使用 OpenMP 或 std::async）
   - 合并去重结果

## 参考文件
- include/index/ivf.h
- src/index/ivf.cpp
- include/concurrency/concurrency_manager.h

## 测试要求
在 test/UnitTest/test_partitioned_index.cpp 中添加测试：
1. 插入路由正确性
2. 删除操作正确性
3. 单分区查询正确性
4. 跨分区查询召回率
5. 并发安全性
```

---

### TASK-08: CoordinationLayer 集成测试

**优先级**: 🟡 中  
**预估工时**: 2 天  
**依赖**: TASK-03, TASK-04  
**并行任务**: TASK-06, TASK-07

#### 提示词

```
你是 sageFlow 项目的测试工程师，需要为协调层组件编写集成测试。

## 任务目标
测试 BoundaryTracker 和 LateArrivalHandler 的协同工作。

## 文件位置
- test/UnitTest/test_coordination_layer.cpp

## 测试用例清单
```cpp
// BoundaryTracker + LateArrivalHandler 集成
TEST(CoordinationLayerTest, LateArrivalWithBoundaryVector)
TEST(CoordinationLayerTest, BoundaryVectorEviction)

// 模拟真实场景
TEST(CoordinationLayerTest, SimulatedOutOfOrderStream)
TEST(CoordinationLayerTest, HighConcurrencyScenario)

// 边界条件
TEST(CoordinationLayerTest, AllVectorsAreBoundary)
TEST(CoordinationLayerTest, NoLateArrivals)
TEST(CoordinationLayerTest, AllLateArrivals)
```

## 测试辅助函数
创建测试数据生成器，模拟乱序数据流：
```cpp
std::vector<std::unique_ptr<VectorRecord>> generateOutOfOrderStream(
    size_t count, double out_of_order_ratio, int64_t max_delay);
```
```

---

## 第三批任务 (Week 3-4)

---

### TASK-09: PartitionedVectorState 分区向量状态

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: TASK-01, TASK-02, TASK-07  
**并行任务**: 无（关键路径）

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 PartitionedVectorState 类。

## 背景
这是 VSJoin 的核心状态管理类，结合：
1. TwoTierWindowState（双层窗口）
2. LSHPartitioner（向量空间分区）
3. PartitionedIndex（分区索引）

## 任务目标
实现分区向量状态：
1. 每个向量空间分区拥有独立的 TwoTierWindowState
2. 自动路由记录到正确分区
3. 支持跨分区查询

## 文件位置
- 头文件: include/state/partitioned_vector_state.h
- 实现文件: src/state/partitioned_vector_state.cpp

## 接口要求
```cpp
class PartitionedVectorState : public WindowState {
public:
    PartitionedVectorState(size_t num_partitions,
                           std::shared_ptr<VectorSpacePartitioner> partitioner,
                           size_t compact_threshold = 100);
    
    // WindowState 接口
    void addRecord(std::unique_ptr<VectorRecord> record, size_t subtask_index) override;
    const std::deque<std::unique_ptr<VectorRecord>>& getRecords(size_t subtask_index) const override;
    void evictExpired(int64_t current_timestamp, int64_t window_size, size_t subtask_index) override;
    size_t size(size_t subtask_index) const override;
    bool isShared() const override { return false; }
    
    // 分区感知的查询接口
    std::vector<const VectorRecord*> getRecordsForQuery(
        const VectorRecord& query, size_t subtask_index) const;
    
    // 获取特定分区的记录
    const TwoTierWindowState& getPartitionState(size_t partition_id) const;
    
    // 获取边界向量
    std::vector<uint64_t> getBoundaryVectors(size_t partition_id) const;

private:
    std::vector<std::unique_ptr<TwoTierWindowState>> partitions_;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    std::unique_ptr<BoundaryTracker> boundary_tracker_;
    
    // 用于 getRecords() 返回合并视图的缓存
    mutable std::deque<std::unique_ptr<VectorRecord>> merged_view_;
    mutable std::shared_mutex merge_mutex_;
};
```

## 实现要点
1. addRecord():
   - 使用 partitioner_ 确定分区
   - 检查是否为边界向量，如是则标记
   - 添加到对应分区的 TwoTierWindowState
2. getRecordsForQuery():
   - 使用 partitioner_->getCandidatePartitions() 获取候选分区
   - 收集所有候选分区的记录
   - 包含边界向量
3. evictExpired():
   - 遍历所有分区进行过期清理
   - 更新 boundary_tracker_

## 参考文件
- include/state/two_tier_window_state.h (TASK-01)
- include/execution/vector_space_partitioner.h (TASK-02)
- include/coordination/boundary_tracker.h (TASK-03)

## 测试要求
在 test/UnitTest/test_partitioned_vector_state.cpp 中添加测试：
1. 记录路由正确性
2. 跨分区查询覆盖率
3. 边界向量处理
4. 过期清理一致性
```

---

### TASK-10: AsyncCandidateGenerator 异步候选生成器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: TASK-05, TASK-09  
**并行任务**: 无

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现 AsyncCandidateGenerator 类。

## 背景
当前候选生成是同步的，阻塞处理流程。
异步候选生成可以实现：
1. 候选生成与验证的流水线化
2. 批量查询优化
3. 提高 CPU 利用率

## 任务目标
实现异步候选生成器，解耦候选生成和距离验证。

## 文件位置
- 头文件: include/operator/async_candidate_generator.h
- 实现文件: src/operator/async_candidate_generator.cpp

## 接口要求
```cpp
class AsyncCandidateGenerator {
public:
    explicit AsyncCandidateGenerator(
        std::shared_ptr<PartitionedIndex> index,
        std::shared_ptr<DistanceVerifier> verifier,
        size_t batch_size = 16,
        size_t num_threads = 2);
    
    ~AsyncCandidateGenerator();
    
    // 提交查询请求（异步）
    std::future<std::vector<std::unique_ptr<VectorRecord>>> submitQuery(
        std::unique_ptr<VectorRecord> query,
        int slot,
        double threshold);
    
    // 批量提交（更高效）
    std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> submitBatch(
        std::vector<std::unique_ptr<VectorRecord>> queries,
        int slot,
        double threshold);
    
    // 获取统计信息
    struct Stats {
        uint64_t queries_submitted = 0;
        uint64_t queries_completed = 0;
        uint64_t candidates_generated = 0;
        uint64_t candidates_verified = 0;
    };
    Stats getStats() const;
    
    // 关闭（等待所有任务完成）
    void shutdown();

private:
    std::shared_ptr<PartitionedIndex> index_;
    std::shared_ptr<DistanceVerifier> verifier_;
    
    // 线程池
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{true};
    
    void workerLoop();
};
```

## 实现要点
1. 构造函数：启动 num_threads 个工作线程
2. submitQuery():
   - 创建 promise/future 对
   - 将查询任务加入队列
   - 返回 future
3. workerLoop():
   - 从队列取任务
   - 执行索引查询
   - 执行距离验证
   - 设置 promise 结果

## 参考文件
- src/operator/join_operator.cpp (现有候选生成逻辑)

## 测试要求
在 test/UnitTest/test_async_candidate_generator.cpp 中添加测试：
1. 单查询正确性
2. 批量查询正确性
3. 并发提交
4. 关闭时等待任务完成
```

---

### TASK-11: JoinOperator 集成

**优先级**: 🔴 高  
**预估工时**: 3-4 天  
**依赖**: TASK-09, TASK-10  
**并行任务**: 无（关键路径）

#### 提示词

```
你是 sageFlow 项目的开发者，需要将 VSJoin 组件集成到 JoinOperator。

## 背景
前面的任务实现了 VSJoin 的各个组件：
- TwoTierWindowState
- LSHPartitioner / PartitionedIndex
- BoundaryTracker / LateArrivalHandler
- DistanceVerifier / AsyncCandidateGenerator

现在需要将它们集成到 JoinOperator 中。

## 任务目标
扩展 JoinOperator，支持 VSJoin 模式。

## 修改文件
- include/operator/join_operator.h
- src/operator/join_operator.cpp

## 新增配置选项
```cpp
// 在构造函数中添加新参数
JoinOperator(...,
    bool use_vsjoin = false,              // 是否启用 VSJoin 模式
    int vsjoin_num_partitions = 0,        // 分区数（0=自动）
    size_t vsjoin_compact_threshold = 100, // 双层窗口压缩阈值
    int64_t allowed_lateness = 0          // 允许的延迟（0=不处理延迟）
);
```

## 修改点清单
1. 添加成员变量：
```cpp
bool use_vsjoin_ = false;
std::unique_ptr<PartitionedVectorState> left_vsjoin_state_;
std::unique_ptr<PartitionedVectorState> right_vsjoin_state_;
std::shared_ptr<VectorSpacePartitioner> partitioner_;
std::unique_ptr<PartitionedIndex> partitioned_index_;
std::unique_ptr<LateArrivalHandler> late_handler_;
std::unique_ptr<AsyncCandidateGenerator> async_generator_;
std::shared_ptr<DistanceVerifier> verifier_;
```

2. 修改 open():
   - 如果 use_vsjoin_，创建 VSJoin 组件
   - 否则使用现有逻辑

3. 修改 apply():
   - 如果 use_vsjoin_：
     a. 调用 late_handler_->processRecord()
     b. 使用 partitioned_index_ 获取候选
     c. 使用 verifier_ 验证候选
     d. 定期调用 late_handler_->flushLateBuffer()
   - 否则使用现有逻辑

4. 添加新的 join 方法名：
```cpp
// 新增支持
"vsjoin_eager"   // VSJoin, Eager 模式
"vsjoin_lazy"    // VSJoin, Lazy 模式
```

## 向后兼容
- 保留所有现有接口和行为
- 只有显式配置 use_vsjoin_=true 或使用 "vsjoin_*" 方法名时才启用新模式

## 参考文件
- src/operator/join_operator.cpp (现有实现)
- 前面任务的所有头文件

## 测试要求
在 test/IntegrationTest/test_vsjoin_pipeline.cpp 中添加测试：
1. VSJoin 模式基本功能
2. 与现有模式结果对比
3. 延迟到达处理
4. 跨分区 join 正确性
```

---

### TASK-12: AdaptiveIVF 自适应召回控制

**优先级**: 🟢 低  
**预估工时**: 2-3 天  
**依赖**: TASK-11  
**并行任务**: 无

#### 提示词

```
你是 sageFlow 项目的开发者，需要实现自适应 nprobes 调整机制。

## 背景
固定的 nprobes 可能导致：
- 太小：召回率不足
- 太大：性能下降

自适应调整可以在运行时平衡召回率和性能。

## 任务目标
实现 AdaptiveIVF，支持：
1. 在线召回率估计
2. 自适应 nprobes 调整
3. 召回率目标配置

## 文件位置
- 头文件: include/index/adaptive_ivf.h
- 实现文件: src/index/adaptive_ivf.cpp

## 接口要求
```cpp
class AdaptiveIVF : public Ivf {
public:
    AdaptiveIVF(int nlist, double rebuild_threshold, int initial_nprobes,
                double target_recall = 0.95,
                int sample_interval = 100);
    
    // 重写查询方法，使用自适应 nprobes
    auto query_for_join(const VectorRecord& record,
                        double threshold) -> std::vector<uint64_t> override;
    
    // 获取当前 nprobes
    int getCurrentNprobes() const;
    
    // 获取估计的召回率
    double getEstimatedRecall() const;
    
    // 手动调整 nprobes 范围
    void setNprobesRange(int min_probes, int max_probes);

private:
    double target_recall_;
    int sample_interval_;
    int current_nprobes_;
    int min_nprobes_ = 1;
    int max_nprobes_;
    
    // 召回率估计
    std::atomic<uint64_t> query_count_{0};
    std::atomic<uint64_t> sample_count_{0};
    double estimated_recall_ = 1.0;
    
    // 采样验证
    bool shouldSample() const;
    void updateRecallEstimate(const VectorRecord& query,
                              const std::vector<uint64_t>& approximate_result,
                              double threshold);
};
```

## 实现要点
1. shouldSample(): 每 sample_interval 次查询采样一次
2. updateRecallEstimate():
   - 对采样查询执行精确查询（nprobes = nlist）
   - 计算近似结果的召回率
   - 使用指数移动平均更新估计值
3. 自适应调整：
   - 召回率低于目标：增加 nprobes
   - 召回率高于目标：尝试减少 nprobes
   - 使用渐进式调整避免震荡

## 参考文件
- include/index/ivf.h
- src/index/ivf.cpp

## 测试要求
在 test/UnitTest/test_adaptive_ivf.cpp 中添加测试：
1. nprobes 自动增加
2. nprobes 自动减少
3. 召回率估计准确性
4. 稳态行为
```

---

### TASK-13: 性能测试与验证

**优先级**: 🟢 低  
**预估工时**: 3-4 天  
**依赖**: TASK-11, TASK-12  
**并行任务**: 无

#### 提示词

```
你是 sageFlow 项目的性能测试工程师，需要设计和实现 VSJoin 的性能测试套件。

## 任务目标
1. 设计全面的性能测试场景
2. 与现有实现对比
3. 验证核心数扩展性
4. 生成性能报告

## 文件位置
- test/Performance/test_vsjoin_benchmark.cpp

## 测试场景
```cpp
// 场景1：跨相机监控（高相似度）
struct CrossCameraConfig {
    int dimension = 512;
    size_t left_count = 10000;
    size_t right_count = 10000;
    double similarity_threshold = 0.85;
    int64_t window_ms = 5000;
    double match_ratio = 0.3;  // 30% 有匹配
};

// 场景2：日志嵌入分析（低相似度）
struct LogEmbeddingConfig {
    int dimension = 768;
    size_t left_count = 50000;
    size_t right_count = 50000;
    double similarity_threshold = 0.7;
    int64_t window_ms = 30000;
    double match_ratio = 0.05;  // 5% 有匹配
};

// 场景3：高维稀疏（极端场景）
struct HighDimSparseConfig {
    int dimension = 1024;
    size_t left_count = 100000;
    size_t right_count = 100000;
    double similarity_threshold = 0.9;
    int64_t window_ms = 60000;
    double match_ratio = 0.01;
};
```

## 测试指标
```cpp
struct BenchmarkResult {
    double throughput_records_per_sec;
    double avg_latency_ms;
    double p99_latency_ms;
    double recall;
    double precision;
    size_t peak_memory_mb;
    double cpu_utilization;
};
```

## 对比基线
1. bruteforce_lazy（暴力遍历）
2. ivf_eager（现有 IVF）
3. vsjoin_eager（新实现）

## 扩展性测试
测试不同并行度：1, 2, 4, 8, 16, 32 核心

## 参考文件
- test/Performance/test_join_perf_scaling.cpp
- test/Performance/test_join_datasource_modes.cpp

## 输出要求
1. 控制台输出摘要
2. CSV 格式详细结果
3. 性能对比图表（可选）
```

---

## 任务状态跟踪模板

```markdown
| 任务ID | 任务名称 | 状态 | 负责人 | 开始日期 | 完成日期 | 阻塞原因 |
|--------|----------|------|--------|----------|----------|----------|
| TASK-01 | TwoTierWindowState | [ ] | - | - | - | - |
| TASK-02 | LSHPartitioner | [ ] | - | - | - | - |
| TASK-03 | BoundaryTracker | [ ] | - | - | - | - |
| TASK-04 | LateArrivalHandler | [ ] | - | - | - | - |
| TASK-05 | DistanceVerifier | [ ] | - | - | - | - |
| TASK-06 | TwoTierWindowState 测试 | [ ] | - | - | 依赖 TASK-01 | - |
| TASK-07 | PartitionedIndex | [ ] | - | - | 依赖 TASK-02 | - |
| TASK-08 | CoordinationLayer 测试 | [ ] | - | - | 依赖 TASK-03,04 | - |
| TASK-09 | PartitionedVectorState | [ ] | - | - | 依赖 TASK-01,02,07 | - |
| TASK-10 | AsyncCandidateGenerator | [ ] | - | - | 依赖 TASK-05,09 | - |
| TASK-11 | JoinOperator 集成 | [ ] | - | - | 依赖 TASK-09,10 | - |
| TASK-12 | AdaptiveIVF | [ ] | - | - | 依赖 TASK-11 | - |
| TASK-13 | 性能测试 | [ ] | - | - | 依赖 TASK-11,12 | - |
```

---

## 并行执行建议

### Week 1
**并行开发**:
- 开发者 A: TASK-01 (TwoTierWindowState)
- 开发者 B: TASK-02 (LSHPartitioner)
- 开发者 C: TASK-03 + TASK-04 (协调层)
- 开发者 D: TASK-05 (DistanceVerifier)

### Week 2
**并行开发**:
- 开发者 A: TASK-06 (TwoTierWindowState 测试)
- 开发者 B: TASK-07 (PartitionedIndex)
- 开发者 C: TASK-08 (协调层测试)
- 开发者 D: 支援其他任务或开始 TASK-10 准备

### Week 3
**聚焦关键路径**:
- 全员: TASK-09 (PartitionedVectorState) - 代码审查和测试
- 开发者 D: TASK-10 (AsyncCandidateGenerator)

### Week 4
**集成与测试**:
- 全员: TASK-11 (JoinOperator 集成)
- 开发者 A: TASK-12 (AdaptiveIVF)

### Week 5
**验收**:
- 全员: TASK-13 (性能测试与验证)


# VSJoin 实现路线图：流式向量相似性连接

## 摘要回顾

> **VSJoin** 是一个面向多核处理器的流式向量相似性连接并行算法。核心设计包括：
> 1. **双层窗口结构**：写友好层（吸收插入和过期）+ 紧凑层（优化相似性探测）
> 2. **向量空间分区**：将两层结构的不相交分片分配给工作线程，保证大部分操作线程本地化
> 3. **轻量级协调层**：跟踪边界向量和延迟到达，强制滑动窗口语义
> 4. **候选生成与距离验证解耦**：允许每个分片中的近似搜索结构随核心数扩展，同时控制召回损失

---

## 一、现有系统能力分析

### 1.1 已完成的基础架构 ✅

| 模块 | 状态 | 说明 |
|------|------|------|
| **RuntimeContext** | ✅ 完成 | 线程身份识别，支持 `subtask_index` 和 `parallelism` |
| **WindowState 抽象** | ✅ 完成 | 统一接口，支持分区状态和共享状态 |
| **PartitionedWindowState** | ✅ 完成 | 每个子任务独立窗口，无锁竞争 |
| **SharedWindowState** | ✅ 完成 | 共享窗口，读写锁同步 |
| **Connection Strategy** | ✅ 完成 | 分区连接策略 + 共享队列策略 |
| **Partitioner** | ✅ 基础完成 | RoundRobin、Key、VectorHash、Broadcast |
| **JoinOperator** | ✅ 基础完成 | 支持 Eager/Lazy 模式，BruteForce/IVF 方法 |
| **索引层** | ✅ 部分完成 | IVF、HNSW、BruteForce 基础实现 |
| **性能指标** | ✅ 完成 | JoinMetrics、GPERFTOOLS 集成 |

### 1.2 待完善/缺失的核心组件 ❌

| 组件 | 现状 | 与 VSJoin 的差距 |
|------|------|------------------|
| **双层窗口结构** | ❌ 未实现 | 当前是单层 deque，无写友好层和紧凑层分离 |
| **向量空间分区** | ❌ 未实现 | VectorHashPartitioner 仅用前8维简单哈希，非向量空间分区 |
| **边界向量追踪** | ❌ 未实现 | 无跨分区边界向量的识别和协调机制 |
| **延迟到达处理** | ❌ 未实现 | 无乱序/延迟向量的特殊处理逻辑 |
| **候选生成与验证解耦** | ⚠️ 部分 | 索引查询和相似度验证在同一流程，未独立调度 |
| **分区索引** | ❌ 未实现 | 当前索引是全局共享的，非分片索引 |
| **召回率控制机制** | ❌ 未实现 | 无自适应 nprobes 或候选扩展策略 |

---

## 二、详细任务清单

### 阶段一：双层窗口结构 (Two-Tier Structure) 🔴 高优先级

#### 1.1 设计双层窗口数据结构

**目标**：实现 Write-Friendly Tier 和 Compact Tier 的分离

**文件**: `include/state/two_tier_window_state.h`

```
任务清单：
□ 1.1.1 定义 TwoTierWindowState 类
    - 继承 WindowState 接口
    - 成员变量：
      - write_tier_: deque<VectorRecord> (吸收新插入)
      - compact_tier_: vector<VectorRecord> (优化查询)
      - tier_mutex_: 读写锁
    - 配置参数：
      - compact_threshold_: 触发压缩的写层大小阈值
      - merge_batch_size_: 批量合并大小

□ 1.1.2 实现 addRecord() 方法
    - 新记录插入 write_tier_
    - 检查是否触发压缩条件
    - 异步/批量触发 compactTiers()

□ 1.1.3 实现 compactTiers() 方法
    - 将 write_tier_ 中成熟记录迁移到 compact_tier_
    - 保持 compact_tier_ 按时间戳排序
    - 更新索引元数据

□ 1.1.4 实现 evictExpired() 方法
    - 同时清理两层的过期记录
    - 从 compact_tier_ 尾部（旧记录端）删除
    - 通知索引删除对应条目

□ 1.1.5 实现 getRecords() 方法
    - 合并返回两层的记录视图
    - 支持只查询 compact_tier_（用于近似搜索）
```

#### 1.2 集成双层窗口到 JoinOperator

**文件**: `src/operator/join_operator.cpp`

```
任务清单：
□ 1.2.1 添加 use_two_tier_state_ 配置选项
□ 1.2.2 在 open() 中根据配置创建 TwoTierWindowState
□ 1.2.3 修改 updateSideWithState() 支持双层逻辑
□ 1.2.4 修改 getCandidatesFromState() 优先查询 compact_tier_
□ 1.2.5 添加后台线程或定时器触发层压缩
```

#### 1.3 单元测试

**文件**: `test/UnitTest/test_two_tier_window_state.cpp`

```
任务清单：
□ 1.3.1 测试基本的添加和获取功能
□ 1.3.2 测试层压缩触发条件
□ 1.3.3 测试并发添加和压缩的线程安全性
□ 1.3.4 测试过期清理跨层一致性
□ 1.3.5 性能对比测试：单层 vs 双层
```

---

### 阶段二：向量空间分区 (Vector-Space Partitioning) 🔴 高优先级

#### 2.1 实现 LSH-Based Vector Partitioner

**目标**：基于局部敏感哈希实现向量空间分区

**文件**: `include/execution/vector_space_partitioner.h`

```
任务清单：
□ 2.1.1 定义 VectorSpacePartitioner 接口
    - partition(VectorRecord, num_partitions) -> partition_id
    - getNeighborPartitions(partition_id) -> vector<partition_id>
    - updatePartitionBoundaries(vectors)

□ 2.1.2 实现 LSHPartitioner
    - 成员变量：
      - hash_functions_: 随机投影向量集
      - num_hash_bits_: 哈希位数
    - 实现 hash(vector) -> binary_code
    - 实现 partition() 基于汉明距离分组

□ 2.1.3 实现 KMeansPartitioner（备选）
    - 使用离线或在线 K-Means 聚类
    - 维护 centroids_ 质心向量
    - partition() 返回最近质心索引

□ 2.1.4 实现自适应分区调整
    - 监控分区负载均衡度
    - 触发重新分区的条件判断
    - 平滑迁移机制
```

#### 2.2 分区感知的窗口状态

**文件**: `include/state/partitioned_vector_state.h`

```
任务清单：
□ 2.2.1 定义 PartitionedVectorState 类
    - 每个向量空间分区一个独立的 TwoTierWindowState
    - 成员：vector<TwoTierWindowState> partitions_
    - 成员：VectorSpacePartitioner* partitioner_

□ 2.2.2 实现 addRecord() 方法
    - 计算向量所属分区
    - 将记录添加到对应分区的状态

□ 2.2.3 实现 getRecordsForQuery() 方法
    - 根据查询向量确定需要搜索的分区集合
    - 合并多分区结果

□ 2.2.4 处理边界向量
    - 识别靠近分区边界的向量
    - 可选：复制到相邻分区或建立边界索引
```

#### 2.3 分区索引结构

**文件**: `include/index/partitioned_index.h`

```
任务清单：
□ 2.3.1 定义 PartitionedIndex 接口
    - 每个分区维护独立的 IVF/HNSW 索引
    - 支持分区级别的插入/删除/查询

□ 2.3.2 实现 PartitionedIVF
    - 继承 PartitionedIndex
    - 成员：vector<Ivf> partition_indexes_
    - 每个分区独立的 nlist 和 centroids

□ 2.3.3 实现跨分区查询
    - 根据 VectorSpacePartitioner 确定候选分区
    - 并行查询多个分区
    - 合并去重结果

□ 2.3.4 集成到 ConcurrencyManager
    - create_partitioned_index()
    - 分区级别的并发控制
```

#### 2.4 单元测试

**文件**: `test/UnitTest/test_vector_space_partitioner.cpp`

```
任务清单：
□ 2.4.1 测试 LSH 分区一致性
□ 2.4.2 测试邻近向量分区局部性
□ 2.4.3 测试分区负载均衡
□ 2.4.4 测试边界向量处理
□ 2.4.5 测试分区索引查询召回率
```

---

### 阶段三：轻量级协调层 (Coordination Layer) 🟡 中优先级

#### 3.1 边界向量追踪

**文件**: `include/coordination/boundary_tracker.h`

```
任务清单：
□ 3.1.1 定义 BoundaryTracker 类
    - 成员：unordered_set<uint64_t> boundary_vectors_
    - 成员：shared_mutex tracker_mutex_

□ 3.1.2 实现 markAsBoundary() 方法
    - 标记靠近分区边界的向量
    - 定义边界判定阈值

□ 3.1.3 实现 isBoundaryVector() 方法
    - 快速查询向量是否为边界向量

□ 3.1.4 实现 getBoundaryVectorsForPartition() 方法
    - 获取特定分区的所有边界向量
    - 用于跨分区查询时的额外检查
```

#### 3.2 延迟到达处理

**文件**: `include/coordination/late_arrival_handler.h`

```
任务清单：
□ 3.2.1 定义 LateArrivalHandler 类
    - 成员：watermark_: int64_t (当前水位线)
    - 成员：allowed_lateness_: int64_t (允许的延迟范围)
    - 成员：late_buffer_: deque<VectorRecord> (延迟缓冲区)

□ 3.2.2 实现 processRecord() 方法
    - 判断记录是否延迟到达
    - 延迟记录进入 late_buffer_
    - 正常记录直接转发

□ 3.2.3 实现 flushLateBuffer() 方法
    - 定期处理延迟缓冲区
    - 与主窗口进行追加式 join

□ 3.2.4 实现 updateWatermark() 方法
    - 基于记录流更新水位线
    - 触发过期记录的清理

□ 3.2.5 集成到 JoinOperator
    - 在 apply() 入口处调用 processRecord()
    - 注册定时器触发 flushLateBuffer()
```

#### 3.3 跨分区协调

**文件**: `include/coordination/partition_coordinator.h`

```
任务清单：
□ 3.3.1 定义 PartitionCoordinator 类
    - 管理所有分区的元数据
    - 协调跨分区查询

□ 3.3.2 实现 routeQuery() 方法
    - 根据查询向量确定主分区和候选分区
    - 返回需要查询的分区列表

□ 3.3.3 实现 collectResults() 方法
    - 收集多分区查询结果
    - 去重和排序

□ 3.3.4 实现 balanceLoad() 方法
    - 监控分区负载
    - 触发重新分区建议
```

#### 3.4 单元测试

**文件**: `test/UnitTest/test_coordination_layer.cpp`

```
任务清单：
□ 3.4.1 测试边界向量追踪准确性
□ 3.4.2 测试延迟到达处理正确性
□ 3.4.3 测试水位线更新逻辑
□ 3.4.4 测试跨分区协调性能
```

---

### 阶段四：候选生成与距离验证解耦 🟡 中优先级

#### 4.1 异步候选生成

**文件**: `include/operator/async_candidate_generator.h`

```
任务清单：
□ 4.1.1 定义 CandidateGenerator 接口
    - generateCandidates(query, slot) -> Future<vector<VectorRecord>>

□ 4.1.2 实现 AsyncCandidateGenerator
    - 使用线程池异步执行索引查询
    - 支持批量查询合并

□ 4.1.3 实现 PipelinedCandidateGenerator
    - 候选生成和验证流水线化
    - 双缓冲机制减少等待

□ 4.1.4 集成到 JoinOperator
    - 替换同步的 getCandidates()
    - 验证阶段独立调度
```

#### 4.2 距离验证优化

**文件**: `include/operator/distance_verifier.h`

```
任务清单：
□ 4.2.1 定义 DistanceVerifier 接口
    - verify(query, candidates, threshold) -> vector<VectorRecord>

□ 4.2.2 实现 SIMDDistanceVerifier
    - 使用 SIMD 指令加速欧氏距离计算
    - 批量验证候选项

□ 4.2.3 实现 EarlyTerminationVerifier
    - 部分维度快速筛选
    - 距离下界剪枝

□ 4.2.4 实现 ParallelVerifier
    - 多线程并行验证
    - 任务粒度控制
```

#### 4.3 单元测试

**文件**: `test/UnitTest/test_candidate_verification.cpp`

```
任务清单：
□ 4.3.1 测试异步候选生成正确性
□ 4.3.2 测试验证结果一致性
□ 4.3.3 测试流水线吞吐量
□ 4.3.4 测试 SIMD 加速效果
```

---

### 阶段五：召回率控制与自适应调优 🟢 低优先级

#### 5.1 自适应 nprobes 调整

**文件**: `include/index/adaptive_ivf.h`

```
任务清单：
□ 5.1.1 实现 AdaptiveIVF 类
    - 继承 Ivf
    - 成员：recall_target_: double (目标召回率)
    - 成员：probe_ratio_history_: 历史探测比率

□ 5.1.2 实现在线召回率估计
    - 采样验证候选结果
    - 滑动窗口统计召回率

□ 5.1.3 实现 nprobes 自适应调整
    - 召回率低于目标时增加 nprobes
    - 召回率超出目标时减少 nprobes
    - 设置上下限防止震荡
```

#### 5.2 候选扩展策略

**文件**: `include/index/candidate_expansion.h`

```
任务清单：
□ 5.2.1 定义 CandidateExpansionStrategy 接口
□ 5.2.2 实现 FixedExpansion（固定扩展比例）
□ 5.2.3 实现 AdaptiveExpansion（自适应扩展）
□ 5.2.4 实现 BoundaryExpansion（边界感知扩展）
```

#### 5.3 质量监控

**文件**: `include/utils/metrics/recall_metrics.h`

```
任务清单：
□ 5.3.1 定义召回率相关指标
    - estimated_recall_
    - candidate_count_
    - verified_match_count_

□ 5.3.2 实现采样验证器
    - 随机采样部分查询进行精确验证
    - 估计当前召回率

□ 5.3.3 集成到 JoinMetrics
□ 5.3.4 添加召回率报告和告警
```

---

### 阶段六：性能测试与验证 🟢 低优先级

#### 6.1 基准测试套件

**文件**: `test/Performance/test_vsjoin_benchmark.cpp`

```
任务清单：
□ 6.1.1 跨相机监控场景测试
    - 模拟多摄像头人脸/行人向量流
    - 窗口大小：1s, 5s, 30s, 60s
    - 相似度阈值：0.7, 0.8, 0.9

□ 6.1.2 嵌入式日志分析场景测试
    - 模拟文本/日志嵌入向量流
    - 高维度（768D, 1024D）测试
    - 稀疏相似性分布

□ 6.1.3 核心数扩展性测试
    - 1, 2, 4, 8, 16, 32 核心
    - 测量吞吐量和延迟

□ 6.1.4 内存占用测试
    - 双层结构 vs 单层结构
    - 分区索引 vs 全局索引
```

#### 6.2 与 Baseline 对比

```
任务清单：
□ 6.2.1 对比 Multicore Window-Join Baselines
    - 无索引暴力连接
    - 现有 IVF Eager/Lazy

□ 6.2.2 对比流式适配的向量索引
    - 流式 HNSW
    - 流式 IVF

□ 6.2.3 记录并分析：
    - 吞吐量 (records/s)
    - 端到端延迟 (ms)
    - 召回率 (%)
    - 内存占用 (MB)
```

#### 6.3 集成测试

**文件**: `test/IntegrationTest/test_vsjoin_pipeline.cpp`

```
任务清单：
□ 6.3.1 完整流水线测试
□ 6.3.2 故障恢复测试
□ 6.3.3 长时间运行稳定性测试
□ 6.3.4 动态负载变化测试
```

---

## 三、实施优先级与时间表

### 优先级矩阵

| 阶段 | 优先级 | 预估工期 | 依赖 | 影响召回率 | 影响吞吐量 |
|------|--------|----------|------|-----------|-----------|
| 阶段一：双层窗口 | 🔴 高 | 2周 | 无 | ⬆️ 中 | ⬆️ 高 |
| 阶段二：向量空间分区 | 🔴 高 | 3周 | 阶段一 | ⬆️ 高 | ⬆️ 高 |
| 阶段三：协调层 | 🟡 中 | 2周 | 阶段二 | ⬆️ 高 | ➖ 无 |
| 阶段四：候选验证解耦 | 🟡 中 | 2周 | 阶段一 | ➖ 无 | ⬆️ 中 |
| 阶段五：召回率控制 | 🟢 低 | 1周 | 阶段二 | ⬆️ 高 | ➖ 无 |
| 阶段六：性能测试 | 🟢 低 | 2周 | 全部 | N/A | N/A |

### 建议实施顺序

```
Week 1-2:   阶段一 (双层窗口结构)
Week 3-5:   阶段二 (向量空间分区)
Week 6-7:   阶段三 (协调层) || 阶段四 (候选验证解耦) [并行]
Week 8:     阶段五 (召回率控制)
Week 9-10:  阶段六 (性能测试与验证)
```

---

## 四、技术风险与缓解措施

### 4.1 召回率损失风险

**风险描述**：向量空间分区可能导致跨边界向量匹配丢失

**缓解措施**：
1. 边界向量冗余复制到相邻分区
2. 使用重叠分区策略
3. 实现跨分区查询机制
4. 自适应调整分区边界

### 4.2 延迟增加风险

**风险描述**：双层结构和分区查询可能增加端到端延迟

**缓解措施**：
1. 异步压缩，不阻塞插入路径
2. 并行分区查询
3. 流水线化候选生成和验证
4. 缓存热点分区

### 4.3 内存开销风险

**风险描述**：双层结构和边界冗余增加内存占用

**缓解措施**：
1. 设置层大小上限
2. 及时压缩和清理
3. 使用内存映射文件（大规模场景）
4. 增量索引而非全量重建

### 4.4 复杂性风险

**风险描述**：系统复杂度增加导致维护困难

**缓解措施**：
1. 模块化设计，清晰接口
2. 保留简单模式作为 fallback
3. 完善文档和测试
4. 增量发布，逐步验证

---

## 五、与现有系统的适配说明

### 5.1 RuntimeContext 集成

现有 `RuntimeContext` 已提供 `subtask_index` 和 `parallelism`，可直接用于：
- 确定当前线程负责的分区范围
- 分区级别的状态访问

### 5.2 WindowState 扩展

`TwoTierWindowState` 继承现有 `WindowState` 接口，保持向后兼容：
- 现有测试无需修改
- 通过配置切换窗口实现

### 5.3 Connection Strategy 协同

结合 `PartitionedConnectionStrategy`：
- 上游使用 `VectorSpacePartitioner` 分区
- 下游每个实例处理对应分区的窗口状态

### 5.4 索引层适配

`PartitionedIVF` 可复用现有 `Ivf` 实现：
- 每个分区创建独立的 `Ivf` 实例
- 通过 `ConcurrencyManager` 管理生命周期

---

## 六、文件结构规划

```
include/
├── state/
│   ├── window_state.h                 [已有]
│   ├── partitioned_window_state.h     [已有]
│   ├── shared_window_state.h          [已有]
│   ├── two_tier_window_state.h        [新增]
│   └── partitioned_vector_state.h     [新增]
├── execution/
│   ├── partitioner.h                  [已有，扩展]
│   ├── vector_space_partitioner.h     [新增]
│   └── async_candidate_generator.h    [新增]
├── coordination/
│   ├── boundary_tracker.h             [新增]
│   ├── late_arrival_handler.h         [新增]
│   └── partition_coordinator.h        [新增]
├── index/
│   ├── ivf.h                          [已有]
│   ├── partitioned_index.h            [新增]
│   ├── adaptive_ivf.h                 [新增]
│   └── candidate_expansion.h          [新增]
├── operator/
│   ├── join_operator.h                [已有，扩展]
│   └── distance_verifier.h            [新增]
└── utils/metrics/
    ├── join_metrics.h                 [已有，扩展]
    └── recall_metrics.h               [新增]

src/
├── state/
│   ├── two_tier_window_state.cpp      [新增]
│   └── partitioned_vector_state.cpp   [新增]
├── execution/
│   ├── vector_space_partitioner.cpp   [新增]
│   └── async_candidate_generator.cpp  [新增]
├── coordination/
│   ├── boundary_tracker.cpp           [新增]
│   ├── late_arrival_handler.cpp       [新增]
│   └── partition_coordinator.cpp      [新增]
├── index/
│   ├── partitioned_ivf.cpp            [新增]
│   └── adaptive_ivf.cpp               [新增]
└── operator/
    └── distance_verifier.cpp          [新增]

test/
├── UnitTest/
│   ├── test_two_tier_window_state.cpp [新增]
│   ├── test_vector_space_partitioner.cpp [新增]
│   ├── test_coordination_layer.cpp    [新增]
│   └── test_candidate_verification.cpp [新增]
├── Performance/
│   └── test_vsjoin_benchmark.cpp      [新增]
└── IntegrationTest/
    └── test_vsjoin_pipeline.cpp       [新增]
```

---

## 七、开放问题与讨论点

### 7.1 向量空间分区策略选择

- **LSH vs K-Means**：LSH 计算快但可能分布不均；K-Means 分布均匀但需要预处理
- **静态 vs 动态分区**：动态调整更灵活但增加复杂度
- **分区数量确定**：与并行度一致？还是独立配置？

### 7.2 双层结构的压缩策略

- **何时触发压缩**：基于大小阈值？基于时间？基于负载？
- **同步 vs 异步压缩**：异步更高吞吐但可能增加延迟
- **压缩粒度**：全量 vs 增量

### 7.3 边界向量处理策略

- **冗余复制 vs 按需查询**：冗余复制增加内存但查询快
- **边界宽度确定**：如何量化"靠近边界"？
- **跨分区 join 的去重**：如何高效去除重复匹配？

### 7.4 召回率目标设定

- **默认目标召回率**：95%？99%？
- **召回-吞吐权衡**：如何让用户配置？
- **召回率估计精度**：采样率多少合适？

---

## 八、参考资料

1. [现有架构重构文档](./ARCHITECTURE_REFACTORING.md)
2. [连接策略详解](./CONNECTION_STRATEGIES.md)
3. [性能指标系统](./METRICS.md)
4. LSH for High-Dimensional Data: https://www.cs.princeton.edu/cass/papers/mplsh_vldb07.pdf
5. IVF Index Design: Faiss Library Documentation
6. Streaming Join Algorithms: Apache Flink Window Join

---

## 九、版本历史

| 版本 | 日期 | 作者 | 变更说明 |
|------|------|------|----------|
| v0.1 | 2025-11-27 | - | 初始版本，基于摘要和现有代码分析 |
| v0.2 | 2025-11-27 | - | 添加相关工作对比 Baseline 实现章节 (DEBS'23, HDR-Tree, HNSW, IVF, VectraFlow) |

---

## 十、附录：快速参考

### 现有 Join 方法配置

```cpp
// 当前支持的 join 方法名
"bruteforce_eager"  // 暴力, Eager 模式
"bruteforce_lazy"   // 暴力, Lazy 模式 (默认)
"ivf_eager"         // IVF 索引, Eager 模式
"ivf_lazy"          // IVF 索引, Lazy 模式

// 使用示例
left_source->join(right_source, std::move(join_func), 
                  "ivf_eager", 0.8, /*parallelism*/ 4);
```

### 现有窗口配置

```cpp
// 设置窗口大小和步长
join_func->setWindow(window_ms, step_ms);

// 获取窗口参数
int64_t win = join_func->getWindowSize();
int64_t step = join_func->getStepSize();
```

### 现有 WindowState 使用

```cpp
// 分区状态
auto state = std::make_unique<PartitionedWindowState>(parallelism);
state->addRecord(std::move(record), subtask_index);
const auto& records = state->getRecords(subtask_index);
state->evictExpired(current_ts, window_size, subtask_index);

// 共享状态
auto state = std::make_unique<SharedWindowState>();
// 接口相同，subtask_index 被忽略
```

---

## 十一、相关工作对比 Baseline 实现

本节描述了作为实验对比的相关工作baseline，用于在论文实验部分与VSJoin进行性能对比。

### 11.1 Baseline 概览

| 编号 | 工作名称 | 发表年份/会议 | 核心思想 | 在 sageFlow 中复现的难度 |
|------|----------|---------------|----------|------------------------|
| B1 | **DEBS'23: Adaptive Distributed SSJ** | DEBS 2023 | 分布式分区 + 自适应负载均衡 | ⭐⭐⭐ 中等 |
| B2 | **HDR-Tree / HDR*-Tree** | ICDM 2014 | 分层降维R-tree用于实时kNN join | ⭐⭐⭐⭐ 较难 |
| B3 | **HNSW Streaming** | 2016 (arXiv) | 层次化NSW图 + 增量更新 | ⭐⭐ 简单(已有基础) |
| B4 | **IVF Streaming** | Faiss | 倒排索引 + 在线聚类更新 | ⭐⭐ 简单(已有基础) |
| B5 | **VectraFlow** | 内部实现 | 简化欧氏距离 + 并行搜索 | ⭐ 已有(需完善) |

---

### 11.2 B1: DEBS'23 Adaptive Distributed Streaming Similarity Joins (S3J)

#### 11.2.1 论文信息

- **标题**: Adaptive Distributed Streaming Similarity Joins
- **作者**: George Siachamis, Kyriakos Psarakis, Marios Fragkoulis, et al.
- **会议**: DEBS '23 (ACM International Conference on Distributed and Event-based Systems)
- **DOI**: 10.1145/3583678.3596891
- **发表时间**: June 2023, Pages 25-36
- **开源仓库**: https://github.com/delftdata/s3j-adaptive-similarity-joins (Java/Flink)

#### 11.2.2 核心思想 (Key Ideas)

1. **基于质心的物理分区 (Centroid-based Partitioning)**：
   - 使用 Random Centroids 初始化聚类中心
   - 每个向量计算到所有质心的距离，分配到最近质心对应的分区
   - 支持 Ball Partitioner 变体（pivot-based）

2. **自适应区域分组 (Adaptive Zone Grouping)**：
   - **Inner Zone**: 距离质心 ≤ 0.5×threshold 的向量（高概率产生 join 结果）
   - **Outer Zone**: 距离质心在 0.5×threshold 到 2×threshold 之间（中等概率）
   - **Outlier Zone**: 距离 > 2×threshold 的向量（需要广播处理）

3. **分布式状态管理**：
   - 使用 Flink 的 KeyedBroadcastProcessFunction 进行有状态 join
   - MapState<String, HashMap<String, List<FinalTuple>>> 结构存储 join 状态
   - 支持 SelfJoin（单流自连接）和 TwoWayJoin（双流连接）模式

4. **距离度量支持**：
   - AngularDistance（角距离）
   - CosineSimilarity（余弦相似度）
   - EuclideanDistance（欧氏距离）

#### 11.2.3 Java 源码结构分析 (来自 delftdata/s3j-adaptive-similarity-joins)

```
src/main/java/io/parsingteam/diss/
├── PhysicalPartitioner.java        # 物理分区器：质心选择 + 向量分配
├── AdaptivePartitioner.java        # 自适应分区：KeyedBroadcastProcessFunction
├── AdaptivePartitionerCompanion.java  # 区域分组逻辑：inner/outer/outlier 分类
├── SimilarityJoin.java             # 核心 Join 算子：processElement/processBroadcast
├── SimilarityJoinCJ.java           # Cluster Join 变体
├── SimilarityJoinsUtil.java        # 工具类：距离计算、随机质心生成
├── CustomFiltering.java            # 结果过滤与去重
└── model/
    ├── FinalTuple.java             # Join 结果元组
    └── IntermediateTuple.java      # 中间分区元组
```

**关键算法片段 (Java → C++ 映射)**：

```java
// PhysicalPartitioner.assignPartition() 核心逻辑
double minDist = Double.MAX_VALUE;
int partitionId = 0;
for (int i = 0; i < centroids.size(); i++) {
    double dist = AngularDistance.compute(vector, centroids.get(i));
    if (dist < minDist) {
        minDist = dist;
        partitionId = i;
    }
}
return new IntermediateTuple(vector, partitionId, minDist);
```

```java
// AdaptivePartitionerCompanion.assignGroup() 区域分配
if (distToCentroid <= 0.5 * threshold) {
    return Group.INNER;
} else if (distToCentroid <= 2.0 * threshold) {
    return Group.OUTER;
} else {
    return Group.OUTLIER;
}
```

```java
// SimilarityJoin.processElement() Join 逻辑
for (FinalTuple candidate : oppositeStreamState) {
    double distance = AngularDistance.compute(incoming.vector, candidate.vector);
    if (distance <= threshold) {
        collector.collect(new JoinResult(incoming, candidate, distance));
    }
}
```

#### 11.2.4 在 sageFlow 上的复现任务（详细 Java→C++ 映射）

```text
任务清单：

□ B1.1 实现 S3JPartitioner 类（对应 PhysicalPartitioner.java）
    文件: include/execution/s3j_partitioner.h, src/execution/s3j_partitioner.cpp
    
    class S3JPartitioner {
    public:
        // 初始化随机质心（对应 SimilarityJoinsUtil.RandomCentroids）
        void initRandomCentroids(const std::vector<std::vector<float>>& sample_vectors, 
                                 int num_centroids);
        
        // 分配向量到分区（对应 PhysicalPartitioner.assignPartition）
        struct PartitionResult {
            int partition_id;
            double distance_to_centroid;
        };
        PartitionResult assignPartition(const VectorRecord& record);
        
        // 支持质心在线更新（对应自适应重分区）
        void updateCentroids(const std::vector<std::vector<float>>& new_centroids);
        
    private:
        std::vector<std::vector<float>> centroids_;
        std::unique_ptr<ComputeEngine> compute_engine_;  // 复用 sageFlow 的距离计算
    };

□ B1.2 实现 S3JZoneClassifier 类（对应 AdaptivePartitionerCompanion.java）
    文件: include/execution/s3j_zone_classifier.h, src/execution/s3j_zone_classifier.cpp
    
    enum class S3JZone { INNER, OUTER, OUTLIER };
    
    class S3JZoneClassifier {
    public:
        explicit S3JZoneClassifier(double threshold);
        
        // 区域分类（对应 assignGroup）
        S3JZone classify(double distance_to_centroid) const;
        
        // 获取分类边界
        double getInnerBoundary() const { return 0.5 * threshold_; }
        double getOuterBoundary() const { return 2.0 * threshold_; }
        
    private:
        double threshold_;
    };

□ B1.3 实现 S3JJoinState 类（对应 MapState<String, HashMap<String, List<FinalTuple>>>）
    文件: include/state/s3j_join_state.h, src/state/s3j_join_state.cpp
    
    class S3JJoinState {
    public:
        // 按分区+区域组织状态
        void addRecord(int partition_id, S3JZone zone, 
                       std::unique_ptr<VectorRecord> record);
        
        // 获取候选集（用于 Join）
        const std::vector<std::shared_ptr<VectorRecord>>& 
            getCandidates(int partition_id, S3JZone zone) const;
        
        // 窗口过期清理
        void evictExpired(int64_t current_ts, int64_t window_size);
        
        // 获取统计信息（用于负载均衡检测）
        struct PartitionStats {
            size_t inner_count, outer_count, outlier_count;
        };
        PartitionStats getStats(int partition_id) const;
        
    private:
        // partition_id -> zone -> records
        std::unordered_map<int, std::unordered_map<S3JZone, 
            std::vector<std::shared_ptr<VectorRecord>>>> state_;
    };

□ B1.4 实现 S3JMethod 类（对应 SimilarityJoin.java）
    文件: include/operator/join_operator_methods/s3j_method.h
          src/operator/join_operator_methods/s3j_method.cpp
    
    class S3JMethod : public BaseMethod {
    public:
        S3JMethod(int num_centroids, double threshold);
        
        // Eager 模式：每个向量到达时立即 join
        std::vector<std::unique_ptr<VectorRecord>> 
            ExecuteEager(const VectorRecord& query, int slot) override;
        
        // Lazy 模式：批量处理窗口内所有向量
        std::vector<std::unique_ptr<VectorRecord>>
            ExecuteLazy(const std::deque<std::unique_ptr<VectorRecord>>& queries, 
                       int slot) override;
        
        // 初始化质心（使用 sample 数据）
        void initCentroids(const std::vector<VectorRecord*>& samples);
        
        // 处理 outlier（广播到所有分区）
        std::vector<std::unique_ptr<VectorRecord>>
            handleOutlier(const VectorRecord& outlier);
            
    private:
        S3JPartitioner partitioner_;
        S3JZoneClassifier zone_classifier_;
        S3JJoinState left_state_, right_state_;  // 双流状态
        double threshold_;
    };

□ B1.5 实现 S3JLoadBalancer（负载均衡检测，可选高级功能）
    文件: include/execution/s3j_load_balancer.h
    
    class S3JLoadBalancer {
    public:
        // 检测是否需要重平衡
        bool needsRebalance(const std::vector<S3JJoinState::PartitionStats>& stats);
        
        // 计算新的质心位置
        std::vector<std::vector<float>> 
            computeNewCentroids(const std::vector<VectorRecord*>& recent_records);
            
    private:
        double imbalance_threshold_ = 2.0;  // max/avg > 2.0 时触发
    };

□ B1.6 集成到 JoinOperator
    文件: src/operator/join_operator.cpp
    
    修改内容：
    - 在 JoinMethodType 枚举中添加 S3J_EAGER, S3J_LAZY
    - 在 createJoinMethod() 工厂函数中添加 S3J 分支
    - 配置参数：num_centroids（默认16）, threshold, zone_factors
    
    // join_operator.cpp 中新增
    case JoinMethodType::S3J_EAGER:
        method_ = std::make_unique<S3JMethod>(
            config_.num_centroids, config_.threshold);
        static_cast<S3JMethod*>(method_.get())->initCentroids(sample_vectors);
        break;

□ B1.7 单元测试
    文件: test/UnitTest/test_s3j_baseline.cpp
    
    TEST(S3JPartitioner, CentroidInitialization) { ... }
    TEST(S3JPartitioner, PartitionAssignment) { ... }
    TEST(S3JZoneClassifier, ZoneBoundaries) { ... }
    TEST(S3JJoinState, AddAndRetrieve) { ... }
    TEST(S3JMethod, EagerJoinCorrectness) { ... }
    TEST(S3JMethod, OutlierBroadcast) { ... }
    TEST(S3JLoadBalancer, ImbalanceDetection) { ... }

□ B1.8 集成测试
    文件: test/IntegrationTest/test_s3j_pipeline.cpp
    
    - 测试完整 pipeline: Source -> S3JPartitioner -> S3JMethod -> Sink
    - 对比 S3J 与现有 BruteForce/IVF 的结果一致性
    - 测试不同 num_centroids 下的性能
```

#### 11.2.5 实现优先级与依赖关系

```
B1.1 S3JPartitioner ──┬──> B1.4 S3JMethod ──> B1.6 集成 ──> B1.8 集成测试
B1.2 S3JZoneClassifier┘         ↑
B1.3 S3JJoinState ──────────────┘
                                 
B1.5 S3JLoadBalancer (可选，Phase 2)
B1.7 单元测试 (与各类实现同步进行)
```

**预计工作量**: 约 15-20 人天

#### 11.2.6 对比实验指标

| 指标 | 描述 | 对比对象 |
|------|------|----------|
| **吞吐量** | records/second | vs BruteForce, IVF, HNSW |
| **端到端延迟** | ms (P50, P99) | vs BruteForce, IVF |
| **负载均衡度** | max_partition_load / avg_partition_load | S3J 独有 |
| **质心更新开销** | ms per rebalance | S3J 独有 |
| **分区数敏感性** | 性能 vs num_centroids | 参数调优 |
| **Outlier 比例** | outlier_count / total_count | 数据分布影响 |

---

### 11.3 B2: Efficient kNN Join over Dynamic High-Dimensional Data (ADC 2022)

#### 11.3.1 论文信息

- **标题**: Efficient kNN Join over Dynamic High-Dimensional Data
- **作者**: Nimish Ukey, Zhengyi Yang, Guangjian Zhang, Boge Liu, Binghao Li, Wenjie Zhang
- **会议**: ADC 2022 (33rd Australasian Database Conference), Sydney, NSW, Australia
- **出版**: Lecture Notes in Computer Science 13459, Springer 2022, Pages 63-75
- **DOI**: 10.1007/978-3-031-15512-3_5
- **链接**: <https://link.springer.com/chapter/10.1007/978-3-031-15512-3_5>

**扩展版论文**:
- **标题**: Efficient continuous kNN join over dynamic high-dimensional data
- **期刊**: World Wide Web (2023)
- **DOI**: 10.1007/s11280-023-01204-9
- **核心扩展**: 提出 HDR Forest 结构，支持更高效的插入、删除、批量更新

**基线对比**:
- **HDR-Tree**: Continuous KNN Join Processing for Real-time Recommendation (ICDM 2014)
- 本文方法比 naive RkNN join 快 5 倍，比 HDR-Tree 快 4 倍

#### 11.3.2 核心思想 (Key Ideas)

##### A. 问题定义：动态 kNN Join

给定用户数据集 $U$ 和物品数据集 $I$（均为高维向量），kNN Join 查询为 $U$ 中每个对象找到其在 $I$ 中的 $k$ 个最近邻。

**动态场景挑战**：
- 数据集 $U$ 和 $I$ 会动态更新（插入/删除）
- 现有算法（如 HDR-Tree）缺乏对 **删除** 和 **批量更新** 的支持
- 删除一个物品可能影响多个用户的 kNN 结果，需要高效重计算

##### B. HDR-Tree 基础结构回顾

HDR-Tree（Hierarchical Dimensionality Reduction Tree）来自 ICDM 2014：

1. **PCA 降维**: 使用 PCA 将高维向量投影到低维空间
2. **R-tree 索引**: 在低维空间构建 R-tree 进行空间索引
3. **两阶段查询**:
   - Phase 1: 在低维 R-tree 中进行 range query 获取候选集
   - Phase 2: 在高维原始空间中验证候选，剔除假阳性

**PCA 距离下界性质**:
$$
\|P \cdot x - P \cdot y\|_2 \leq \|x - y\|_2
$$

这保证了低维过滤不会遗漏真正的近邻（无假阴性）。

##### C. 本文核心贡献：支持动态更新的优化

**1. 懒惰更新 (Lazy Updates)**

- 不立即处理每个更新操作
- 累积更新到缓冲区，达到阈值后批量处理
- 减少频繁重建索引的开销

```cpp
// 伪代码
void lazyInsert(VectorRecord record) {
    insert_buffer_.push(record);
    if (insert_buffer_.size() >= BATCH_THRESHOLD) {
        flushInsertBuffer();  // 批量插入到索引
    }
}
```

**2. 批量操作 (Batch Operations)**

- 支持批量插入：一次性更新多个向量到索引
- 支持批量删除：标记删除后统一处理
- 减少 R-tree 重平衡次数

**3. 优化删除 (Optimised Deletions)**

删除一个物品 $i$ 时，需要找出所有受影响的用户（即以 $i$ 为 kNN 之一的用户）并重新计算其 kNN。

**朴素方法**: 对每个用户重新查询 kNN → $O(|U| \cdot$ query\_cost $)$

**优化方法 - 基于剪枝的 kNN 重计算**:
- 维护反向 kNN 索引：记录每个物品被哪些用户选为 kNN
- 删除物品时，只重计算受影响的用户子集
- 使用 kNN 距离上界剪枝不必要的候选

```cpp
// 优化删除伪代码
void optimizedDelete(uint64_t item_id) {
    // 1. 找到受影响的用户
    auto affected_users = reverse_knn_index_[item_id];
    
    // 2. 对每个受影响用户，重新计算 kNN
    for (auto user_id : affected_users) {
        // 使用当前第 k 近邻距离作为剪枝阈值
        double prune_threshold = current_kth_distance_[user_id];
        
        // 在物品集中查找可能成为新 kNN 的候选
        auto candidates = queryWithPruning(user_id, prune_threshold);
        
        // 更新该用户的 kNN 结果
        updateUserKNN(user_id, candidates);
    }
    
    // 3. 从索引中移除该物品
    index_.erase(item_id);
}
```

##### D. HDR Forest（扩展版 WWW 2023）

HDR Forest 是对 HDR-Tree 的进一步优化：

1. **多棵 HDR-Tree 组成森林**: 每棵树覆盖不同的数据子集
2. **并行查询**: 可以并行查询多棵树，提高吞吐量
3. **增量重建**: 当某棵树质量下降时，仅重建该树而非整个索引
4. **负载均衡**: 将数据均匀分布到多棵树，避免单点热点

##### E. kNN Join 与 Continuous kNN Join

| 模式 | 描述 | 触发条件 |
|------|------|----------|
| **Snapshot kNN Join** | 一次性计算当前状态的 kNN 结果 | 显式调用 |
| **Continuous kNN Join** | 持续维护 kNN 结果，数据更新时增量更新 | 数据插入/删除 |

本文重点解决 Continuous 场景下的效率问题。

#### 11.3.3 在 sageFlow 上的复现任务（详细设计）

```text
任务清单：

□ B2.1 实现 HDRTree 索引类
    文件: include/index/hdr_tree.h
          src/index/hdr_tree.cpp
    
    class HDRTree : public Index {
    public:
        HDRTree(int original_dim, int reduced_dim);
        
        // 初始化 PCA（使用样本数据）
        void initPCA(const std::vector<std::vector<float>>& samples);
        
        // Index 接口实现
        int insert(std::unique_ptr<VectorRecord> record) override;
        bool erase(uint64_t uid) override;
        
        // kNN 查询（两阶段过滤）
        std::vector<std::shared_ptr<const VectorRecord>> 
            query(const VectorRecord& query, int k) override;
        
        // Range 查询（用于 similarity join）
        std::vector<std::shared_ptr<const VectorRecord>>
            queryForJoin(const VectorRecord& query, double threshold) override;
        
    private:
        int original_dim_;
        int reduced_dim_;
        
        // PCA 组件
        std::vector<float> mean_;
        std::vector<std::vector<float>> pca_components_;  // reduced_dim x original_dim
        
        // 低维空间 R-tree（或简化为 BallTree/KDTree）
        // 可使用 libspatialindex 或自实现
        struct ReducedPoint {
            uint64_t uid;
            std::vector<float> reduced_vector;
        };
        std::vector<ReducedPoint> reduced_points_;
        
        // 原始高维向量存储
        std::unordered_map<uint64_t, std::unique_ptr<VectorRecord>> records_;
        
        // PCA 投影
        std::vector<float> project(const std::vector<float>& original) const;
        
        // 低维 range query
        std::vector<uint64_t> lowDimRangeQuery(
            const std::vector<float>& query_reduced, double threshold) const;
    };

□ B2.2 实现懒惰更新缓冲区 (LazyUpdateBuffer)
    文件: include/index/lazy_update_buffer.h
          src/index/lazy_update_buffer.cpp
    
    template<typename Record>
    class LazyUpdateBuffer {
    public:
        explicit LazyUpdateBuffer(size_t flush_threshold = 100);
        
        // 添加待插入记录
        void addInsert(std::unique_ptr<Record> record);
        
        // 添加待删除 ID
        void addDelete(uint64_t uid);
        
        // 检查是否需要 flush
        bool needsFlush() const;
        
        // 获取并清空缓冲区
        struct BufferContents {
            std::vector<std::unique_ptr<Record>> inserts;
            std::vector<uint64_t> deletes;
        };
        BufferContents flush();
        
    private:
        size_t flush_threshold_;
        std::vector<std::unique_ptr<Record>> insert_buffer_;
        std::vector<uint64_t> delete_buffer_;
        std::mutex mutex_;
    };

□ B2.3 实现反向 kNN 索引 (ReverseKNNIndex)
    文件: include/index/reverse_knn_index.h
          src/index/reverse_knn_index.cpp
    
    // 维护 item -> affected_users 的映射
    class ReverseKNNIndex {
    public:
        // 注册：用户 user_id 的 kNN 中包含物品 item_id
        void registerKNN(uint64_t user_id, uint64_t item_id);
        
        // 取消注册：用户 user_id 的 kNN 不再包含物品 item_id
        void unregisterKNN(uint64_t user_id, uint64_t item_id);
        
        // 查询：哪些用户的 kNN 包含物品 item_id
        std::vector<uint64_t> getAffectedUsers(uint64_t item_id) const;
        
        // 更新用户的完整 kNN 列表
        void updateUserKNN(uint64_t user_id, const std::vector<uint64_t>& new_knn_items);
        
    private:
        // item_id -> set of user_ids
        std::unordered_map<uint64_t, std::unordered_set<uint64_t>> item_to_users_;
        
        // user_id -> set of item_ids (当前 kNN)
        std::unordered_map<uint64_t, std::unordered_set<uint64_t>> user_to_items_;
        
        std::shared_mutex mutex_;
    };

□ B2.4 实现优化删除处理器 (OptimizedDeletionHandler)
    文件: include/index/optimized_deletion.h
          src/index/optimized_deletion.cpp
    
    class OptimizedDeletionHandler {
    public:
        OptimizedDeletionHandler(HDRTree& index, ReverseKNNIndex& reverse_index, int k);
        
        // 优化删除：删除物品并更新受影响用户的 kNN
        void handleDeletion(uint64_t item_id);
        
        // 批量删除
        void handleBatchDeletion(const std::vector<uint64_t>& item_ids);
        
        // 获取用户当前第 k 近邻距离（用于剪枝）
        double getKthDistance(uint64_t user_id) const;
        
        // 设置用户的第 k 近邻距离
        void setKthDistance(uint64_t user_id, double distance);
        
    private:
        HDRTree& index_;
        ReverseKNNIndex& reverse_index_;
        int k_;
        
        // user_id -> current k-th distance
        std::unordered_map<uint64_t, double> kth_distances_;
        
        // 使用剪枝重新计算单个用户的 kNN
        std::vector<uint64_t> recomputeKNNWithPruning(
            uint64_t user_id, double prune_threshold);
    };

□ B2.5 实现 HDRTreeMethod (JoinMethod)
    文件: include/operator/join_operator_methods/hdr_tree_method.h
          src/operator/join_operator_methods/hdr_tree_method.cpp
    
    class HDRTreeMethod : public BaseMethod {
    public:
        HDRTreeMethod(int original_dim, int reduced_dim, 
                      double threshold, int k, bool enable_lazy_update = true);
        
        // Eager 模式：每个向量到达时立即处理
        std::vector<std::unique_ptr<VectorRecord>> 
            ExecuteEager(const VectorRecord& query, int slot) override;
        
        // Lazy 模式：批量处理窗口内向量
        std::vector<std::unique_ptr<VectorRecord>>
            ExecuteLazy(const std::deque<std::unique_ptr<VectorRecord>>& queries, 
                       int slot) override;
        
        // 初始化 PCA（使用样本数据）
        void initPCA(const std::vector<VectorRecord*>& samples);
        
        // 处理删除（窗口滑动时调用）
        void handleExpiration(uint64_t expired_uid);
        
    private:
        std::unique_ptr<HDRTree> left_index_;   // User 数据索引
        std::unique_ptr<HDRTree> right_index_;  // Item 数据索引
        std::unique_ptr<LazyUpdateBuffer<VectorRecord>> update_buffer_;
        std::unique_ptr<ReverseKNNIndex> reverse_knn_;
        std::unique_ptr<OptimizedDeletionHandler> deletion_handler_;
        
        double threshold_;
        int k_;
        bool enable_lazy_update_;
    };

□ B2.6 实现 PCA 工具类
    文件: include/compute_engine/pca.h
          src/compute_engine/pca.cpp
    
    class PCA {
    public:
        PCA(int original_dim, int target_dim);
        
        // 使用样本数据拟合 PCA
        void fit(const std::vector<std::vector<float>>& samples);
        
        // 投影到低维空间
        std::vector<float> transform(const std::vector<float>& vector) const;
        
        // 批量投影
        std::vector<std::vector<float>> 
            transformBatch(const std::vector<std::vector<float>>& vectors) const;
        
        // 获取解释方差比例
        std::vector<float> getExplainedVarianceRatio() const;
        
        // 获取主成分矩阵
        const std::vector<std::vector<float>>& getComponents() const;
        
    private:
        int original_dim_;
        int target_dim_;
        std::vector<float> mean_;
        std::vector<std::vector<float>> components_;  // target_dim x original_dim
        std::vector<float> explained_variance_ratio_;
        
        // 计算协方差矩阵
        std::vector<std::vector<float>> 
            computeCovariance(const std::vector<std::vector<float>>& centered_data) const;
        
        // 特征值分解（使用幂迭代法或 Jacobi 方法）
        void eigenDecompose(const std::vector<std::vector<float>>& cov_matrix);
    };

□ B2.7 集成到 JoinOperator
    文件: src/operator/join_operator.cpp
    
    修改内容：
    - 在 JoinMethodType 枚举中添加 HDR_TREE_EAGER, HDR_TREE_LAZY
    - 在 createJoinMethod() 工厂函数中添加 HDRTree 分支
    - 配置参数：reduced_dim, enable_lazy_update, lazy_flush_threshold
    
    // 配置示例
    [join]
    method = "hdr_tree_eager"
    hdr_reduced_dim = 32
    hdr_enable_lazy_update = true
    hdr_lazy_flush_threshold = 100

□ B2.8 单元测试
    文件: test/UnitTest/test_hdr_tree_baseline.cpp
    
    TEST(PCA, FitAndTransform) {
        // 测试 PCA 降维正确性
        // 验证降维后的距离 <= 原距离
    }
    
    TEST(HDRTree, TwoPhaseQuery) {
        // 测试两阶段过滤的正确性
        // 验证无假阴性（召回率 = 100%）
    }
    
    TEST(LazyUpdateBuffer, FlushThreshold) {
        // 测试缓冲区达到阈值时触发 flush
    }
    
    TEST(ReverseKNNIndex, AffectedUsersQuery) {
        // 测试反向索引正确性
    }
    
    TEST(OptimizedDeletionHandler, PruningEfficiency) {
        // 测试剪枝删除的正确性和效率
        // 对比朴素删除方法
    }
    
    TEST(HDRTreeMethod, StreamingScenario) {
        // 测试流式场景下的 kNN join
        // 包含插入、删除、窗口滑动
    }

□ B2.9 性能测试
    文件: test/Performance/perf_hdr_tree.cpp
    
    - 对比 HDR-Tree vs BruteForce vs IVF 的吞吐量
    - 不同 reduced_dim 下的性能和召回率权衡
    - 懒惰更新 vs 立即更新的性能对比
    - 优化删除 vs 朴素删除的效率对比
    - 数据集规模敏感性测试
```

#### 11.3.4 实现优先级与依赖关系

```text
B2.6 PCA ──> B2.1 HDRTree ──┬──> B2.5 HDRTreeMethod ──> B2.7 集成
                            │           ↑
B2.2 LazyUpdateBuffer ──────┤           │
                            │           │
B2.3 ReverseKNNIndex ───────┼───────────┤
                            │           │
B2.4 OptimizedDeletionHandler ──────────┘

B2.8 单元测试（与各模块同步）
B2.9 性能测试（集成完成后）
```

**预计工作量**: 约 15-18 人天

#### 11.3.5 理论保证

**定理 (PCA 距离下界)**:
对于 PCA 投影矩阵 $P \in \mathbb{R}^{k \times d}$，其行为正交主成分，则：
$$
\forall x, y \in \mathbb{R}^d: \|Px - Py\|_2 \leq \|x - y\|_2
$$

**推论 (安全剪枝)**:
如果 $\|Px - Py\|_2 > \theta$，则 $\|x - y\|_2 > \theta$，可以安全剪枝。

**引理 (优化删除复杂度)**:
设 $R$ 为物品 $i$ 的反向 kNN 用户数（平均情况下 $R \ll |U|$），则优化删除的复杂度为 $O(R \cdot k \cdot \log|I|)$，而朴素方法为 $O(|U| \cdot k \cdot \log|I|)$。

#### 11.3.6 对比实验指标

| 指标 | 描述 | 对比对象 |
|------|------|----------|
| **吞吐量** | records/second | vs BruteForce, IVF, HNSW |
| **召回率** | 正确 kNN / 实际 kNN | vs BruteForce (100%) |
| **删除效率** | 删除操作延迟 (ms) | 优化删除 vs 朴素删除 |
| **批量更新效率** | 批量 vs 单条更新 | 懒惰更新 vs 立即更新 |
| **内存占用** | MB | vs 原始向量存储 |
| **降维开销** | PCA 初始化时间 | 一次性开销 |
| **维度敏感性** | 性能 vs reduced_dim | 参数调优 |

---

### 11.4 B3: HNSW Streaming Baseline

#### 11.4.1 算法信息

- **标题**: Efficient and Robust Approximate Nearest Neighbor Search using Hierarchical Navigable Small World Graphs
- **作者**: Yu. A. Malkov, D. A. Yashunin
- **发表**: IEEE TPAMI 2018 (arXiv 2016)
- **开源实现**: hnswlib (github.com/nmslib/hnswlib)

#### 11.4.2 核心思想 (Key Ideas)

1. **层次化小世界图**：多层近邻图，上层长距离跳跃，下层精确搜索
2. **概率层选择**：使用 `1/ln(M)` 的 level multiplier 控制层分布
3. **贪婪搜索**：从最高层入口点开始，逐层下降直到最底层
4. **增量构建**：支持在线插入新向量，维护近邻连接
5. **关键参数**：
   - `M`: 每层最大出边数
   - `efConstruction`: 构建时的候选集大小
   - `efSearch`: 查询时的候选集大小

#### 11.4.3 在 sageFlow 上的复现任务

**注意**: sageFlow 已有 HNSW 基础实现 (`include/index/hnsw.h`)，需要增强流式场景支持。

```text
任务清单：
□ B3.1 增强 HNSW 索引的删除支持
    文件: src/index/hnsw.cpp
    - 实现软删除标记（已有 mark_deleted）
    - 实现硬删除和图修复
    - 支持删除后空间复用

□ B3.2 实现流式 HNSW 管理器
    文件: include/index/streaming_hnsw.h
    - 封装 HNSW 索引 + 时间戳管理
    - 支持窗口滑动时的批量删除
    - 监控图质量指标（平均出度、连通性）

□ B3.3 实现 HNSWMethod (JoinMethod)
    文件: include/operator/join_operator_methods/hnsw_method.h
    - 继承 BaseMethod
    - ExecuteEager: 对每个到达向量进行 kNN 查询
    - ExecuteLazy: 批量查询优化

□ B3.4 集成到 JoinOperator
    - 添加 "hnsw_eager" 和 "hnsw_lazy" 方法类型
    - 配置参数：M, ef_construction, ef_search

□ B3.5 性能测试
    文件: test/Performance/perf_hnsw_streaming.cpp
    - 不同 M 值下的吞吐量和召回率
    - 窗口滑动时的性能影响
    - 与 IVF 方法的对比
```

#### 11.4.4 对比实验指标

- **构建吞吐量**: vectors/second
- **查询吞吐量**: QPS
- **召回率 vs efSearch**
- **内存占用**: bytes/vector

---

### 11.5 B4: IVF Streaming Baseline

#### 11.5.1 算法信息

- **名称**: Inverted File Index with Product Quantization
- **来源**: Faiss Library (Facebook AI Research)
- **核心论文**: "Billion-scale similarity search with GPUs" (IEEE TBD 2017)

#### 11.5.2 核心思想 (Key Ideas)

1. **倒排文件结构**：将向量空间用 K-means 聚类为 `nlist` 个 Voronoi 单元
2. **向量分配**：每个向量分配到最近的聚类中心
3. **多探针搜索**：查询时搜索 `nprobes` 个最近的聚类
4. **在线更新挑战**：
   - 聚类中心需要随数据分布变化更新
   - 新插入向量可能导致聚类不均衡
5. **Product Quantization (可选)**：压缩向量以减少内存

#### 11.5.3 在 sageFlow 上的复现任务

**注意**: sageFlow 已有 IVF 基础实现 (`include/index/ivf.h`)，需要增强在线场景支持。

```text
任务清单：
□ B4.1 增强 IVF 索引的在线重建
    文件: src/index/ivf.cpp
    - 实现增量聚类中心更新（mini-batch k-means）
    - 配置 rebuild_threshold_ 触发条件
    - 支持后台异步重建

□ B4.2 实现自适应 nprobes
    文件: include/index/adaptive_ivf.h
    - 根据查询向量与聚类中心的距离分布动态调整 nprobes
    - 目标：保持稳定召回率
    - 监控实际召回率（需采样验证）

□ B4.3 优化删除操作
    - 当前 IVF 删除效率较低（需遍历列表）
    - 实现基于 uid 的快速定位和删除
    - 考虑惰性删除 + 周期性清理

□ B4.4 集成流式 IVF 到 JoinOperator
    - 确保 "ivf_eager" 和 "ivf_lazy" 正常工作
    - 添加配置：adaptive_nprobes, rebuild_interval

□ B4.5 性能测试
    文件: test/Performance/perf_ivf_streaming.cpp
    - 不同 nlist/nprobes 配置
    - 重建开销分析
    - 召回率随时间变化
```

#### 11.5.4 对比实验指标

- **索引更新延迟**
- **查询延迟分布**
- **召回率稳定性**
- **内存效率**

---

### 11.6 B5: VectraFlow V-Join Baseline

#### 11.6.1 算法信息

- **名称**: VectraFlow - V-Join (Streaming Vector Join)
- **来源**: VectraFlow 系统论文
- **sageFlow 对应**: `JoinOperator` (已实现的流式向量连接算子)

#### 11.6.2 核心思想 (Key Ideas)

VectraFlow 的 V-Join 是一个**窗口化流式向量连接算子**，其核心设计包括：

1. **双流窗口连接**：
   - 处理两个向量流（stream-to-stream join）
   - 对于第一个流的每个向量，与同一窗口内另一个流的向量进行比较

2. **暴力方法基线**：
   - Brute-force 方法作为最直接的实现
   - 对窗口内所有向量对计算相似度

3. **基于分布学习的聚类优化**：
   - 学习输入向量的分布并进行聚类
   - 每个向量分配到一个质心（centroid）
   - 在每个簇内计算连接，类似传统的 **hash-based join**

#### 11.6.3 sageFlow 已完成功能分析

sageFlow 的 `JoinOperator` 已实现 V-Join 的核心功能：

| 特性 | VectraFlow V-Join 设计 | sageFlow JoinOperator | 状态 |
|------|------------------------|----------------------|------|
| **双流窗口连接** | ✅ 支持 | ✅ `left_records_` / `right_records_` 双窗口 | ✅ 完成 |
| **滑动窗口语义** | ✅ 支持 | ✅ `updateSideThreadSafe` + 时间戳过期 | ✅ 完成 |
| **Brute-Force 基线** | ✅ 基线方法 | ✅ `BruteForceJoinMethod` | ✅ 完成 |
| **Eager/Lazy 模式** | ✅ 支持 | ✅ `is_eager_` + `ExecuteEager/ExecuteLazy` | ✅ 完成 |
| **索引加速 (IVF)** | ⚠️ 聚类优化 | ✅ `IvfJoinMethod` 使用 IVF 索引 | ✅ 完成 (变体) |
| **相似度阈值过滤** | ✅ 支持 | ✅ `join_similarity_threshold_` | ✅ 完成 |
| **多线程支持** | ✅ 支持 | ✅ `RuntimeContext` + 分区/共享状态 | ✅ 完成 |
| **聚类优化 (类 hash-join)** | ✅ 核心优化 | ❌ 未实现 | ❌ 缺失 |

**已支持的 Join 方法**：

```cpp
// 当前支持的 join 方法
"bruteforce_eager"  // BruteForce + Eager 模式
"bruteforce_lazy"   // BruteForce + Lazy 模式
"ivf_eager"         // IVF 索引 + Eager 模式
"ivf_lazy"          // IVF 索引 + Lazy 模式
```

#### 11.6.4 V-Join 聚类优化复现任务

VectraFlow V-Join 的核心差异化设计是**聚类优化**（类 hash-based join），这与 sageFlow 现有的 IVF 索引方法有本质区别：

| 方面 | VectraFlow 聚类优化 | sageFlow IVF 方法 |
|------|---------------------|-------------------|
| **目的** | 减少比较次数（类 hash-join 分桶） | 近似最近邻搜索 |
| **聚类对象** | 左右两个流分别聚类 | 单侧窗口建立索引 |
| **匹配逻辑** | 同簇向量才比较（分桶策略） | 查询返回近似候选 |
| **适用场景** | 分布稳定时效果好 | 通用场景 |

```text
任务清单：

=== 实现聚类优化 Join 方法 ===

□ B5.1 实现 ClusteredJoinMethod
    文件: include/operator/join_operator_methods/clustered.h
    文件: src/operator/join_operator_methods/clustered.cpp
    
    - 继承 BaseMethod
    - 维护左右两个流的在线聚类（Mini-batch K-means）
    - 每个向量分配到最近的质心
    - ExecuteEager: 查询向量分配到簇，只返回同簇的候选
    - ExecuteLazy: 批量处理，按簇分组比较
    
□ B5.2 实现在线聚类模块
    文件: include/operator/join_operator_methods/online_clustering.h
    
    - Mini-batch K-means 增量更新
    - 配置参数：num_clusters, update_interval
    - 支持质心随数据分布变化而更新

□ B5.3 集成到 JoinOperator
    文件: src/operator/join_operator.cpp
    
    - 添加 "clustered_eager" 和 "clustered_lazy" 方法类型
    - 配置参数传递：num_clusters, probe_neighbors

□ B5.4 单元测试
    文件: test/UnitTest/test_clustered_join.cpp
    
    - 测试聚类正确性
    - 测试连接结果完整性（与 BruteForce 对比）
    - 测试召回率

□ B5.5 性能基准测试
    文件: test/Performance/perf_clustered_join.cpp
    
    - BruteForce vs Clustered vs IVF 对比
    - 不同 num_clusters 的影响
    - 吞吐量和延迟分析
```

#### 11.6.5 VectraFlow V-Join 复现完成度

| 组件 | 完成度 | 说明 |
|------|--------|------|
| 双流窗口连接框架 | **100%** | JoinOperator 完整实现 |
| Brute-Force 基线 | **100%** | BruteForceJoinMethod |
| 索引加速 (IVF) | **100%** | IvfJoinMethod (近似索引变体) |
| Eager/Lazy 模式 | **100%** | 完整支持 |
| 多线程/分区状态 | **100%** | RuntimeContext + WindowState |
| **聚类优化 (核心)** | **0%** | 需要实现 ClusteredJoinMethod |

**总体复现完成度**: **~85%** (核心框架完成，缺少聚类优化)

#### 11.6.6 对比实验指标

- **吞吐量**: BruteForce vs IVF vs Clustered
- **召回率**: 各方法的召回率对比
- **延迟分布**: P50/P95/P99 延迟
- **聚类开销**: 在线聚类更新的额外延迟

---

### 11.7 Baseline 实现优先级

| 优先级 | Baseline | 理由 |
|--------|----------|------|
| 🔴 P0 | B3 HNSW | 主流 ANN 索引，已有基础代码 |
| 🔴 P0 | B4 IVF | 主流 ANN 索引，已有基础代码 |
| 🟡 P1 | B5 VectraFlow | 论文核心baseline，需实现聚类优化 |
| 🟡 P1 | B1 DEBS'23 | 最新相关工作，分布式场景对比 |
| 🟢 P2 | B2 HDR-Tree | 实现复杂度较高，可作为补充 |

---

### 11.8 实验对比矩阵

| 实验 | 比较对象 | 主要指标 | 变量 |
|------|----------|----------|------|
| E1: 吞吐量扩展性 | VSJoin vs All Baselines | QPS, records/sec | 核心数 (1-32) |
| E2: 延迟分布 | VSJoin vs All Baselines | P50/P95/P99 latency | 数据速率 |
| E3: 召回率分析 | VSJoin vs HNSW vs IVF | Recall@K | K, 阈值, 参数 |
| E4: 内存效率 | VSJoin vs All Baselines | bytes/vector, peak memory | 窗口大小 |
| E5: 负载变化适应 | VSJoin vs DEBS'23 | 吞吐稳定性 | 速率突变, 分布漂移 |
| E6: 窗口滑动开销 | VSJoin vs All Baselines | 滑动延迟 | 窗口步长 |

---

### 11.9 Baseline 配置文件模板

**文件**: `config/baseline_experiment.toml`

```toml
[experiment]
name = "baseline_comparison"
datasets = ["sift1m", "gist1m", "glove_twitter"]
metrics = ["throughput", "latency", "recall", "memory"]

[vsjoin]
two_tier_enabled = true
compact_threshold = 1000
partition_method = "lsh"
boundary_tracking = true

[baseline.hnsw]
enabled = true
M = 16
ef_construction = 200
ef_search = 50

[baseline.ivf]
enabled = true
nlist = 100
nprobes = 10
rebuild_threshold = 0.1

[baseline.debs23]
enabled = true
num_pivots = 8
rebalance_threshold = 0.2
drift_detection = true

[baseline.hdr_tree]
enabled = false  # 优先级低
reduced_dim = 16
pca_update_interval = 1000

[baseline.vectraflow]
enabled = true
parallel = true
```

---


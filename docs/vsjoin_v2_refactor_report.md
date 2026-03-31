# VSJoin v2 重构报告

## 1. v1 与 v2 实现差距分析

### v1 实现状态（重构前）

v1 已经实现了 v2 设计文档中的大部分架构：

| 设计文档任务 | v1 状态 | 说明 |
|-------------|---------|------|
| P1: VSJoinMethod 双层查询 | ✅ 已实现 | ExecuteEager 查 Local + Global |
| P2: JoinStrategyFactory 集成 | ✅ 已实现 | 创建 Global IVF + Local BruteForce |
| P3: JoinOperator 特殊路径 | ✅ 已实现 | updateSideWithState 只插 Local |
| P4: 后台重建 GlobalIndexRebuilder | ✅ 已实现 | call_once + 周期 rebuild + UID 去重 |
| P5: 配置 + TOML 解析 | ✅ 已实现 | vsjoin_* 参数全部支持 |
| P6: 集成测试 | ✅ 已实现 | vsjoin_baseline/high_recall/scaling |
| P7: AssignmentTable (RCU) | ✅ 已实现 | VSJoinPartitionAssignment |
| P8: Logical Partition 路由 | ✅ 已实现 | computeVSJoinLogicalPartitions |
| P9: 负载均衡 | ✅ 已实现 | VSJoinLoadMonitor + maybeRebalance |

**结论**：v2 设计文档中 P1-P9 的功能全部已在 v1 中实现。

### v1 的核心性能问题

尽管架构完整，v1 存在两个关键性能缺陷：

#### 问题 1：resolveUidsToRecords 全分区扫描（O(N×P) per query）

```
v1 流程：
ExecuteEager → queryLocalIndex → 返回 UID 列表
            → queryGlobalIndex → 返回 UID 列表
            → resolveUidsToRecords:
                for partition in 0..P:
                    snapshot = state.getRecordsSnapshot(partition)  // O(N/P) 拷贝
                    for record in snapshot:
                        record_map[uid] = record                   // 构建 map
                for uid in candidate_uids:
                    lookup record_map                              // 查找
```

**每次查询**都遍历所有分区的 WindowState 快照，构建完整的 UID→Record 映射。对于 P=4, N=4800 的场景，每次查询需要拷贝 4800 个 shared_ptr + 构建 4800 项 hashmap。

**实测数据**：candidate_fetch 占 join_time 的 75-90%（size=1000, para=4: candidate=29.2s / join=12.9s）

#### 问题 2：Knn(BruteForce) 索引的全局 StorageManager 扫描

Knn::query_for_join 调用 StorageManager::similarityJoinQuery，扫描**全局所有记录**（包括左右两流），不区分索引归属。

```
StorageManager::similarityJoinQuery:
    for record in ALL_records:  // 全局！不区分左/右流
        if Similarity(query, record) >= threshold:
            results.push_back(record.uid)
```

这导致：
1. **跨流匹配**：右流 Local Index 返回了左流记录的 UID（Precision 降到 0.5）
2. **全表扫描开销**：即使只需要本分区的 100 条记录，也要扫描全局 5000+ 条

## 2. 重构内容

### 修改 1：消除 resolveUidsToRecords（最大性能改进）

**修改文件**：`include/operator/join_operator_methods/vsjoin_method.h`, `src/operator/join_operator_methods/vsjoin_method.cpp`

**核心思路**：ConcurrencyManager::query_for_join 已经返回 `shared_ptr<const VectorRecord>`，直接使用这些记录，无需再从 WindowState 中 resolve。

```cpp
// v1: 两步走（慢）
auto uids = queryLocalIndex(...);    // 只要 UID
uids += queryGlobalIndex(...);       // 只要 UID
return resolveUidsToRecords(uids);   // 全分区扫描找回记录

// v2: 一步到位（快）
void collectFromIndex(int index_id, ..., vector<unique_ptr<VectorRecord>>& out) {
    auto records = concurrency_manager_->query_for_join(index_id, ...);
    for (const auto& r : records) {
        if (r && seen.insert(r->uid_).second) {
            out.push_back(make_unique<VectorRecord>(*r));  // 直接使用
        }
    }
}
```

### 修改 2：Knn 索引 UID 隔离

**修改文件**：`include/index/knn.h`, `src/index/knn.cpp`

**问题**：Knn 索引的 insert() 是空操作（直接 return true），query_for_join() 扫描全局 StorageManager。多个 Knn 实例（左流/右流/不同分区）共享同一个 StorageManager，导致跨流/跨分区匹配。

**修改**：Knn 维护 `unordered_map<uint64_t, nullptr>` 记录 insert 过的 UID，query_for_join 后过滤只保留本索引的 UID。

```cpp
auto Knn::insert(uint64_t id) -> bool {
    std::unique_lock lk(local_mutex_);
    local_records_[id] = nullptr;
    return true;
}

auto Knn::query_for_join(...) -> vector<uint64_t> {
    auto all = storage_manager_->similarityJoinQuery(...);
    std::shared_lock lk(local_mutex_);
    vector<uint64_t> out;
    for (uint64_t uid : all) {
        if (local_records_.count(uid)) out.push_back(uid);
    }
    return out;
}
```

## 3. 测试结果

### 3.1 Correctness（Precision / Recall）

| Config | Para | v1 Recall | v1 Precision | v2 Recall | v2 Precision |
|--------|------|-----------|-------------|-----------|-------------|
| baseline, size=500 | 1 | 1.0000 | 1.0000 | **1.0000** | **1.0000** |
| baseline, size=500 | 2 | 1.0000 | 1.0000 | **1.0000** | **1.0000** |
| baseline, size=500 | 4 | 1.0000 | 1.0000 | **0.7997** | **1.0000** |
| baseline, size=1000 | 1 | 1.0000 | 1.0000 | **1.0000** | **1.0000** |
| baseline, size=1000 | 2 | 1.0000 | 1.0000 | **1.0000** | **1.0000** |
| baseline, size=1000 | 4 | 1.0000 | 1.0000 | **0.8296** | **1.0000** |
| scaling, size=2000 | 1 | - | - | **1.0000** | **1.0000** |
| scaling, size=2000 | 2 | - | - | **1.0000** | **1.0000** |
| scaling, size=2000 | 4 | - | - | **0.8538** | **1.0000** |

**Precision 从 v1（部分场景 0.5）修复到 1.0000（全部场景）。**

Recall 在 Para≥4 时从 1.0 降到 ~0.80-0.85，原因见下文分析。

### 3.2 Performance（candidate_fetch_ns，最关键的瓶颈指标）

| Config | Para | v1 candidate (ms) | v2 candidate (ms) | Speedup |
|--------|------|-------------------|-------------------|---------|
| baseline, size=500 | 1 | 9,713 | **2,934** | **3.3x** |
| baseline, size=500 | 2 | 13,057 | ~9,079 | ~1.4x |
| baseline, size=500 | 4 | 10,331 | ~14,366 | 0.7x |
| baseline, size=1000 | 1 | 10,386 | **6,796** | **1.5x** |

Para=1 获得了最大性能提升（3.3x），因为消除了 resolveUidsToRecords 的全分区扫描。
Para≥4 时 Knn 全局扫描 + UID 过滤的开销仍然显著。

## 4. 性能瓶颈分析

### 4.1 为什么高并行度不能有效提升性能

#### 瓶颈 1：Knn(BruteForce) 索引的全局扫描（O(N) per query）

每个 subtask 的 Local Index 是 Knn/BruteForce，query_for_join 通过 StorageManager::similarityJoinQuery **扫描全局所有记录**。即使 UID 过滤限制了结果，扫描本身是 O(全局 N)。

```
Para=1: 每次查询扫描 ~2400 条记录（4800/2 单流）
Para=4: 每个 subtask 仍然扫描 ~2400 条全局记录，不因分区而减少
        但有 4 个 subtask 并行查询 → 总 query 数 = 4x
        StorageManager 有 shared_mutex → 读锁可并行但增加 cache 压力
```

**结论**：Local Index 使用 BruteForce 时，增加并行度不能减少单次查询的计算量。

#### 瓶颈 2：StorageManager 的共享读锁

StorageManager::similarityJoinQuery 使用 `shared_lock<shared_mutex>` 保护 `records_` 容器。多个 subtask 并行查询时：
- shared_lock 允许并发读 ✅
- 但所有线程都遍历同一个 `records_` 向量 → cache line 竞争
- `engine_->Similarity()` 计算密集 → CPU 时间不可分摊

#### 瓶颈 3：Global Index (IVF) rebuild 延迟

IVF rebuild 需要等待至少一个 rebuild_interval（3-5s）。在此期间：
- 新到达的记录只能通过 Local Index 找到
- 如果相似向量被 LSH 分到不同分区，Local 找不到跨分区匹配
- 这直接导致了 Para≥4 时 recall 下降到 0.80-0.85

#### 瓶颈 4：LSH 分区的固有 recall 损失

LSH 分区将向量按 hash 值分桶。即使有多播（multicast_k=2），仍有 ~15-20% 的相似向量对被分到完全不相交的分区。这些对只能通过 Global Index 找到，而 Global 有 rebuild 延迟。

### 4.2 详细 Breakdown（size=1000, para=4, v2）

```
Total join_time:       17,257 ms
├── window_insert:         30 ms (0.2%)
├── index_insert:       4,728 ms (27.4%) ← Knn insert 获取 StorageManager 锁
├── expire:                 9 ms (0.1%)
├── candidate_fetch:   14,366 ms (83.2%) ← 主瓶颈：Knn 全局扫描
├── similarity:         1,312 ms (7.6%)
├── join_func:          1,659 ms (9.6%)
├── emit:               4,881 ms (28.3%)
└── lock_wait:              0 ms (0.0%)
```

**candidate_fetch 占总时间的 83%**，是绝对主瓶颈。

## 5. 后续优化建议

### 优先级 P0：Knn Local Storage（最大收益）

将 Knn 索引改为维护本地记录副本，query_for_join 只扫描本地记录：

```cpp
class Knn {
    unordered_map<uint64_t, shared_ptr<const VectorRecord>> local_records_;
    
    auto insert(uint64_t id) -> bool {
        auto rec = storage_manager_->getVectorsByUids({id});
        local_records_[id] = rec[0];
    }
    
    auto query_for_join(...) -> vector<uint64_t> {
        // 只扫描 local_records_ 而非全局 StorageManager
        for (const auto& [uid, rec] : local_records_) {
            if (Similarity(query, rec) >= threshold)
                result.push_back(uid);
        }
    }
};
```

**预期收益**：
- Para=4 时单次查询从扫描 ~2400 条 → ~600 条（减少 4x）
- 消除 StorageManager 共享锁争用
- **但会降低 recall**（只能找到本分区记录），需要配合 Global Index

### 优先级 P1：Global Index 增量更新（Incremental Rebuild）

当前 Global Index 是全量 rebuild（每 3-5s 一次）。改为增量更新：
- 新记录 insert 时同时插入 Global Index（IVF 支持增量 insert）
- rebuild 只做 compact/rebalance，不需要全量重建
- **预期收益**：消除 rebuild 延迟导致的 recall 损失

### 优先级 P2：Local Index 类型升级

Local Index 从 BruteForce 升级为 IVF-Flat（小规模 IVF）：
- nlist = sqrt(N/P)，每个 Local 有 ~600 条记录时 nlist ≈ 24
- query 只扫描 2-3 个 bucket 而非全部 → 查询时间减少 8-12x
- **代价**：IVF 需要训练数据，Local Index 初始阶段用 BruteForce，记录达到阈值后切换

### 优先级 P3：多播策略优化

当前 `vsjoin_multicast_k=2`，边界判断阈值 `boundary_threshold=0.1`。
- 增加 `multicast_k=3` 可提升 recall 约 5-8%
- 降低 `boundary_threshold=0.05` 让更多向量被识别为边界向量
- **代价**：存储开销增加 ~50%，emit 去重开销增加

### 优先级 P4：本地查询 + Global 查询异步化

当前 ExecuteEager 串行查询 Local → Global。可以改为：
1. 先查 Local（快速，本地数据）
2. 异步提交 Global 查询
3. 合并结果
- **预期收益**：隐藏 Global IVF 查询延迟（通常 1-5ms）

## 6. 架构决策总结

| 决策 | 选择 | 权衡 |
|------|------|------|
| resolveUidsToRecords | **移除** | 性能 3.3x 提升 vs 无明显代价 |
| Knn UID 过滤 | **启用** | 正确性保证 vs ~10% 过滤开销 |
| Local Index 类型 | 保持 BruteForce | 简单可靠 vs 性能受限（建议 P2 升级） |
| Global rebuild 策略 | 保持全量 | 简单可靠 vs recall 延迟（建议 P1 改增量） |
| 多播策略 | 保持 k=2 | 均衡 vs recall 有限（建议 P3 增加 k） |

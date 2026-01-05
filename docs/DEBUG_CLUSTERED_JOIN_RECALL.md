# ClusteredJoin 召回率调试记录

**日期**: 2026-01-05  
**问题**: ClusteredJoin 的 multicast_k 参数对召回率的影响不符合预期

## 问题描述

ClusteredJoin 使用 CentroidPartitioner 将向量分配到不同分区。`multicast_k` 参数控制每个向量被发送到多少个分区：
- `k=1`: 只发送到最近的 1 个分区（unicast）
- `k=4`: 发送到最近的 4 个分区
- `k=8`: 发送到最近的 8 个分区
- `k=16`: 发送到所有 16 个分区（broadcast）

**预期行为**: 召回率应该随 k 单调递增，因为 k 越大，向量覆盖的分区越多，找到匹配的概率越高。

**实际行为**: 
- 修复前：k=4 召回率 ~84%，k=8 召回率 ~37%（反常！）
- 修复后：k=4 和 k=8 都是 ~100% 召回率（也不对！）

## 架构概述

```
┌─────────────────────────────────────────────────────────────────────┐
│                    数据流（Multicast 模式）                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  LeftSource ──┬── CentroidPartitioner ──┬──> Partition 0 (subtask 0)│
│               │   (multicast_k=4)       ├──> Partition 1 (subtask 1)│
│               │                         ├──> Partition 2 (subtask 2)│
│               │                         └──> Partition 3 (subtask 3)│
│               │                                                     │
│  RightSource ─┴── CentroidPartitioner ──┬──> Partition 0 (subtask 0)│
│                   (multicast_k=4)       ├──> Partition 1 (subtask 1)│
│                                         ├──> Partition 2 (subtask 2)│
│                                         └──> Partition 3 (subtask 3)│
│                                                                     │
│  每个 Partition 有独立的 WindowState，只搜索本分区内的数据           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 已修复的问题

### 问题 1: Owner-Computes 去重导致召回率下降

**症状**: k 越大，召回率反而越低

**原因**: ClusteredJoinMethod 中有 Owner-Computes 去重逻辑：
```cpp
// clustered_join_method.cpp (已移除)
bool is_owner = (std::min(left_uid, right_uid) % effective_parallelism_) == subtask_index_;
if (is_owner) {
    results.push_back(...);  // 只有 owner 分区输出
}
```

在 multicast 模式下，同一个匹配对可能在多个分区被找到，但只有 owner 分区输出。问题是：
- k=1 时，向量只去 1 个分区，碰巧是 owner 就输出
- k=4 时，向量去 4 个分区，非 owner 分区找到的匹配被丢弃
- k=8 时，向量去 8 个分区，更多匹配被丢弃！

**修复**: 移除 Owner-Computes，改为在 Sink 层统一去重（基于 `combined_id = left_uid * 1000000 + right_uid`）

### 问题 2: 全局时间戳导致分区模式下窗口被错误清空

**症状**: k=8 时大量窗口显示 `window_size=0, results=0`

**原因**: `JoinOperator` 使用全局的 `max_seen_left_ts_` 和 `max_seen_right_ts_` 进行窗口过期清理：
```cpp
// join_operator.cpp (修复前)
int64_t safe_ts_left = max_seen_left_ts_.load();  // 全局时间戳！
int64_t safe_ts_right = max_seen_right_ts_.load();
int64_t safe_evict_ts = std::min(safe_ts_left, safe_ts_right);
```

在 multicast 模式下，不同分区收到数据的顺序不同：
- 分区 A 可能只收到时间戳 0-500 的数据
- 分区 B 可能收到时间戳 500-1000 的数据
- 全局时间戳被更新为 1000
- 分区 A 的数据被错误地清空（因为 1000 - window_size > 500）

**修复**: 分区策略使用分区级别的时间戳：
```cpp
// join_operator.cpp (修复后)
if (isPartitionedStrategy()) {
    // 使用分区级别的时间戳
    int64_t safe_ts = state->getSafeEvictTimestamp(subtask_index, opposite_state);
}
```

## 当前问题

### 问题 3: k=4 和 k=8 都是 100% 召回率

**现象**:
```
k=4:  Recall=1.0000, TP=386456, Dedup=1952854
k=8:  Recall=1.0000, TP=386456, Dedup=~?
k=16: Recall=1.0000, TP=386456 (预期)
```

**预期**:
```
k=1:  Recall ~20-30%  (只覆盖 1/16 分区)
k=4:  Recall ~50-60%  (覆盖 4/16 分区)
k=8:  Recall ~70-80%  (覆盖 8/16 分区)
k=16: Recall=100%     (覆盖所有分区)
```

**可能原因**:

1. **冷启动广播模式覆盖了太多匹配**
   - `clustered_training_samples = 100`
   - 冷启动阶段（前 100 个样本）会广播到所有 16 个分区
   - 如果大部分匹配对都在冷启动阶段被处理，那么 k 值就不重要了

2. **测试数据的特性**
   - `positive_pairs = 500`, `near_threshold = 50`, `negative_pairs = 500`
   - 总共约 2400 个向量，每侧 1200 个
   - 如果匹配对的向量都很相似，它们会被分配到相同的分区
   - CentroidPartitioner 会把相似向量发到相同的分区！

3. **multicast 覆盖过度**
   - 即使 k=4，由于 K-means 聚类的特性，相似向量可能已经在同一个分区
   - multicast 只是增加了冗余覆盖，而不是"补充"缺失的覆盖

## 测试配置

```toml
# config/integration_test_cases.toml
[[test_case]]
name = "multicast_k_scan_k4"
algorithm = "clustered_join"
num_partitions = 16
clustered_multicast_k = 4
clustered_training_samples = 100  # 冷启动样本数
window_size_ms = 1000
data_sizes = [1000]
parallelism = [16]
```

## 关键数据

### multicast_k 扫描结果（最新）

| multicast_k | 记录数 (L/R) | Emits | TP | Dedup | Recall |
|-------------|-------------|-------|-----|-------|--------|
| k=4 | 21,588 | 2,235,984 | 386,456 | 1,952,854 | 100% |
| k=8 | 27,192 | ~2M | 386,456 | ~1.9M | 100% |
| k=16 | 38,400 | ~6M | 386,456 | ~5.6M | 100% |

### overlap_ratio 扫描结果（k=0 模式）

| overlap_ratio | 平均分区数 | Recall |
|---------------|-----------|--------|
| 0.01 | 1-2 | 44.5% |
| 0.02 | 2-4 | 57% |
| 0.05 | 4-7 | 93% |
| 0.10 | 7-8 | 100% |

## 待解决问题

1. **为什么 k=4 就能达到 100% 召回率？**
   - 需要分析 CentroidPartitioner 的分区策略
   - 检查相似向量是否被分配到相同的"最近"分区集合

2. **如何设计测试用例来展示 k 值的影响？**
   - 可能需要使用更分散的测试数据
   - 或者减少 `clustered_training_samples` 以减少冷启动影响

3. **冷启动期间的广播是否影响了测试结果？**
   - 前 100 个样本被广播到所有分区
   - 这 100 个样本中可能包含了大量匹配对

## 代码变更记录

### 1. 移除 Owner-Computes (clustered_join_method.cpp)

```diff
- // Owner-Computes: 只有 owner subtask 输出该匹配对
- bool is_owner = (effective_parallelism_ <= 1) || 
-                 ((std::min(left_uid, right_uid) % effective_parallelism_) == subtask_index);
- if (is_owner) {
-     results.push_back(std::make_unique<VectorRecord>(*record_ptr));
- }
+ // 直接输出所有匹配 - Sink 层会进行去重
+ results.push_back(std::make_unique<VectorRecord>(*record_ptr));
```

### 2. 添加 Sink 去重 (join_integration_pipeline_helper.cpp)

```cpp
void MatchCollectorSink::invoke(std::unique_ptr<VectorRecord>& record) {
    uint64_t combined_id = record->uid_;
    
    // Sink 层去重：相同的 combined_id 只处理一次
    if (!seen_ids_.insert(combined_id).second) {
        dedup_count_++;
        return;  // 重复，跳过
    }
    // ... 处理匹配
}
```

### 3. 分区模式使用分区级时间戳 (join_operator.cpp)

```diff
+ // 分区策略使用分区级别的时间戳进行 evict
+ if (isPartitionedStrategy()) {
+     int64_t safe_ts = state->getSafeEvictTimestamp(subtask_index, opposite_state);
+     state->evictExpired(safe_ts, window_size, subtask_index);
+ } else {
      // 共享策略使用全局时间戳
      int64_t safe_evict_ts = std::min(max_seen_left_ts_.load(), max_seen_right_ts_.load());
      state->evictExpired(safe_evict_ts, window_size, subtask_index);
+ }
```

### 4. ResultPartition 支持 multicast (result_partition.cpp)

```cpp
void ResultPartition::emit(Response&& data, int output_slot) {
    if (partitioner_->supportsMulticast() && !partitioner_->isBroadcast()) {
        // 多播模式：发送到多个目标分区
        auto targets = partitioner_->partitionMulti(data, queues_.size());
        for (size_t target : targets) {
            auto data_copy = data;  // 复制数据
            queues_[target]->push(std::move(data_copy));
        }
    }
    // ...
}
```

## 下一步

1. 分析 CentroidPartitioner 的分区策略，理解为什么相似向量会被分配到相同的分区
2. 设计更分散的测试数据，使匹配对的向量分布在不同的质心区域
3. 考虑是否需要调整测试参数（如减少冷启动样本数）

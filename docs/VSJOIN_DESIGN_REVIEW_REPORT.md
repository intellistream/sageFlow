# VSJoin 设计文档评审报告

**文档**: `docs/vsjoin_compliant_design_c745d987.plan.md`  
**评审日期**: 2026-01-15  
**评审人**: GitHub Copilot  

---

## 一、必须修复的阻塞问题 (P0)

### 1. `ConcurrencyManager::replaceIndex()` API 不存在

**问题描述**:  
文档第 4.2 节 `globalIndexRebuildLoop()` 中使用了不存在的 API：
```cpp
concurrency_manager_->replaceIndex(vsjoin_global_left_id_, new_left_index);
```

**现状**:  
`ConcurrencyManager` 只有 `create_index()`, `register_index()`, `drop_index()`, `insert()`, `erase()`, `query()` 方法。

**需要的修改**:  
1. 方案 A：扩展 `ConcurrencyManager`，新增 `replaceIndex(int index_id, std::shared_ptr<Index> new_index)` 方法
2. 方案 B：使用 "create_new → atomic_swap_id → drop_old" 模式，文档需要详细描述该流程

---

### 2. `LSHPartitioner` 缺少多播支持

**问题描述**:  
文档假设 `LSHPartitioner` 支持多播：
```cpp
lsh_partitioner->setMulticastEnabled(true);
lsh_partitioner->setMulticastK(strategy_config_.vsjoin_v2_multicast_k);
```

**现状**:  
- `LSHPartitioner` 没有 `setMulticastEnabled()` 和 `setMulticastK()` 方法
- `LSHPartitioner` 没有 `partitionMulti()` 方法返回多个目标分区
- `CentroidPartitioner` 已实现 `partitionMulti()` 可作为参考

**需要的修改**:  
1. 选择方案并在文档中说明：
   - 方案 A：扩展 `LSHPartitioner` 实现多播
   - 方案 B：复用 `CentroidPartitioner` 的多播逻辑
2. 补充 `LSHPartitioner` 的接口扩展设计

---

## 二、需要补充的关键设计 (P1)

### 3. 新配置字段未在 `JoinStrategyConfig` 中定义

**问题描述**:  
文档提到的新配置字段在现有代码中不存在：

| 文档中的字段 | 是否存在 |
|-------------|---------|
| `vsjoin_v2_multicast_k` | ❌ 不存在 |
| `vsjoin_v2_rebuild_interval_ms` | ❌ 不存在 |
| `vsjoin_v2_rebuild_threshold` | ❌ 不存在 |
| `vsjoin_v2_local_index_type` | ❌ 不存在 |
| `vsjoin_v2_global_index_type` | ❌ 不存在 |

**需要的修改**:  
在文档中补充完整的 `JoinStrategyConfig` 修改清单（包含类型、默认值、注释）。

---

### 4. `StrategyComponents` 扩展字段未详细说明

**问题描述**:  
文档提到在 `StrategyComponents` 中添加：
```cpp
std::vector<int> local_left_ids;
std::vector<int> local_right_ids;
int global_left_id;
int global_right_id;
```

**需要的修改**:  
补充 `StrategyComponents` 结构体的完整修改内容，包括初始化和清理逻辑。

---

### 5. Join 输出的去重策略未明确

**问题描述**:  
多播场景下，同一条记录被发送到多个分区，可能产生重复的 Join 输出结果。

**需要澄清**:  
1. Join 输出是否也会多播？
2. 使用 Sink 层统一去重（基于 `combined_id`）还是 Owner-Computes 规则？
3. 与 ClusteredJoin 的去重机制是否一致？

---

### 6. Global/Local 一致性窗口的召回影响

**问题描述**:  
Global Index 重建周期 5 秒，期间新记录只在 Local Index 中。文档提到 "清理 Local Index 中已合并的记录" 是可选的。

**需要澄清**:  
1. 是否保留 Local Index 中已合并到 Global 的记录？（建议保留，避免召回抖动）
2. 重建期间的查询一致性保证？

---

## 三、需要优化的设计细节 (P2)

### 7. 后台线程快速关闭机制

**问题描述**:  
当前设计使用 `std::this_thread::sleep_for()`，析构时需要等待 sleep 结束。

**建议修改**:  
使用 `std::condition_variable` + `wait_for()` 替代，支持快速唤醒：
```cpp
std::condition_variable rebuild_cv_;
std::mutex rebuild_mutex_;

// 在循环中
std::unique_lock<std::mutex> lock(rebuild_mutex_);
rebuild_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms),
                     [this] { return !rebuild_running_.load(); });
```

---

### 8. 冷启动行为未定义

**需要澄清**:  
1. Global Index 初始为空时的查询行为（只走 Local？）
2. 是否需要类似 ClusteredJoin 的 `enable_cold_start` 广播模式？
3. LSH 分区器是否需要训练？（实际不需要，但应在文档中说明）

---

### 9. `isPartitionedStrategy()` 的兼容性

**问题描述**:  
现有代码将 `LSH` 分区策略识别为 `PartitionedStrategy`：
```cpp
return strategy_config_.partition_strategy == PartitionStrategy::CENTROID ||
       strategy_config_.partition_strategy == PartitionStrategy::LSH;
```

VSJoin 的 "Global 共享 + Local 分区" 混合模式可能需要单独处理。

**需要澄清**:  
VSJoin 应该被识别为 `PartitionedStrategy` 还是需要新增策略类型？

---

## 四、需要补充的内容 (P3)

### 10. 量化验收标准

**需要补充**:  
| 指标 | 目标值 |
|-----|-------|
| 召回率（vs BruteForce） | ≥ ?% |
| 吞吐量（records/sec） | ≥ ? |
| P99 延迟（ms） | ≤ ? |
| 与 ClusteredJoin 对比 | ? |

---

### 11. 测试用例规划

**需要补充**:  
1. 单元测试清单（VSJoinMethodV2 的核心方法）
2. 集成测试 TOML 配置示例
3. 与现有 baseline 的对比测试用例

---

## 五、确认正确的设计点 ✓

以下设计符合现有架构，无需修改：

- ✓ 使用 `std::call_once` 保护后台线程启动
- ✓ 使用 `getRecordsSnapshot()` 线程安全访问 WindowState
- ✓ 复用 `TwoTierWindowState` 而非新建
- ✓ 每分区独立 index_id 的设计（方案 B）
- ✓ 通过 `ConcurrencyManager` 管理所有索引访问

---

## 六、修改优先级总结

| 优先级 | 问题编号 | 简述 |
|-------|---------|------|
| P0 | #1 | `replaceIndex()` API 不存在 |
| P0 | #2 | `LSHPartitioner` 无多播支持 |
| P1 | #3 | 新配置字段未定义 |
| P1 | #4 | `StrategyComponents` 扩展未说明 |
| P1 | #5 | Join 输出去重策略未明确 |
| P1 | #6 | Global/Local 一致性窗口 |
| P2 | #7 | 后台线程快速关闭 |
| P2 | #8 | 冷启动行为 |
| P2 | #9 | `isPartitionedStrategy()` 兼容性 |
| P3 | #10 | 量化验收标准 |
| P3 | #11 | 测试用例规划 |

---

**请在修改文档后，重新进行评审确认。**


# Task 01: VSJoinMethod 基础实现

## 任务概述

实现 VSJoin 双层索引查询的核心方法类 `VSJoinMethod`，继承自 `BaseMethod`，实现 `ExecuteEager()` 接口，协调 Global Index 和 Local Index 的双层查询逻辑。

**预估工时**: 2 天  
**依赖**: 无（基础任务）

**重要说明**：新的 VSJoin 实现将替换现有的 v1 版本（`vsjoin_method.h/cpp`），使用符合 SageFlow 架构约束的设计。

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 4 章: VSJoinMethod 实现
  - 第 11.4 节: 查询阶段的 UID 去重

## 实现要求

### 1. 创建文件

- **头文件**: `include/operator/join_operator_methods/vsjoin_method.h`（替换现有的 v1 版本）
- **实现文件**: `src/operator/join_operator_methods/vsjoin_method.cpp`（替换现有的 v1 版本）

### 2. 类定义要求

参考设计文档第 4.1 节，实现以下接口：

#### 2.1 核心接口

```cpp
class VSJoinMethod : public BaseMethod {
public:
    struct Config {
        double similarity_threshold = 0.8;
        int dimension = 128;
        int num_partitions = 8;
        int multicast_k = 2;  // 边界向量多播到 k 个分区（推荐 2-3）
        int64_t rebuild_interval_ms = 5000;
        size_t rebuild_threshold = 1000;
    };
    
    // 核心查询接口
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot,
        size_t subtask_index) override;
    
    // 初始化接口
    void initialize(const RuntimeContext& context,
                   std::shared_ptr<ConcurrencyManager> concurrency_manager);
    
    // 设置索引 ID
    void setGlobalIndexIds(int left_id, int right_id);
    void setLocalIndexIds(const std::vector<int>& left_ids, 
                          const std::vector<int>& right_ids);
    
    // 设置 WindowState
    void setWindowStates(WindowState* left_state, WindowState* right_state);
    
    // 设置分区器
    void setPartitioner(std::shared_ptr<VectorSpacePartitioner> partitioner);
    
    // 辅助方法
    int getLocalLeftIndexId(size_t subtask_index) const;
    int getLocalRightIndexId(size_t subtask_index) const;
    
    // 重建支持（可选，为后续任务预留）
    bool needsGlobalRebuild(size_t subtask_index) const;
    std::vector<const VectorRecord*> getRecordsForRebuild(size_t subtask_index) const;

private:
    // 内部查询方法
    std::vector<uint64_t> queryGlobalIndex(const VectorRecord& query, int target_index_id);
    std::vector<uint64_t> queryLocalIndex(const VectorRecord& query, 
                                          int query_slot,
                                          size_t subtask_index);
    std::vector<std::unique_ptr<VectorRecord>> resolveUidsToRecords(
        const std::vector<uint64_t>& uids, WindowState* state, size_t subtask_index);
    
    // 成员变量
    Config config_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    int global_left_id_ = -1;
    int global_right_id_ = -1;
    std::vector<int> local_left_ids_;
    std::vector<int> local_right_ids_;
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
};
```

### 3. ExecuteEager 实现要点

参考设计文档第 4.2 节，实现双层查询逻辑：

1. **确定目标索引和窗口状态**
   - 根据 `query_slot` 确定查询的是左流还是右流
   - `query_slot == 0` 表示查询右流，`query_slot == 1` 表示查询左流

2. **第一阶段：查询 Global Index（无锁）**
   - 调用 `queryGlobalIndex()` 查询共享的 Global Index
   - Global Index 是只读的，所有 subtask 共享，无需锁

3. **第二阶段：查询本分区 Local Index（无锁）**
   - 调用 `queryLocalIndex()` 查询本分区的 Local Index
   - **关键**: 只查询本分区，不查询邻近分区（边界向量已通过多播复制到本分区）
   - 本分区独占访问，完全无锁

4. **合并去重**
   - 使用 `std::unordered_set<uint64_t>` 合并 Global 和 Local 查询结果
   - 去重逻辑：`uid_set.insert(uid)`，自动去重
   - **性能**: O(n) 开销，n 通常 < 1000，性能影响可忽略

5. **过滤过期记录**
   - 遍历去重后的 UID，调用 `target_state->isExpired(uid, subtask_index)` 过滤过期记录

6. **解析 UID 到实际记录**
   - 调用 `resolveUidsToRecords()` 从 WindowState 获取实际记录

### 4. queryGlobalIndex 实现

```cpp
std::vector<uint64_t> VSJoinMethod::queryGlobalIndex(
    const VectorRecord& query, int target_index_id) {
    if (target_index_id < 0 || !concurrency_manager_) {
        return {};
    }
    
    // 通过 ConcurrencyManager 查询（内部处理并发）
    auto candidates = concurrency_manager_->query_for_join(
        target_index_id, query, config_.similarity_threshold, similarity_alpha_);
    
    std::vector<uint64_t> uids;
    for (const auto& c : candidates) {
        uids.push_back(c->uid_);
    }
    return uids;
}
```

### 5. queryLocalIndex 实现

```cpp
std::vector<uint64_t> VSJoinMethod::queryLocalIndex(
    const VectorRecord& query, int query_slot, size_t subtask_index) {
    if (!concurrency_manager_) {
        return {};
    }
    
    // 选择对侧的 Local 索引（只查询本分区）
    const auto& target_local_ids = (query_slot == 0) 
        ? local_right_ids_ : local_left_ids_;
    
    if (subtask_index >= target_local_ids.size()) {
        return {};
    }
    
    int local_index_id = target_local_ids[subtask_index];
    if (local_index_id < 0) {
        return {};
    }
    
    // 查询本分区的 Local Index（独占访问，无锁）
    auto candidates = concurrency_manager_->query_for_join(
        local_index_id, query, config_.similarity_threshold, similarity_alpha_);
    
    std::vector<uint64_t> uids;
    for (const auto& c : candidates) {
        uids.push_back(c->uid_);
    }
    return uids;
}
```

### 6. resolveUidsToRecords 实现

需要从 WindowState 中根据 UID 获取实际记录。参考 `WindowState` 接口：

```cpp
std::vector<std::unique_ptr<VectorRecord>> VSJoinMethod::resolveUidsToRecords(
    const std::vector<uint64_t>& uids, WindowState* state, size_t subtask_index) {
    std::vector<std::unique_ptr<VectorRecord>> results;
    
    // 从 WindowState 获取快照
    auto snapshot = state->getRecordsSnapshot(subtask_index);
    
    // 构建 UID 到记录的映射
    std::unordered_map<uint64_t, const VectorRecord*> uid_to_record;
    for (const auto& record : snapshot) {
        uid_to_record[record->uid_] = record.get();
    }
    
    // 根据 UID 列表获取记录
    for (uint64_t uid : uids) {
        auto it = uid_to_record.find(uid);
        if (it != uid_to_record.end()) {
            // 创建记录的副本
            results.push_back(std::make_unique<VectorRecord>(*it->second));
        }
    }
    
    return results;
}
```

## 关键设计点

1. **完全无锁查询**
   - Global Index: 只读，所有 subtask 共享，无需锁
   - Local Index: 本分区独占访问，无需锁

2. **只查询本分区**
   - **重要**: 不再查询邻近分区，因为边界向量已通过多播复制到本分区
   - 这保证了查询路径的简单性和无锁特性

3. **去重在查询结果合并时**
   - 使用 `unordered_set<uint64_t>` 高效去重
   - O(n) 开销，n 通常 < 1000，性能影响可忽略

## 测试要求

### 单元测试

创建 `test/operator/join_operator_methods/test_vsjoin_method.cpp`：

1. **基础查询测试**
   - 测试 Global Index 查询
   - 测试 Local Index 查询
   - 测试查询结果合并去重

2. **边界情况测试**
   - 空查询结果
   - 无效索引 ID
   - 无效 subtask_index

3. **并发安全测试**（可选）
   - 多线程并发查询 Global Index
   - 多线程并发查询不同分区的 Local Index

### 运行测试

```bash
cd build
ctest -R test_vsjoin_method
```

## 注意事项

1. **继承 BaseMethod**
   - 确保正确实现 `ExecuteEager()` 接口
   - 注意 `similarity_alpha_` 成员变量（从 BaseMethod 继承）

2. **内存管理**
   - `resolveUidsToRecords()` 返回的记录需要是 `unique_ptr`，确保内存安全

3. **日志记录**
   - 使用 `SAGEFLOW_LOG_DEBUG` 记录关键操作
   - 日志标签使用 `"VSJOIN_METHOD"`

4. **错误处理**
   - 检查索引 ID 有效性（>= 0）
   - 检查 `concurrency_manager_` 和 `state` 指针有效性

5. **代码风格**
   - 遵循 SageFlow 命名规范（camelBack 方法名，lower_case_ 成员变量）
   - 参考现有 JoinMethod 实现（如 `ivf_method.h/cpp`）

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] 代码符合 SageFlow 代码风格规范
- [ ] 实现了双层查询逻辑（Global + Local）
- [ ] 实现了查询结果合并去重
- [ ] 查询路径完全无锁
- [ ] 日志记录完整

## 后续任务

完成本任务后，可以继续：
- Task 02: JoinStrategyFactory 集成（创建 Global + Local 索引对）
- Task 05: 配置验证 + TOML 解析

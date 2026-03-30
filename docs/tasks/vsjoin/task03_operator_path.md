# Task 03: JoinOperator VSJoin 特殊路径

## 任务概述

在 `JoinOperator` 中实现 VSJoin 的特殊处理路径，包括：
1. 在 `updateSideWithState()` 中只插入到本分区的 Local Index
2. 在 `initializeWithStrategyConfig()` 中设置 VSJoin 相关索引 ID
3. 在 `getPreferredPartitioner()` 中返回 LSH 分区器（支持多播）

**预估工时**: 1 天  
**依赖**: Task 02 (JoinStrategyFactory 集成)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 5 章: JoinOperator 集成
  - 第 5.4 节: 分区路由与多播策略

## 实现要求

### 1. 修改文件

- **头文件**: `include/operator/join_operator.h`
- **实现文件**: `src/operator/join_operator.cpp`

### 2. 添加成员变量

在 `JoinOperator` 类中添加 VSJoin 专用成员：

```cpp
class JoinOperator final : public Operator {
private:
    // ... 现有成员 ...
    
    // ==================== VSJoin 专用 ====================
    // Local Index ID 数组（每分区独立）
    std::vector<int> vsjoin_local_left_ids_;   // size = parallelism_
    std::vector<int> vsjoin_local_right_ids_;  // size = parallelism_
    
    // Global Index ID（共享只读）
    int vsjoin_global_left_id_ = -1;
    int vsjoin_global_right_id_ = -1;
};
```

### 3. 修改 initializeWithStrategyConfig()

在 `initializeWithStrategyConfig()` 中添加 VSJoin 特殊处理：

```cpp
void JoinOperator::initializeWithStrategyConfig(const RuntimeContext& context) {
    // ... 现有逻辑 ...
    
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // 从 StrategyComponents 获取索引 ID
        vsjoin_global_left_id_ = components.global_left_id;
        vsjoin_global_right_id_ = components.global_right_id;
        vsjoin_local_left_ids_ = components.local_left_ids;
        vsjoin_local_right_ids_ = components.local_right_ids;
        
        // 传递给 VSJoinMethod
        auto* vsjoin_method = dynamic_cast<VSJoinMethod*>(join_method_.get());
        if (vsjoin_method) {
            vsjoin_method->setGlobalIndexIds(
                vsjoin_global_left_id_, vsjoin_global_right_id_);
            vsjoin_method->setLocalIndexIds(
                vsjoin_local_left_ids_, vsjoin_local_right_ids_);
            vsjoin_method->setWindowStates(left_state_.get(), right_state_.get());
        }
    }
}
```

### 4. 修改 updateSideWithState()

在 `updateSideWithState()` 中添加 VSJoin 特殊处理：

```cpp
auto JoinOperator::updateSideWithState(
    WindowState* state,
    int index_id_for_cc,
    std::unique_ptr<VectorRecord> data_ptr,
    int64_t now_time_stamp,
    int slot,
    size_t subtask_index) -> bool {
    
    // ... 现有逻辑（WindowState 更新等） ...
    
    // VSJoin 特殊处理：只插入到本分区的 Local Index
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // 选择本分区对应的 Local Index ID
        const auto& local_ids = (slot == left_slot_id_) 
            ? vsjoin_local_left_ids_ : vsjoin_local_right_ids_;
        
        int local_index_id = (subtask_index < local_ids.size()) 
            ? local_ids[subtask_index] : -1;
        
        if (local_index_id >= 0 && concurrency_manager_) {
            // 本分区独占访问，无锁插入
            concurrency_manager_->insert(local_index_id, std::move(data_for_index_insert));
        }
        
        // Global Index 不在此处插入，由后台重建线程处理
        SAGEFLOW_LOG_DEBUG("VSJOIN", "subtask_{} inserted to local_id={}", 
                          subtask_index, local_index_id);
    } else {
        // 其他算法的正常插入逻辑
        if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
            concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
        }
    }
    
    // ... 其余逻辑 ...
}
```

**关键点**：
- VSJoin 只插入到本分区的 Local Index（`local_ids[subtask_index]`）
- Global Index 不在此处插入，由后台重建线程处理（Task 04）
- 本分区独占访问，无锁插入

### 5. 修改 getPreferredPartitioner()

在 `getPreferredPartitioner()` 中添加 VSJoin 特殊处理：

```cpp
std::unique_ptr<IPartitioner> JoinOperator::getPreferredPartitioner(
    int dimension, int num_partitions) const override {
    
    // VSJoin 使用 LSH 分区器 + 多播策略
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // 创建 LSH 分区器（支持多播）
        auto lsh_partitioner = std::make_unique<LSHPartitionerAdapter>(
            strategy_config_.dimension,
            strategy_config_.vsjoin_num_hash_functions,
            strategy_config_.vsjoin_boundary_threshold);
        
        // 启用多播（边界向量复制到 k 个分区）
        lsh_partitioner->setMulticastEnabled(true);
        lsh_partitioner->setMulticastK(strategy_config_.vsjoin_multicast_k);
        
        return lsh_partitioner;
    }
    
    // ... 其他算法的分区器选择逻辑 ...
}
```

**关键点**：
- VSJoin 使用 LSH 分区器（`LSHPartitionerAdapter`）
- 启用多播（`setMulticastEnabled(true)`）
- 设置多播参数 k（`setMulticastK()`）

## 关键设计点

1. **索引插入策略**
   - VSJoin: 只插入到本分区的 Local Index
   - 其他算法: 使用 `index_id_for_cc` 插入到共享索引

2. **分区路由**
   - VSJoin 使用 LSH 分区器 + 多播策略
   - 边界向量会被复制到 k 个分区（推荐 k=2-3）
   - 非边界向量路由到主分区（单播）

3. **数据流（多播模式）**
   ```
   Source → LSHPartitioner (multicast_k=2)
            ├─ 主分区 → subtask_i → Local Index i
            └─ 边界分区 → subtask_j → Local Index j (复制)
   ```

4. **多播策略**
   - **非边界向量**: 路由到主分区（单播）
   - **边界向量**: 路由到主分区 + k-1 个邻近分区（多播）
   - **查询时**: 只查本分区，边界向量已通过多播保证存在

## 测试要求

### 单元测试

创建 `test/operator/test_vsjoin_operator_path.cpp`：

1. **索引插入测试**
   - 验证 VSJoin 只插入到本分区的 Local Index
   - 验证 Global Index 不在此处插入
   - 验证其他算法不受影响

2. **分区路由测试**
   - 验证 VSJoin 使用 LSH 分区器
   - 验证多播功能启用
   - 验证多播参数 k 设置正确

3. **初始化测试**
   - 验证 `initializeWithStrategyConfig()` 正确设置索引 ID
   - 验证 VSJoinMethod 正确接收索引 ID 和 WindowState

4. **并发测试**（可选）
   - 多线程并发插入到不同分区的 Local Index
   - 验证无锁插入的正确性

### 运行测试

```bash
cd build
ctest -R test_vsjoin_operator_path
```

## 注意事项

1. **索引 ID 有效性检查**
   - 检查 `local_index_id >= 0` 和 `subtask_index < local_ids.size()`
   - 检查 `concurrency_manager_` 指针有效性

2. **日志记录**
   - 使用 `SAGEFLOW_LOG_DEBUG` 记录插入操作
   - 日志标签使用 `"VSJOIN"`

3. **向后兼容**
   - 确保其他算法的逻辑不受影响
   - 使用 `if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN)` 隔离 VSJoin 特殊处理

4. **LSHPartitionerAdapter 接口**
   - 确认 `LSHPartitionerAdapter` 支持多播功能
   - 如果不存在，需要先实现或使用替代方案

5. **配置参数**
   - `vsjoin_num_hash_functions`: LSH 哈希函数数量
   - `vsjoin_boundary_threshold`: 边界向量阈值
   - `vsjoin_multicast_k`: 多播参数 k（推荐 2-3）

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] VSJoin 只插入到本分区的 Local Index
- [ ] Global Index 不在此处插入
- [ ] VSJoin 使用 LSH 分区器 + 多播策略
- [ ] 其他算法不受影响
- [ ] 日志记录完整

## 后续任务

完成本任务后，可以继续：
- Task 04: 后台重建机制 GlobalIndexRebuilder（含局部 unordered_set 去重）
- Task 07: AssignmentTable (RCU) + LoadMonitor 实现

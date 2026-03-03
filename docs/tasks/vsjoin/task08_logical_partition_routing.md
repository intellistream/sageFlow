# Task 08: Logical Partition 路由集成

## 任务概述

实现 Logical Partition 路由功能，将 LSH 分区器扩展为支持 logical partition，并集成 AssignmentTable 实现逻辑分区到物理 subtask 的映射。

**预估工时**: 1 天  
**依赖**: Task 07 (AssignmentTable + LoadMonitor)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 12.3.1 节: Logical Partition 拆分
  - 第 12.3.4 节: 路由流程

## 实现要求

### 1. 修改文件

- **头文件**: `include/operator/join_operator.h`
- **实现文件**: `src/operator/join_operator.cpp`
- **分区器文件**: `include/operator/utils/lsh_partitioner_adapter.h`（如果存在）

### 2. 扩展 LSHPartitionerAdapter

修改 `LSHPartitionerAdapter` 支持 logical partition：

```cpp
class LSHPartitionerAdapter : public IPartitioner {
public:
    // ... 现有接口 ...
    
    // 设置 logical partition 数量（P * V）
    void setLogicalPartitionCount(size_t num_logical_partitions);
    
    // 获取 logical partition ID（替代原来的物理分区 ID）
    int getLogicalPartitionId(const VectorRecord& record) const override;
    
    // 获取多播的 logical partition IDs
    std::vector<int> getMulticastLogicalPartitionIds(const VectorRecord& record) const;
    
private:
    size_t num_logical_partitions_ = 0;
    size_t num_physical_partitions_ = 0;
    size_t virtual_nodes_per_partition_ = 1;  // V
};
```

**实现逻辑**：

```cpp
int LSHPartitionerAdapter::getLogicalPartitionId(const VectorRecord& record) const {
    // 1. 计算 LSH hash（得到物理分区 ID）
    int physical_pid = computeLSHHash(record);
    
    // 2. 转换为 logical partition ID
    // logical_pid = physical_pid * V + v_idx
    // 其中 v_idx 可以是 0（简单情况）或基于向量的额外哈希
    int v_idx = computeVirtualNodeIndex(record, physical_pid);
    int logical_pid = physical_pid * virtual_nodes_per_partition_ + v_idx;
    
    return logical_pid;
}

std::vector<int> LSHPartitionerAdapter::getMulticastLogicalPartitionIds(
    const VectorRecord& record) const {
    std::vector<int> logical_pids;
    
    // 1. 获取主 logical partition ID
    int main_logical_pid = getLogicalPartitionId(record);
    logical_pids.push_back(main_logical_pid);
    
    // 2. 判断是否为边界向量
    if (isBoundaryVector(record)) {
        // 3. 获取邻近 logical partition IDs（多播）
        auto neighbor_logical_pids = getNeighborLogicalPartitionIds(main_logical_pid);
        logical_pids.insert(logical_pids.end(), 
                           neighbor_logical_pids.begin(), 
                           neighbor_logical_pids.end());
    }
    
    return logical_pids;
}
```

### 3. 集成 AssignmentTable 到 JoinOperator

在 `JoinOperator` 中添加 AssignmentTable 和路由逻辑：

```cpp
class JoinOperator : public Operator {
private:
    // ... 现有成员 ...
    
    // ==================== VSJoin 负载均衡 ====================
    std::unique_ptr<VSJoinPartitionAssignment> partition_assignment_;
    std::unique_ptr<VSJoinLoadMonitor> load_monitor_;
    size_t num_logical_partitions_ = 0;  // P * V
    size_t virtual_nodes_per_partition_ = 8;  // V
    
    // 路由逻辑
    std::vector<size_t> routeToPhysicalSubtasks(
        const std::vector<int>& logical_pids) const;
};
```

**路由实现**：

```cpp
std::vector<size_t> JoinOperator::routeToPhysicalSubtasks(
    const std::vector<int>& logical_pids) const {
    std::vector<size_t> physical_subtasks;
    
    if (!partition_assignment_) {
        // 如果没有 AssignmentTable，直接使用 logical_pid % parallelism_
        for (int logical_pid : logical_pids) {
            physical_subtasks.push_back(logical_pid % parallelism_);
        }
        return physical_subtasks;
    }
    
    // 通过 AssignmentTable 获取 physical subtask
    for (int logical_pid : logical_pids) {
        int physical_subtask = partition_assignment_->getPhysicalSubtask(logical_pid);
        if (physical_subtask >= 0) {
            physical_subtasks.push_back(static_cast<size_t>(physical_subtask));
        }
    }
    
    return physical_subtasks;
}
```

### 4. 修改 getPreferredPartitioner()

在 `getPreferredPartitioner()` 中设置 logical partition 参数：

```cpp
std::unique_ptr<IPartitioner> JoinOperator::getPreferredPartitioner(
    int dimension, int num_partitions) const override {
    
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        auto lsh_partitioner = std::make_unique<LSHPartitionerAdapter>(
            strategy_config_.dimension,
            strategy_config_.vsjoin_num_hash_functions,
            strategy_config_.vsjoin_boundary_threshold);
        
        // 启用多播
        lsh_partitioner->setMulticastEnabled(true);
        lsh_partitioner->setMulticastK(strategy_config_.vsjoin_multicast_k);
        
        // 设置 logical partition 数量
        num_logical_partitions_ = parallelism_ * virtual_nodes_per_partition_;
        lsh_partitioner->setLogicalPartitionCount(num_logical_partitions_);
        
        return lsh_partitioner;
    }
    
    // ... 其他算法的分区器选择逻辑 ...
}
```

### 5. 初始化 AssignmentTable

在 `initializeWithStrategyConfig()` 中初始化 AssignmentTable：

```cpp
void JoinOperator::initializeWithStrategyConfig(const RuntimeContext& context) {
    // ... 现有逻辑 ...
    
    if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
        // ... 现有 VSJoin 初始化逻辑 ...
        
        // 初始化 AssignmentTable
        num_logical_partitions_ = parallelism_ * virtual_nodes_per_partition_;
        partition_assignment_ = std::make_unique<VSJoinPartitionAssignment>(
            num_logical_partitions_, parallelism_);
        
        // 初始化 LoadMonitor
        load_monitor_ = std::make_unique<VSJoinLoadMonitor>(parallelism_);
    }
}
```

## 关键设计点

1. **Logical Partition 拆分**
   - `logical_partitions = P * V`（P = 物理分区数，V = 虚拟节点数，推荐 8）
   - `logical_pid = physical_pid * V + v_idx`

2. **路由流程**
   ```
   Source → LSHPartitioner → logical_pid [0, P*V) 
          → AssignmentTable → physical_subtask [0, P)
          → ExecutionVertex → WindowState + Local Index
   ```

3. **多播支持**
   - 边界向量返回多个 logical_pid
   - 每个 logical_pid 通过 AssignmentTable 映射到 physical_subtask
   - 记录被路由到对应的多个 subtask

4. **初始化策略**
   - 初始时使用简单轮询：`logical_pid % P`
   - 后续可以通过负载均衡器动态调整

## 测试要求

### 单元测试

创建 `test/operator/test_vsjoin_routing.cpp`：

1. **Logical Partition 路由测试**
   - 验证 logical_pid 正确计算
   - 验证 logical_pid 到 physical_subtask 的映射正确

2. **多播路由测试**
   - 验证边界向量路由到多个 subtask
   - 验证非边界向量路由到单个 subtask

3. **AssignmentTable 集成测试**
   - 验证路由通过 AssignmentTable
   - 验证 AssignmentTable 更新后路由正确更新

### 运行测试

```bash
cd build
ctest -R test_vsjoin_routing
```

## 注意事项

1. **向后兼容**
   - 如果没有 AssignmentTable，使用简单路由（`logical_pid % parallelism_`）
   - 确保现有代码不受影响

2. **Virtual Nodes 数量**
   - 推荐 V = 8 或 16
   - 可以通过配置参数设置

3. **LSHPartitionerAdapter 修改**
   - 如果 `LSHPartitionerAdapter` 不存在，需要先实现
   - 或者使用现有的 LSH 分区器并扩展

4. **日志记录**
   - 使用 `SAGEFLOW_LOG_DEBUG` 记录路由信息
   - 日志标签使用 `"VSJOIN_ROUTING"`

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] Logical Partition 路由正确
- [ ] 多播路由正确
- [ ] AssignmentTable 集成正确
- [ ] 向后兼容性保证

## 后续任务

完成本任务后，可以继续：
- Task 09: 负载均衡测试（AssignmentTable 并发安全 + 负载均衡效果）

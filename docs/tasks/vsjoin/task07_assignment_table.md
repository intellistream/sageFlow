# Task 07: AssignmentTable (RCU) + LoadMonitor 实现

## 任务概述

实现 VSJoin 的负载均衡组件，包括：
1. `VSJoinPartitionAssignment`：使用 RCU (Read-Copy-Update) 实现逻辑分区到物理 subtask 的映射
2. `VSJoinLoadMonitor`：采样和聚合各 subtask 的负载信息

**预估工时**: 2 天  
**依赖**: Task 03 (JoinOperator VSJoin 特殊路径)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 12.3.2 节: AssignmentTable RCU 并发安全设计
  - 第 12.3.3 节: LoadMonitor 采样负载信息
  - 第 13.1.2 节: AssignmentTable 并发访问注意事项

## 实现要求

### 1. 创建文件

- **头文件**: `include/operator/join_operator_methods/vsjoin_components/partition_assignment.h`
- **实现文件**: `src/operator/join_operator_methods/vsjoin_components/partition_assignment.cpp`
- **头文件**: `include/operator/join_operator_methods/vsjoin_components/load_monitor.h`
- **实现文件**: `src/operator/join_operator_methods/vsjoin_components/load_monitor.cpp`

### 2. 实现 VSJoinPartitionAssignment

参考设计文档第 12.3.2 节，实现 RCU 方案：

```cpp
// partition_assignment.h
class VSJoinPartitionAssignment {
public:
    explicit VSJoinPartitionAssignment(
        size_t num_logical_partitions, 
        size_t num_physical_subtasks);
    
    // ==================== 读操作（高频，完全无锁） ====================
    int getPhysicalSubtask(int logical_pid) const;
    
    // ==================== 写操作（低频，批量更新） ====================
    void updateMapping(const std::vector<std::pair<int, int>>& updates);
    void setPhysicalSubtask(int logical_pid, int physical_subtask);
    
    // 获取当前映射表快照（用于调试）
    std::vector<int> getCurrentMapping() const;

private:
    size_t num_logical_;
    size_t num_physical_;
    
    // 双缓冲：两个映射表实例
    std::unique_ptr<std::vector<int>> current_table_;  // 当前版本（读）
    std::unique_ptr<std::vector<int>> next_table_;      // 准备版本（写）
    
    // 原子指针：指向当前可读的映射表
    std::atomic<std::vector<int>*> current_ptr_;
    
    // 写互斥锁：保护 next_table_ 的更新过程（避免并发写冲突）
    mutable std::mutex write_mutex_;
};
```

**关键实现点**：

1. **读操作（完全无锁）**
   ```cpp
   int VSJoinPartitionAssignment::getPhysicalSubtask(int logical_pid) const {
       // 原子读取当前指针（memory_order_acquire 确保看到最新的映射表）
       std::vector<int>* table = current_ptr_.load(std::memory_order_acquire);
       
       if (logical_pid < 0 || static_cast<size_t>(logical_pid) >= num_logical_) {
           return -1;
       }
       
       // 直接访问数组元素（无锁）
       return (*table)[logical_pid];
   }
   ```

2. **写操作（批量更新原子性）**
   ```cpp
   void VSJoinPartitionAssignment::updateMapping(
       const std::vector<std::pair<int, int>>& updates) {
       // 1. 在 next_table_ 上准备新映射（复制当前版本）
       {
           std::lock_guard<std::mutex> lock(write_mutex_);
           *next_table_ = *current_table_;  // 复制当前版本
           
           // 2. 应用批量更新
           for (const auto& [logical_pid, physical_subtask] : updates) {
               if (logical_pid >= 0 && static_cast<size_t>(logical_pid) < num_logical_ &&
                   physical_subtask >= 0 && static_cast<size_t>(physical_subtask) < num_physical_) {
                   (*next_table_)[logical_pid] = physical_subtask;
               }
           }
       }
       
       // 3. 原子切换指针（memory_order_release 确保新映射表对所有后续读操作可见）
       current_ptr_.store(next_table_.get(), std::memory_order_release);
       
       // 4. 交换指针，为下次更新做准备（避免每次都重新分配内存）
       std::swap(current_table_, next_table_);
   }
   ```

### 3. 实现 VSJoinLoadMonitor

参考设计文档第 12.3.3 节：

```cpp
// load_monitor.h
struct LoadStat {
    size_t subtask_index;
    size_t record_count;        // 最近窗口内的输入记录数
    double avg_latency_ms;      // 平均处理时延（可选）
    size_t queue_backlog;       // 当前队列 backlog（如能获取）
    std::chrono::steady_clock::time_point last_update;
};

class VSJoinLoadMonitor {
public:
    explicit VSJoinLoadMonitor(size_t num_subtasks);
    
    // 上报负载信息（由各 subtask 调用）
    void reportLoad(size_t subtask_index, size_t record_count, 
                   double avg_latency_ms = 0.0, size_t queue_backlog = 0);
    
    // 获取负载统计（由负载均衡器调用）
    std::vector<LoadStat> getLoadStats() const;
    
    // 计算平均负载
    double getAverageLoad() const;
    
    // 获取最忙和最空闲的 subtask
    size_t getBusiestSubtask() const;
    size_t getIdlestSubtask() const;

private:
    size_t num_subtasks_;
    mutable std::mutex stats_mutex_;
    std::vector<LoadStat> subtask_loads_;
};
```

**实现要点**：

1. **负载上报**
   ```cpp
   void VSJoinLoadMonitor::reportLoad(size_t subtask_index, 
                                      size_t record_count,
                                      double avg_latency_ms,
                                      size_t queue_backlog) {
       std::lock_guard<std::mutex> lock(stats_mutex_);
       
       if (subtask_index < subtask_loads_.size()) {
           subtask_loads_[subtask_index].subtask_index = subtask_index;
           subtask_loads_[subtask_index].record_count = record_count;
           subtask_loads_[subtask_index].avg_latency_ms = avg_latency_ms;
           subtask_loads_[subtask_index].queue_backlog = queue_backlog;
           subtask_loads_[subtask_index].last_update = 
               std::chrono::steady_clock::now();
       }
   }
   ```

2. **负载统计获取**
   ```cpp
   std::vector<LoadStat> VSJoinLoadMonitor::getLoadStats() const {
       std::lock_guard<std::mutex> lock(stats_mutex_);
       return subtask_loads_;
   }
   ```

## 关键设计点

### 1. RCU 并发安全

- **读操作完全无锁**：`atomic_ptr.load()` + 数组访问，开销 ~2ns
- **批量更新原子性**：通过原子指针切换，读操作要么看到旧版本，要么看到新版本
- **内存可见性**：使用 `std::memory_order_acquire/release` 保证内存可见性

### 2. 避免大规模内存拷贝

- 映射表小（~4KB），只在更新时复制一次
- 使用 `std::swap` 交换指针，避免重复分配内存

### 3. 内存安全

- `current_table_` 和 `next_table_` 的生命周期由类管理
- 指针切换后，旧版本会被保留在 `next_table_` 中，直到下次更新时被覆盖

### 4. 性能分析

- **读操作开销**：~2ns，完全无锁
- **写操作开销**：复制映射表（~1μs for 1KB）+ 批量更新 + 原子指针切换
- **内存开销**：双倍映射表（~8KB），可接受

## 测试要求

### 单元测试

创建 `test/operator/join_operator_methods/vsjoin_components/test_partition_assignment.cpp`：

1. **RCU 并发安全测试**
   - 多线程并发读操作
   - 单线程写操作（批量更新）
   - 验证读操作无锁，写操作原子性

2. **批量更新测试**
   - 验证批量更新的原子性
   - 验证读操作要么看到旧版本，要么看到新版本

3. **性能测试**
   - 测量读操作延迟（应该 ~2ns）
   - 测量写操作延迟

创建 `test/operator/join_operator_methods/vsjoin_components/test_load_monitor.cpp`：

1. **负载上报测试**
   - 验证负载信息正确上报
   - 验证多线程并发上报

2. **负载统计测试**
   - 验证平均负载计算正确
   - 验证最忙/最空闲 subtask 识别正确

### 运行测试

```bash
cd build
ctest -R test_partition_assignment
ctest -R test_load_monitor
```

## 注意事项

1. **RCU 实现必须正确**
   - ⚠️ **重要**：必须使用 `std::memory_order_acquire/release` 保证内存可见性
   - 读操作使用 `memory_order_acquire`
   - 写操作使用 `memory_order_release`

2. **内存拷贝开销**
   - 映射表小（~4KB），只在更新时复制一次，开销可接受
   - 使用 `std::swap` 避免重复分配内存

3. **线程安全**
   - LoadMonitor 使用 `std::mutex` 保护负载统计
   - AssignmentTable 读操作无锁，写操作使用 `std::mutex` 保护批量更新

4. **错误处理**
   - 检查 `logical_pid` 和 `physical_subtask` 有效性
   - 检查 `subtask_index` 有效性

5. **日志记录**
   - 使用 `SAGEFLOW_LOG_DEBUG` 记录关键操作
   - 日志标签使用 `"VSJOIN_ASSIGNMENT"` 和 `"VSJOIN_LOAD_MONITOR"`

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] RCU 读操作完全无锁
- [ ] 批量更新原子性验证通过
- [ ] 负载监控功能正确
- [ ] 性能测试通过（读操作 ~2ns）
- [ ] 内存开销可接受（~8KB）

## 后续任务

完成本任务后，可以继续：
- Task 08: Logical Partition 路由集成（LSHPartitioner 扩展）

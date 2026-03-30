# Task 09: 负载均衡测试

## 任务概述

实现 VSJoin 负载均衡功能的测试，验证：
1. AssignmentTable 并发安全性（RCU 读操作无锁，批量更新原子性）
2. 负载均衡效果（Logical Partition 分配调整后负载是否均衡）
3. LoadMonitor 功能正确性

**预估工时**: 0.5 天  
**依赖**: Task 08 (Logical Partition 路由集成)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 12.3.6 节: 第一版落地范围
  - 第 13.1.2 节: AssignmentTable 并发访问注意事项

## 实现要求

### 1. 创建测试文件

- **测试文件**: `test/operator/test_vsjoin_load_balancing.cpp`

### 2. 测试用例设计

#### 2.1 AssignmentTable 并发安全测试

```cpp
TEST(VSJoinLoadBalancingTest, AssignmentTableConcurrentRead) {
    // 1. 创建 AssignmentTable
    VSJoinPartitionAssignment assignment(128, 8);  // 128 logical partitions, 8 physical subtasks
    
    // 2. 多线程并发读操作
    const int num_threads = 16;
    const int reads_per_thread = 10000;
    std::vector<std::thread> threads;
    std::atomic<int> total_reads{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&assignment, &total_reads, reads_per_thread]() {
            for (int i = 0; i < reads_per_thread; ++i) {
                int logical_pid = i % 128;
                int physical_subtask = assignment.getPhysicalSubtask(logical_pid);
                ASSERT_GE(physical_subtask, 0);
                ASSERT_LT(physical_subtask, 8);
                total_reads.fetch_add(1);
            }
        });
    }
    
    // 3. 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    // 4. 验证所有读操作成功
    EXPECT_EQ(total_reads.load(), num_threads * reads_per_thread);
}
```

#### 2.2 AssignmentTable 批量更新原子性测试

```cpp
TEST(VSJoinLoadBalancingTest, AssignmentTableBatchUpdateAtomicity) {
    // 1. 创建 AssignmentTable
    VSJoinPartitionAssignment assignment(128, 8);
    
    // 2. 准备批量更新
    std::vector<std::pair<int, int>> updates;
    for (int i = 0; i < 64; ++i) {
        updates.push_back({i, (i + 1) % 8});  // 将前 64 个 logical partition 重新分配
    }
    
    // 3. 执行批量更新
    assignment.updateMapping(updates);
    
    // 4. 验证更新原子性
    // 所有读操作应该要么看到旧版本，要么看到新版本，不会看到部分更新
    for (int i = 0; i < 64; ++i) {
        int physical_subtask = assignment.getPhysicalSubtask(i);
        // 应该看到新版本（(i + 1) % 8）或旧版本（i % 8），不能是其他值
        EXPECT_TRUE(physical_subtask == (i + 1) % 8 || physical_subtask == i % 8);
    }
}
```

#### 2.3 LoadMonitor 功能测试

```cpp
TEST(VSJoinLoadBalancingTest, LoadMonitorFunctionality) {
    // 1. 创建 LoadMonitor
    VSJoinLoadMonitor monitor(8);  // 8 个 subtask
    
    // 2. 上报负载信息
    monitor.reportLoad(0, 1000, 10.5, 50);  // subtask 0: 1000 条记录，10.5ms 延迟，50 队列积压
    monitor.reportLoad(1, 500, 5.0, 20);
    monitor.reportLoad(2, 2000, 20.0, 100);  // subtask 2: 最忙
    
    // 3. 获取负载统计
    auto stats = monitor.getLoadStats();
    EXPECT_EQ(stats.size(), 8);
    
    // 4. 验证负载统计正确
    EXPECT_EQ(stats[0].record_count, 1000);
    EXPECT_EQ(stats[1].record_count, 500);
    EXPECT_EQ(stats[2].record_count, 2000);
    
    // 5. 验证最忙/最空闲 subtask 识别
    EXPECT_EQ(monitor.getBusiestSubtask(), 2);
    EXPECT_EQ(monitor.getIdlestSubtask(), 1);
    
    // 6. 验证平均负载计算
    double avg_load = monitor.getAverageLoad();
    EXPECT_NEAR(avg_load, (1000 + 500 + 2000) / 3.0, 0.1);
}
```

#### 2.4 负载均衡效果测试

```cpp
TEST(VSJoinLoadBalancingTest, LoadBalancingEffectiveness) {
    // 1. 创建模拟负载不均的场景
    // subtask 0-1: 高负载（2000 条记录）
    // subtask 2-7: 低负载（100 条记录）
    
    VSJoinLoadMonitor monitor(8);
    for (int i = 0; i < 2; ++i) {
        monitor.reportLoad(i, 2000, 20.0, 100);
    }
    for (int i = 2; i < 8; ++i) {
        monitor.reportLoad(i, 100, 1.0, 5);
    }
    
    // 2. 计算负载不均衡度
    double avg_load = monitor.getAverageLoad();
    double max_load = 2000.0;
    double imbalance_ratio = max_load / avg_load;
    EXPECT_GT(imbalance_ratio, 1.5);  // 负载不均衡度 > 1.5
    
    // 3. 执行负载均衡（调整 AssignmentTable）
    VSJoinPartitionAssignment assignment(128, 8);
    
    // 从最忙的 subtask (0, 1) 迁移部分 logical partition 到最空闲的 subtask (2-7)
    std::vector<std::pair<int, int>> rebalance_updates;
    for (int i = 0; i < 32; ++i) {
        // 将 logical partition i 从 subtask 0 迁移到 subtask 2
        rebalance_updates.push_back({i, 2});
    }
    for (int i = 32; i < 64; ++i) {
        // 将 logical partition i 从 subtask 1 迁移到 subtask 3
        rebalance_updates.push_back({i, 3});
    }
    
    assignment.updateMapping(rebalance_updates);
    
    // 4. 验证负载均衡效果（模拟）
    // 注意：实际负载均衡效果需要通过运行时的真实负载来验证
    // 这里主要验证 AssignmentTable 更新正确
    for (int i = 0; i < 32; ++i) {
        EXPECT_EQ(assignment.getPhysicalSubtask(i), 2);
    }
    for (int i = 32; i < 64; ++i) {
        EXPECT_EQ(assignment.getPhysicalSubtask(i), 3);
    }
}
```

#### 2.5 性能测试

```cpp
TEST(VSJoinLoadBalancingTest, AssignmentTablePerformance) {
    VSJoinPartitionAssignment assignment(1024, 16);
    
    // 测量读操作延迟
    auto start = std::chrono::high_resolution_clock::now();
    const int num_reads = 1000000;
    for (int i = 0; i < num_reads; ++i) {
        assignment.getPhysicalSubtask(i % 1024);
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    double avg_latency_ns = duration.count() / static_cast<double>(num_reads);
    
    // 读操作延迟应该 < 10ns（目标 ~2ns）
    EXPECT_LT(avg_latency_ns, 10.0);
    
    SAGEFLOW_LOG_INFO("VSJOIN_PERF", 
        "AssignmentTable read latency: {} ns", avg_latency_ns);
}
```

## 关键测试点

### 1. 并发安全验证

- **读操作无锁**：多线程并发读操作，验证无数据竞争
- **批量更新原子性**：验证读操作要么看到旧版本，要么看到新版本

### 2. 负载均衡效果验证

- **负载不均衡检测**：验证 LoadMonitor 能正确识别负载不均衡
- **负载均衡调整**：验证 AssignmentTable 更新后路由正确调整
- **负载均衡效果**：验证调整后负载更均衡（需要通过运行时测试）

### 3. 性能验证

- **读操作延迟**：应该 < 10ns（目标 ~2ns）
- **写操作延迟**：批量更新延迟可接受（~1μs for 1KB）

## 测试要求

### 运行测试

```bash
cd build
ctest -R test_vsjoin_load_balancing
```

### 测试覆盖率

- [ ] AssignmentTable 并发读测试
- [ ] AssignmentTable 批量更新原子性测试
- [ ] LoadMonitor 功能测试
- [ ] 负载均衡效果测试
- [ ] 性能测试

## 注意事项

1. **并发测试**
   - 使用 `std::thread` 创建多线程
   - 使用 `std::atomic` 统计操作次数
   - 验证无数据竞争和死锁

2. **性能测试**
   - 使用 `std::chrono::high_resolution_clock` 测量延迟
   - 多次运行取平均值
   - 考虑 CPU 缓存影响

3. **负载均衡效果**
   - 实际负载均衡效果需要通过运行时测试验证
   - 单元测试主要验证 AssignmentTable 和 LoadMonitor 功能正确性

4. **日志记录**
   - 测试中使用 `SAGEFLOW_LOG_INFO` 记录性能数据
   - 失败时输出详细信息

## 验收标准

- [ ] 所有测试用例通过
- [ ] AssignmentTable 并发安全验证通过
- [ ] 批量更新原子性验证通过
- [ ] LoadMonitor 功能正确
- [ ] 性能测试通过（读操作 < 10ns）
- [ ] 测试覆盖率 >= 80%

## 后续任务

完成本任务后，VSJoin 负载均衡功能已完成。可以继续：
- 实现运行期动态负载均衡（Phase B3）
- 性能优化和调优

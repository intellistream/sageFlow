# Task 06: 集成测试 + 召回率验证

## 任务概述

实现 VSJoin 的集成测试，验证：
1. VSJoin 双层索引查询功能
2. 多播策略下边界向量不丢失（召回率验证）
3. Global Index 重建和去重功能
4. 端到端流程正确性

**预估工时**: 1 天  
**依赖**: Task 04 (后台重建机制), Task 05 (配置验证)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 13.6.2 节: 测试要点
  - 第 9 章: 多播 vs 邻近分区探测方案对比

## 实现要求

### 1. 创建测试文件

- **测试文件**: `test/IntegrationTest/vsjoin_integration_test.cpp`
- **测试配置**: `config/vsjoin_integration_test.toml`

### 2. 测试用例设计

#### 2.1 基础功能测试

```cpp
TEST(VSJoinIntegrationTest, BasicQuery) {
    // 1. 创建测试数据
    TestDataGenerator generator(config);
    auto [records, expected_matches] = generator.generateData();
    
    // 2. 创建 VSJoin 配置
    JoinStrategyConfig vsjoin_config;
    vsjoin_config.algorithm = JoinAlgorithm::VSJOIN;
    vsjoin_config.window_state_type = WindowStateType::TWO_TIER;
    vsjoin_config.partition_strategy = PartitionStrategy::LSH;
    vsjoin_config.vsjoin_multicast_k = 2;
    
    // 3. 创建 JoinOperator
    auto join_op = createJoinOperator(vsjoin_config);
    
    // 4. 插入数据
    for (const auto& record : records) {
        join_op->apply(std::move(record), slot, collector, context);
    }
    
    // 5. 执行查询
    auto results = join_op->query(query_record, slot, subtask_index);
    
    // 6. 验证结果
    ASSERT_GT(results.size(), 0);
    verifyResults(results, expected_matches);
}
```

#### 2.2 召回率测试（多播策略）

```cpp
TEST(VSJoinIntegrationTest, RecallWithMulticast) {
    // 1. 创建边界向量测试数据
    // 边界向量应该被多播到多个分区
    auto boundary_vectors = generateBoundaryVectors(config);
    
    // 2. 创建 VSJoin 配置（启用多播）
    JoinStrategyConfig vsjoin_config;
    vsjoin_config.algorithm = JoinAlgorithm::VSJOIN;
    vsjoin_config.vsjoin_multicast_k = 2;
    
    // 3. 插入边界向量
    for (const auto& vec : boundary_vectors) {
        join_op->apply(std::move(vec), slot, collector, context);
    }
    
    // 4. 查询边界向量
    for (const auto& query_vec : boundary_vectors) {
        auto results = join_op->query(query_vec, slot, subtask_index);
        
        // 5. 验证召回率
        // 边界向量应该能在至少一个分区中找到匹配
        ASSERT_GT(results.size(), 0) << "Boundary vector should be found";
    }
    
    // 6. 计算整体召回率
    double recall = computeRecall(results, ground_truth);
    EXPECT_GE(recall, 0.95) << "Recall should be >= 95%";
}
```

#### 2.3 Global Index 重建测试

```cpp
TEST(VSJoinIntegrationTest, GlobalIndexRebuild) {
    // 1. 插入大量数据
    auto records = generateLargeDataset(10000);
    for (const auto& record : records) {
        join_op->apply(std::move(record), slot, collector, context);
    }
    
    // 2. 等待重建（或手动触发）
    std::this_thread::sleep_for(std::chrono::milliseconds(6000));
    
    // 3. 验证 Global Index 已重建
    // 可以通过查询 Global Index 验证
    
    // 4. 验证去重正确性
    // 多播导致的重复 UID 应该被正确去重
    verifyDeduplication();
}
```

#### 2.4 去重验证测试

```cpp
TEST(VSJoinIntegrationTest, Deduplication) {
    // 1. 创建重复 UID 的数据（模拟多播）
    auto records_with_duplicates = generateRecordsWithDuplicates();
    
    // 2. 插入数据
    for (const auto& record : records_with_duplicates) {
        join_op->apply(std::move(record), slot, collector, context);
    }
    
    // 3. 触发 Global Index 重建
    triggerRebuild();
    
    // 4. 验证去重
    // Global Index 中不应该有重复的 UID
    verifyNoDuplicatesInGlobalIndex();
    
    // 5. 验证查询结果去重
    auto results = join_op->query(query_record, slot, subtask_index);
    verifyNoDuplicatesInResults(results);
}
```

### 3. 测试工具函数

创建 `test/test_utils/vsjoin_test_helper.h`：

```cpp
class VSJoinTestHelper {
public:
    // 创建 VSJoin JoinOperator
    static std::unique_ptr<JoinOperator> createVSJoinOperator(
        const JoinStrategyConfig& config);
    
    // 生成边界向量测试数据
    static std::vector<std::unique_ptr<VectorRecord>> generateBoundaryVectors(
        const TestConfig& config);
    
    // 计算召回率
    static double computeRecall(
        const std::vector<std::unique_ptr<VectorRecord>>& results,
        const std::vector<std::unique_ptr<VectorRecord>>& ground_truth);
    
    // 验证去重
    static void verifyNoDuplicates(
        const std::vector<std::unique_ptr<VectorRecord>>& records);
    
    // 验证结果正确性
    static void verifyResults(
        const std::vector<std::unique_ptr<VectorRecord>>& results,
        const std::vector<std::unique_ptr<VectorRecord>>& expected);
};
```

## 关键测试点

### 1. 召回率验证

- **目标**: 验证多播策略下边界向量不丢失
- **方法**: 
  - 生成边界向量测试数据
  - 插入数据并启用多播
  - 查询边界向量，验证能找到匹配
  - 计算整体召回率（应该 >= 95%）

### 2. 去重验证

- **目标**: 验证 Global Index 重建和查询结果合并时的去重正确性
- **方法**:
  - 创建包含重复 UID 的数据（模拟多播）
  - 触发 Global Index 重建
  - 验证 Global Index 中无重复 UID
  - 验证查询结果中无重复记录

### 3. 并发安全测试

- **目标**: 验证多线程场景下的正确性
- **方法**:
  - 多线程并发插入数据
  - 多线程并发查询
  - 验证无数据竞争和死锁

### 4. 性能测试（可选）

- **目标**: 验证 VSJoin 性能符合预期
- **方法**:
  - 测量查询延迟
  - 测量吞吐量
  - 对比其他 Join 方法（BruteForce, IVF）

## 测试配置

创建 `config/vsjoin_integration_test.toml`：

```toml
[test_config]
dimension = 128
num_records = 1000
num_queries = 100
similarity_threshold = 0.8

[vsjoin_config]
algorithm = "vsjoin"
window_state_type = "two_tier"
partition_strategy = "lsh"
parallelism = 8

[vsjoin]
multicast_k = 2
rebuild_interval_ms = 5000
rebuild_threshold = 1000

[vsjoin_lsh]
num_hash_functions = 8
boundary_threshold = 0.1
```

## 测试要求

### 运行测试

```bash
cd build
ctest -R vsjoin_integration_test
```

### 测试覆盖率

- [ ] 基础查询功能测试
- [ ] 召回率测试（多播策略）
- [ ] Global Index 重建测试
- [ ] 去重验证测试
- [ ] 并发安全测试（可选）
- [ ] 性能测试（可选）

## 注意事项

1. **测试数据生成**
   - 使用 `TestDataGenerator` 生成测试数据
   - 确保测试数据包含边界向量

2. **召回率计算**
   - 召回率 = (找到的匹配数) / (总匹配数)
   - 目标召回率 >= 95%

3. **去重验证**
   - 使用 `std::unordered_set<uint64_t>` 检查 UID 重复
   - 验证 Global Index 和查询结果都无重复

4. **日志记录**
   - 测试中使用 `SAGEFLOW_LOG_INFO` 记录关键步骤
   - 失败时输出详细信息

5. **测试隔离**
   - 每个测试用例独立，不相互影响
   - 使用 `SetUp()` 和 `TearDown()` 清理资源

## 验收标准

- [ ] 所有测试用例通过
- [ ] 召回率 >= 95%（多播策略下）
- [ ] 去重验证通过（Global Index 和查询结果都无重复）
- [ ] 并发安全测试通过（如果实现）
- [ ] 测试覆盖率 >= 80%
- [ ] 测试文档完整

## 后续任务

完成本任务后，VSJoin 核心功能已完成，可以继续：
- Task 07: AssignmentTable (RCU) + LoadMonitor 实现（负载均衡功能）

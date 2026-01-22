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
## 追加任务：集成测试框架链路打通

### 任务背景

为保证 VSJoin 能够完整地融入 SageFlow 现有的集成测试体系，需要将 VSJoin 接入到 `join_baseline_integration_test.cpp` 和 `run_integration_test.py` 这套标准测试链路中。这样可以：

1. 通过 TOML 配置驱动 VSJoin 测试用例
2. 支持通过 Python 脚本批量运行 VSJoin 测试
3. 与其他 Join 算法（BruteForce, IVF, HNSW 等）使用统一的测试报告和对比框架

### 实现要求

#### 1. 更新 TOML 测试配置

更新 `config/integration_test_cases.toml`，添加启用的 VSJoin 测试用例：

```toml
# ==================== VSJoin 集成测试 ====================
# VSJoin 双层索引 + 后台重建测试

[[test_case]]
name = "vsjoin_baseline"
description = "VSJoin baseline test with two-tier index"
algorithm = "vsjoin"
partition_strategy = "lsh"
window_state_type = "two_tier"
index_strategy = "partitioned"
num_partitions = 4
vsjoin_num_hash_functions = 8
vsjoin_boundary_threshold = 0.1
vsjoin_rebuild_interval_ms = 5000
vsjoin_rebuild_threshold = 500
ivf_nlist = 50
ivf_nprobes = 10
data_sizes = [500, 1000]
parallelism = [1, 2, 4]
expected_min_recall = 0.75
enabled = true

[[test_case]]
name = "vsjoin_high_recall"
description = "VSJoin with higher hash functions for better recall"
algorithm = "vsjoin"
partition_strategy = "lsh"
window_state_type = "two_tier"
index_strategy = "partitioned"
num_partitions = 4
vsjoin_num_hash_functions = 16
vsjoin_boundary_threshold = 0.15
vsjoin_rebuild_interval_ms = 3000
ivf_nlist = 50
ivf_nprobes = 15
data_sizes = [500]
parallelism = [1, 2, 4]
expected_min_recall = 0.80
enabled = true

[[test_case]]
name = "vsjoin_parallelism_scaling"
description = "VSJoin parallelism scalability test"
algorithm = "vsjoin"
partition_strategy = "lsh"
window_state_type = "two_tier"
index_strategy = "partitioned"
num_partitions = 8
vsjoin_num_hash_functions = 8
vsjoin_boundary_threshold = 0.1
ivf_nlist = 50
ivf_nprobes = 10
data_sizes = [1000]
parallelism = [1, 2, 4, 8]
expected_min_recall = 0.70
enabled = true
```

#### 2. 验证 JoinIntegrationPipelineHelper 支持 VSJoin

确认 `test/test_utils/join_integration_pipeline_helper.*` 能够正确处理 VSJoin 配置：

- `JoinStrategyFactory::create()` 能够创建 VSJoin 组件
- Pipeline 正确初始化双层索引（Global + Local）
- 后台重建线程正常启动和停止

#### 3. 验证 run_integration_test.py 支持 VSJoin

确认 `scripts/run_integration_test.py` 的 `METHOD_FILTER_MAP` 已包含 vsjoin：

```python
METHOD_FILTER_MAP = {
    'bruteforce': '*bruteforce*',
    'ivf': '*ivf*',
    'hnsw': '*hnsw*',
    'hdr_tree': '*hdr_tree*',
    'clustered_join': '*clustered*',
    's3j': '*s3j*',
    'vsjoin': '*vsjoin*',  # 确认已存在
}
```

#### 4. 运行测试验证

```bash
# 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j $(nproc)

# 运行 VSJoin 集成测试
python3 scripts/run_integration_test.py --methods vsjoin \
    --config config/integration_test_cases.toml \
    --output-dir test/result/vsjoin_integration

# 或者直接运行测试二进制
./build/bin/test_join_baseline_integration --gtest_filter='*vsjoin*'
```

### 验收项

#### 配置验证
- [ ] `config/integration_test_cases.toml` 包含至少 3 个启用的 VSJoin 测试用例
- [ ] 配置包含必要的 VSJoin 参数（`vsjoin_num_hash_functions`, `vsjoin_boundary_threshold` 等）
- [ ] `num_partitions` 参数设置合理

#### 链路打通验证
- [ ] `JoinStrategyFactory::create()` 能为 VSJoin 正确创建所有组件：
  - `join_method` 为 `VSJoinMethod` 实例
  - `left_state` 和 `right_state` 为 `TwoTierWindowState`
  - `global_left_id` 和 `global_right_id` 有效
  - `local_left_ids` 和 `local_right_ids` 长度等于 parallelism
- [ ] Pipeline 执行过程中：
  - 数据正确插入到 Local Index
  - 查询同时访问 Local 和 Global Index
  - 后台重建线程正常工作
- [ ] 测试完成后 Pipeline 正常关闭，无资源泄漏

#### 测试执行验证
- [ ] `./build/bin/test_join_baseline_integration --gtest_filter='*vsjoin*'` 执行成功
- [ ] `python3 scripts/run_integration_test.py --methods vsjoin` 执行成功
- [ ] 测试结果输出到指定目录
- [ ] 生成的 CSV 报告包含 VSJoin 测试结果

#### 召回率验证
- [ ] `vsjoin_baseline` 测试召回率 >= 75%
- [ ] `vsjoin_high_recall` 测试召回率 >= 80%
- [ ] 所有启用的 VSJoin 测试用例通过

#### 对比验证（可选）
- [ ] VSJoin 与 BruteForce 在相同数据集上的召回率对比记录
- [ ] VSJoin 与 IVF 在相同数据集上的性能对比记录

### 关键检查点

#### 1. WindowState 类型匹配

VSJoin 必须使用 `TwoTierWindowState`：

```cpp
// JoinStrategyFactory::createWindowState() 中
if (config.algorithm == JoinAlgorithm::VSJOIN) {
    return std::make_unique<TwoTierWindowState>(
        parallelism,
        config.two_tier_compact_threshold);
}
```

#### 2. 索引创建验证

VSJoin 需要创建 2 个 Global Index + 2*P 个 Local Index：

```cpp
// JoinStrategyFactory::create() 中
if (config.algorithm == JoinAlgorithm::VSJOIN) {
    // Global Index (IVF)
    components.global_left_id = concurrency_manager->create_index(
        "vsjoin_global_left", IndexType::IVF, ...);
    components.global_right_id = concurrency_manager->create_index(
        "vsjoin_global_right", IndexType::IVF, ...);
    
    // Local Index (BruteForce) - 每个分区一对
    for (int partition = 0; partition < P; ++partition) {
        components.local_left_ids[partition] = concurrency_manager->create_index(...);
        components.local_right_ids[partition] = concurrency_manager->create_index(...);
    }
}
```

#### 3. VSJoinMethod 初始化

确保 VSJoinMethod 正确接收索引 ID：

```cpp
auto* vsjoin = dynamic_cast<VSJoinMethod*>(components.join_method.get());
if (vsjoin) {
    vsjoin->setGlobalIndexIds(components.global_left_id, components.global_right_id);
    vsjoin->setLocalIndexIds(components.local_left_ids, components.local_right_ids);
    vsjoin->setWindowStates(components.left_state.get(), components.right_state.get());
}
```

#### 4. 后台重建线程

JoinOperator 需要为 VSJoin 启动后台重建：

```cpp
// JoinOperator::initializeWithStrategyConfig() 中
if (strategy_config_.algorithm == JoinAlgorithm::VSJOIN) {
    // 启动后台重建线程
    startRebuildThread();
}
```

### 常见问题排查

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 测试无输出 | VSJoin 测试用例 `enabled = false` | 检查 TOML 配置，确保 `enabled = true` |
| 召回率为 0 | 索引未正确创建 | 检查日志中的 `VSJOIN_FACTORY` 消息 |
| 段错误 | WindowState 类型不匹配 | 确保使用 `TwoTierWindowState` |
| 后台重建未执行 | 重建间隔太长 | 减小 `vsjoin_rebuild_interval_ms` |
| 测试超时 | Global Index 过大 | 调整 IVF 参数或减小数据规模 |
## 后续任务

完成本任务后，VSJoin 核心功能已完成，可以继续：
- Task 07: AssignmentTable (RCU) + LoadMonitor 实现（负载均衡功能）

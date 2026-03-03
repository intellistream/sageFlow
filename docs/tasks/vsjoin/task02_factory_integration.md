# Task 02: JoinStrategyFactory 集成

## 任务概述

在 `JoinStrategyFactory` 中集成 VSJoin 算法，创建 Global + Local 索引对，并返回相应的 `StrategyComponents`。

**预估工时**: 1 天  
**依赖**: Task 01 (VSJoinMethod 基础实现)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 2 章: 索引管理策略（通过 ConcurrencyManager）
  - 第 4 章: VSJoinMethod 实现

## 实现要求

### 1. 修改文件

- **头文件**: `include/operator/utils/join_strategy_factory.h`
- **实现文件**: `src/operator/join_strategy_factory.cpp`

### 2. 扩展 StrategyComponents

在 `join_strategy_factory.h` 或相关头文件中，扩展 `StrategyComponents` 结构：

```cpp
struct StrategyComponents {
    // ... 现有字段 ...
    
    // Global Immutable Index（所有 subtask 共享，只读查询）
    // 共 2 个 index_id
    int global_left_id = -1;
    int global_right_id = -1;
    
    // Local Mutable Index（每分区独立，完全隔离）
    // 共 2 * num_partitions 个 index_id
    std::vector<int> local_left_ids;   // size = num_partitions
    std::vector<int> local_right_ids;  // size = num_partitions
};
```

### 3. 添加 JoinAlgorithm::VSJOIN 枚举值

在 `join_strategy_config.h` 中添加：

```cpp
enum class JoinAlgorithm {
    // ... 现有枚举值 ...
    VSJOIN,  // VSJoin 双层索引方案
};
```

### 4. 实现 VSJoin 创建逻辑

在 `JoinStrategyFactory::create()` 方法中添加 VSJOIN case：

```cpp
case JoinAlgorithm::VSJOIN: {
    const int P = static_cast<int>(parallelism);  // 分区数 = 并行度
    
    // 1. 创建 Global Immutable Index（IVF/HNSW，用于快速查询）
    IVFParameters global_ivf_params{
        .nlist = config.ivf_nlist,
        .rebuild_threshold = config.ivf_rebuild_threshold,
        .nprobes = config.ivf_nprobes
    };
    
    components.global_left_id = concurrency_manager->create_index(
        "vsjoin_global_left", IndexType::IVF, config.dimension, global_ivf_params);
    components.global_right_id = concurrency_manager->create_index(
        "vsjoin_global_right", IndexType::IVF, config.dimension, global_ivf_params);
    
    // 2. 创建 Local Mutable Index（每分区独立，完全隔离）
    // 每个分区创建独立的 BruteForce 索引
    // 分区内只有单线程访问，无需复杂索引结构
    components.local_left_ids.resize(P, -1);
    components.local_right_ids.resize(P, -1);
    
    for (int partition = 0; partition < P; ++partition) {
        // 左流分区索引
        std::string left_name = "vsjoin_local_left_p" + std::to_string(partition);
        components.local_left_ids[partition] = concurrency_manager->create_index(
            left_name, IndexType::BruteForce, config.dimension);
        
        // 右流分区索引
        std::string right_name = "vsjoin_local_right_p" + std::to_string(partition);
        components.local_right_ids[partition] = concurrency_manager->create_index(
            right_name, IndexType::BruteForce, config.dimension);
    }
    
    SAGEFLOW_LOG_INFO("VSJOIN_FACTORY", 
        "Created {} Global indexes + {} Local indexes (parallelism={})",
        2, 2 * P, P);
    
    // 3. 创建 VSJoinMethod 实例
    auto method = std::make_unique<VSJoinMethod>();
    method->initialize(context, concurrency_manager);
    components.join_method = std::move(method);
    
    break;
}
```

### 5. 创建 WindowState

在 `JoinStrategyFactory::createWindowState()` 中，确保 VSJoin 使用 `TWO_TIER` 类型：

```cpp
if (config.algorithm == JoinAlgorithm::VSJOIN) {
    return std::make_unique<TwoTierWindowState>(
        parallelism, config.two_tier_compact_threshold);
}
```

## 关键设计点

1. **索引总数计算**
   - Global Index: 2 个（左右各一个共享索引）
   - Local Index: 2 * P 个（每流每分区一个独立索引）
   - **总计**: 2 + 2 * P 个 index_id（P = parallelism）

2. **索引类型选择**
   - Global Index: IVF/HNSW（快速查询，支持大规模数据）
   - Local Index: BruteForce（轻量级，分区内单线程访问，无需复杂索引）

3. **索引命名规范**
   - Global: `"vsjoin_global_left"`, `"vsjoin_global_right"`
   - Local: `"vsjoin_local_left_p{partition}"`, `"vsjoin_local_right_p{partition}"`

4. **索引访问模式**
   ```
   subtask_0 → local_left_ids[0], local_right_ids[0]  // 分区 0 独占
   subtask_1 → local_left_ids[1], local_right_ids[1]  // 分区 1 独占
   ...
   所有 subtask → global_left_id, global_right_id     // 共享只读
   ```

## 测试要求

### 单元测试

创建 `test/operator/utils/test_vsjoin_factory.cpp`：

1. **索引创建测试**
   - 验证 Global Index 创建（2 个）
   - 验证 Local Index 创建（2 * P 个）
   - 验证索引 ID 有效性（>= 0）

2. **索引命名测试**
   - 验证索引名称符合规范
   - 验证分区索引名称唯一性

3. **并行度测试**
   - 测试不同并行度（P=1, 4, 8, 16）下的索引创建

4. **方法创建测试**
   - 验证 VSJoinMethod 实例创建成功
   - 验证方法正确初始化

### 运行测试

```bash
cd build
ctest -R test_vsjoin_factory
```

## 注意事项

1. **错误处理**
   - 检查 `concurrency_manager` 指针有效性
   - 检查索引创建返回值（失败返回 -1）
   - 记录创建失败的索引

2. **日志记录**
   - 使用 `SAGEFLOW_LOG_INFO` 记录索引创建信息
   - 日志标签使用 `"VSJOIN_FACTORY"`

3. **内存管理**
   - `local_left_ids` 和 `local_right_ids` 使用 `std::vector`，自动管理内存

4. **配置参数**
   - Global Index 参数从 `config.ivf_nlist`, `config.ivf_nprobes` 等获取
   - Local Index 使用默认参数（BruteForce 无需特殊参数）

5. **向后兼容**
   - 确保现有其他算法的创建逻辑不受影响
   - `StrategyComponents` 的扩展字段有默认值（-1 或空 vector）

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] VSJoin 索引创建成功（2 + 2*P 个）
- [ ] 索引命名符合规范
- [ ] VSJoinMethod 实例创建成功
- [ ] 日志记录完整
- [ ] 不影响现有其他算法的创建逻辑

## 后续任务

完成本任务后，可以继续：
- Task 03: JoinOperator VSJoin 特殊路径（updateSideWithState 只插 Local）

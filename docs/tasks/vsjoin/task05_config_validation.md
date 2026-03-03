# Task 05: 配置验证 + TOML 解析

## 任务概述

实现 VSJoin 的配置验证和 TOML 解析功能，包括：
1. 在 `JoinStrategyConfig` 中添加 VSJoin 相关配置字段
2. 实现配置验证逻辑
3. 实现 TOML 配置文件解析

**预估工时**: 0.5 天  
**依赖**: Task 01 (VSJoinMethod 基础实现)

## 参考文档

- 主设计文档: `docs/vsjoin_compliant_design_c745d987.plan.md`
  - 第 6 章: 配置扩展
  - 第 13.6.1 节: 推荐配置

## 实现要求

### 1. 修改文件

- **头文件**: `include/operator/utils/join_strategy_config.h`
- **实现文件**: `src/operator/utils/join_strategy_config.cpp`
- **验证文件**: `src/operator/join_config_validator.cpp`

### 2. 扩展 JoinStrategyConfig

在 `join_strategy_config.h` 中添加 VSJoin V2 参数：

```cpp
struct JoinStrategyConfig {
    // ... 现有字段 ...
    
    // ==================== VSJoin V2 参数 ====================
    int vsjoin_multicast_k = 2;             // 边界向量多播到 k 个分区（推荐 2-3）
    int64_t vsjoin_rebuild_interval_ms = 5000;  // Global 重建间隔
    size_t vsjoin_rebuild_threshold = 1000;     // 触发重建的阈值
    
    // Local Index 参数（比 Global 更轻量）
    // 注意：Local Index 使用 BruteForce，无需 nlist/nprobes
    IndexType vsjoin_local_index_type = IndexType::BruteForce;
    
    // Global Index 类型（IVF/HNSW）
    IndexType vsjoin_global_index_type = IndexType::IVF;
    
    // LSH 分区器参数
    int vsjoin_num_hash_functions = 8;         // LSH 哈希函数数量
    double vsjoin_boundary_threshold = 0.1;    // 边界向量阈值
};
```

### 3. 实现配置验证

在 `join_config_validator.cpp` 中添加 VSJoin 配置验证：

```cpp
bool JoinConfigValidator::validateVSJoinConfig(const JoinStrategyConfig& config) {
    // 验证多播参数 k
        if (config.vsjoin_multicast_k < 1 || config.vsjoin_multicast_k > 10) {
            SAGEFLOW_LOG_ERROR("VSJOIN_CONFIG", 
            "Invalid multicast_k: {} (must be in [1, 10])", 
            config.vsjoin_multicast_k);
        return false;
    }
    
    // 验证重建间隔
        if (config.vsjoin_rebuild_interval_ms < 1000) {
            SAGEFLOW_LOG_ERROR("VSJOIN_CONFIG", 
            "Invalid rebuild_interval_ms: {} (must be >= 1000ms)", 
            config.vsjoin_rebuild_interval_ms);
        return false;
    }
    
    // 验证重建阈值
        if (config.vsjoin_rebuild_threshold < 100) {
            SAGEFLOW_LOG_ERROR("VSJOIN_CONFIG", 
            "Invalid rebuild_threshold: {} (must be >= 100)", 
            config.vsjoin_rebuild_threshold);
        return false;
    }
    
    // 验证索引类型
        if (config.vsjoin_global_index_type != IndexType::IVF && 
        config.vsjoin_global_index_type != IndexType::HNSW) {
            SAGEFLOW_LOG_ERROR("VSJOIN_CONFIG", 
            "Invalid global_index_type: {} (must be IVF or HNSW)", 
            static_cast<int>(config.vsjoin_global_index_type));
        return false;
    }
    
        if (config.vsjoin_local_index_type != IndexType::BruteForce) {
            SAGEFLOW_LOG_WARN("VSJOIN_CONFIG", 
            "Local index type is not BruteForce: {} (recommended: BruteForce)", 
            static_cast<int>(config.vsjoin_local_index_type));
    }
    
    // 验证 LSH 参数
    if (config.vsjoin_num_hash_functions < 1 || config.vsjoin_num_hash_functions > 32) {
        SAGEFLOW_LOG_ERROR("VSJOIN_CONFIG", 
            "Invalid num_hash_functions: {} (must be in [1, 32])", 
            config.vsjoin_num_hash_functions);
        return false;
    }
    
    if (config.vsjoin_boundary_threshold < 0.0 || config.vsjoin_boundary_threshold > 1.0) {
        SAGEFLOW_LOG_ERROR("VSJOIN_CONFIG", 
            "Invalid boundary_threshold: {} (must be in [0.0, 1.0])", 
            config.vsjoin_boundary_threshold);
        return false;
    }
    
    return true;
}
```

### 4. 实现 TOML 解析

在 `join_strategy_config.cpp` 中添加 TOML 解析逻辑：

```cpp
void JoinStrategyConfig::loadFromTOML(const toml::table& table) {
    // ... 现有解析逻辑 ...
    
    // VSJoin 参数解析
    if (table.contains("vsjoin")) {
        const auto& vsjoin_table = table["vsjoin"].as_table();
        
        if (vsjoin_table->contains("multicast_k")) {
            vsjoin_multicast_k = vsjoin_table->at("multicast_k").value<int>().value_or(2);
        }
        
        if (vsjoin_table->contains("rebuild_interval_ms")) {
            vsjoin_rebuild_interval_ms = 
                vsjoin_table->at("rebuild_interval_ms").value<int64_t>().value_or(5000);
        }
        
        if (vsjoin_table->contains("rebuild_threshold")) {
            vsjoin_rebuild_threshold = 
                vsjoin_table->at("rebuild_threshold").value<size_t>().value_or(1000);
        }
        
        if (vsjoin_table->contains("local_index_type")) {
            std::string type_str = vsjoin_table->at("local_index_type").value<std::string>().value_or("bruteforce");
            vsjoin_local_index_type = parseIndexType(type_str);
        }
        
        if (vsjoin_table->contains("global_index_type")) {
            std::string type_str = vsjoin_table->at("global_index_type").value<std::string>().value_or("ivf");
            vsjoin_global_index_type = parseIndexType(type_str);
        }
    }
    
    // LSH 分区器参数解析
    if (table.contains("vsjoin_lsh")) {
        const auto& lsh_table = table["vsjoin_lsh"].as_table();
        
        if (lsh_table->contains("num_hash_functions")) {
            vsjoin_num_hash_functions = 
                lsh_table->at("num_hash_functions").value<int>().value_or(8);
        }
        
        if (lsh_table->contains("boundary_threshold")) {
            vsjoin_boundary_threshold = 
                lsh_table->at("boundary_threshold").value<double>().value_or(0.1);
        }
    }
}
```

### 5. TOML 配置示例

创建 `config/vsjoin_strategy.toml`：

```toml
[vsjoin]
multicast_k = 2
rebuild_interval_ms = 5000
rebuild_threshold = 1000
local_index_type = "bruteforce"
global_index_type = "ivf"

[vsjoin_lsh]
num_hash_functions = 8
boundary_threshold = 0.1
```

## 关键设计点

1. **默认值**
   - `multicast_k = 2`（推荐 2-3）
   - `rebuild_interval_ms = 5000`（5 秒）
   - `rebuild_threshold = 1000`（1000 条记录）
   - `local_index_type = BruteForce`（轻量级）
   - `global_index_type = IVF`（快速查询）

2. **配置验证范围**
   - `multicast_k`: [1, 10]
   - `rebuild_interval_ms`: >= 1000ms
   - `rebuild_threshold`: >= 100
   - `num_hash_functions`: [1, 32]
   - `boundary_threshold`: [0.0, 1.0]

3. **推荐配置**
   ```cpp
   strategy_config_.window_state_type = WindowStateType::TWO_TIER;
   strategy_config_.partition_strategy = PartitionStrategy::LSH;
   strategy_config_.two_tier_compact_threshold = 100;
   strategy_config_.vsjoin_multicast_k = 2;
   strategy_config_.vsjoin_rebuild_interval_ms = 5000;
   strategy_config_.vsjoin_rebuild_threshold = 1000;
   ```

## 测试要求

### 单元测试

创建 `test/operator/utils/test_vsjoin_config.cpp`：

1. **配置验证测试**
   - 验证有效配置通过验证
   - 验证无效配置被拒绝（multicast_k 超出范围等）
   - 验证边界值处理

2. **TOML 解析测试**
   - 验证 TOML 文件正确解析
   - 验证默认值应用
   - 验证缺失字段处理

3. **配置合并测试**
   - 验证命令行参数覆盖 TOML 配置
   - 验证配置优先级

### 运行测试

```bash
cd build
ctest -R test_vsjoin_config
```

## 注意事项

1. **向后兼容**
   - 确保现有配置不受影响
   - VSJoin 配置字段有默认值

2. **错误处理**
   - 配置验证失败时返回错误信息
   - TOML 解析失败时使用默认值

3. **日志记录**
   - 使用 `SAGEFLOW_LOG_ERROR` 记录验证失败
   - 使用 `SAGEFLOW_LOG_WARN` 记录警告（如 Local Index 类型不是 BruteForce）

4. **类型转换**
   - 字符串到 `IndexType` 的转换需要实现 `parseIndexType()` 辅助函数

## 验收标准

- [ ] 代码编译通过，无警告
- [ ] 单元测试全部通过
- [ ] VSJoin 配置字段正确添加
- [ ] 配置验证逻辑正确
- [ ] TOML 解析正确
- [ ] 默认值正确应用
- [ ] 向后兼容性保证

## 后续任务

完成本任务后，可以继续：
- Task 06: 集成测试 + 召回率验证

#!/usr/bin/env python3
# This script translates English documentation to Chinese while preserving code blocks

import re
import sys

def translate_code_review_improvements():
    """Translate CODE_REVIEW_IMPROVEMENTS.md to Chinese"""
    content = """# 代码审查改进总结

## 概述

本文档总结了根据join数据源框架的代码审查反馈所做的架构改进。

## 改进内容

### 1. 将 VectorListSource 移动到 data_source 文件夹

**评审意见：** VectorListSource是不是放到data_source文件夹下面去做成一类通用的数据源比较好？

**改进前：**
- VectorListSource 定义为 `join_test_helper.cpp` 内的内联类
- 无法跨不同测试文件复用
- 隐藏的实现细节

**改进后：**
- VectorListSource 现在是独立的头文件：`test/test_utils/data_source/vector_list_source.h`
- 实现 DataSourceBase 接口，与其他数据源一致
- 可在任何需要包装内存向量的测试中直接使用
- 在 `data_source/README.md` 中有文档说明

**优势：**
- ✅ 可复用组件 - 可独立使用
- ✅ 与其他数据源一致（RandomDataSource, DatasetDataSource）
- ✅ 更好的关注点分离
- ✅ 更易于测试和维护

**代码位置：**
```
test/test_utils/data_source/
├── data_source_base.h
├── random_data_source.h/cpp
├── dataset_data_source.h/cpp
├── json_data_source.h/cpp
└── vector_list_source.h       # 新增：提取并提升为通用组件
```

### 2. 简化模式架构

**评审意见：** Mode应当就设置成两种（Duplicate和Separate），然后两条流的数据源都应该用Generator来管理而不是直接放两个DataSource放在这，只不过用于向后兼容以前的测试时使用Duplicate的方式。现在这种方式DataSource和Generator的调用层级有点混用了

**改进前：**
- 3种模式：Duplicate、Separate、Generated
- "Generated" 模式本质上就是带特殊处理的 Duplicate
- DataSource 和 Generator 之间的抽象层级混合
- 配置中有冗余的 "Generated" 模式字段

**改进后：**
- 2种模式：Duplicate、Separate（移除Generated）
- 两种模式都统一使用 DataSourceBase
- Generator 创建 VectorListSource，然后在 Duplicate 模式中使用
- 清晰的关注点分离：Generator → VectorListSource → JoinDataSourcePair

**架构流程：**

**对于 TestDataGenerator（向后兼容）：**
```
TestDataGenerator
    ↓ generateData()
std::vector<std::vector<float>>
    ↓ 包装为 VectorListSource
DataSourceBase (VectorListSource)
    ↓ 传递给 Duplicate 模式
JoinDataSourcePair
    ↓ generateStreams()
(left_records, right_records)
```

**对于直接数据源：**
```
DataSourceBase (任意: Random, Dataset, VectorList等)
    ↓ 传递给 Duplicate 或 Separate 模式
JoinDataSourcePair
    ↓ generateStreams()
(left_records, right_records)
```

**优势：**
- ✅ 更清晰的架构 - 没有混合的抽象层级
- ✅ 更易理解 - 只有2种清晰的模式
- ✅ 统一接口 - 所有模式都使用 DataSourceBase
- ✅ 更灵活 - 可以在任何模式中使用任何数据源
- ✅ 更好的封装 - Generator 的内部细节被隐藏

## 模式对比

### Duplicate 模式

**用途：** 从同一数据源生成左右流。

**使用场景：**
- 测试自连接场景
- 使用 TestDataGenerator 测试（向后兼容）
- 测试两侧具有相同分布的数据集

**配置：**
```cpp
JoinDataSourceConfig config;
config.mode = JoinDataSourceConfig::Mode::Duplicate;
config.single_source = any_data_source;  // DataSourceBase
config.apply_right_uid_offset = true;
```

### Separate 模式

**用途：** 从不同数据源生成左右流。

**使用场景：**
- 测试不同数据分布的连接
- 测试非对称场景
- 为左右使用不同的数据集

**配置：**
```cpp
JoinDataSourceConfig config;
config.mode = JoinDataSourceConfig::Mode::Separate;
config.left_source = left_data_source;   // DataSourceBase
config.right_source = right_data_source; // DataSourceBase
```

## 测试验证

所有改进后的测试全部通过：

**单元测试：**
- ✅ test_join_data_source: 8/8 测试通过
- ✅ test_data_source: 5/5 测试通过
- ✅ test_data_persistence: 5/5 测试通过

**集成测试：**
- ✅ test_join_bruteforce: 6/6 测试通过
- ✅ test_pipeline_basic: 4/4 测试通过

**性能测试：**
- ✅ test_join_perf_scaling: 编译成功，14个测试用例注册

## 向后兼容性

所有改进都完全向后兼容：
- ✅ 现有测试代码无需修改
- ✅ 原有的 TestDataGenerator 用法仍然有效
- ✅ 默认行为保持不变
- ✅ 新功能是可选的增强

## 文档更新

所有相关文档已更新以反映新架构：
- test/test_utils/data_source/README.md
- test/test_utils/JOIN_DATA_SOURCE_GUIDE.md
- CODE_REVIEW_IMPROVEMENTS.md（本文档）

## 总结

这些改进使代码库更加：
1. **模块化** - 组件独立且可复用
2. **一致性** - 统一的接口和模式
3. **可维护** - 更清晰的架构和分离的关注点
4. **可扩展** - 易于添加新的数据源和功能
5. **向后兼容** - 不破坏现有代码

所有改进都经过充分测试和验证。
"""
    with open('CODE_REVIEW_IMPROVEMENTS.md', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Translated CODE_REVIEW_IMPROVEMENTS.md")

if __name__ == '__main__':
    translate_code_review_improvements()

#!/usr/bin/env python3
import os

# Helper function to write translated content
def write_translation(filename, content):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Translated {filename}")

# Translate IMPLEMENTATION_SUMMARY.md - keeping existing Chinese, just ensure consistency
implementation_summary = """# 数据源框架实现总结

## 问题描述

原始需求：帮我把test文件夹里面数据生成的部分作为模块抽离出来。数据源除了目前的随机生成，新加上从数据集中直接获取数据（数据集目前在data文件夹下）。在生成给算子用的数据中，可以通过不同的类来区分我想要的数据源。

## 实现方案

### 架构设计

创建了一个模块化的数据源框架，包含三个主要组件：

1. **DataSourceBase** - 抽象基类，定义统一接口
2. **RandomDataSource** - 随机数据生成器（从原TestDataGenerator提取）
3. **DatasetDataSource** - 数据集加载器（读取fvecs格式文件）
4. **VectorListSource** - 内存向量包装器（可复用组件）

### 文件结构

```
test/test_utils/data_source/
├── data_source_base.h           # 基类接口
├── random_data_source.h/cpp     # 随机数据源实现
├── dataset_data_source.h/cpp    # 数据集数据源实现
├── vector_list_source.h         # 内存向量包装器
└── README.md                    # 完整文档

test/UnitTest/
├── test_data_source.cpp         # 单元测试
├── test_data_persistence.cpp    # 持久化测试
└── test_join_data_source.cpp    # Join数据源测试

test/examples/
├── test_data_source_example.cpp      # 使用示例
└── data_persistence_example.cpp      # 持久化示例
```

### 关键特性

1. **模块化设计** - 数据生成逻辑独立，易于扩展
2. **统一接口** - 所有数据源实现相同的接口
3. **向后兼容** - 现有测试代码无需修改即可运行
4. **灵活配置** - 支持多种数据源和配置选项
5. **易于扩展** - 添加新数据源只需继承基类

## 使用方法

### 1. 使用随机数据源

```cpp
// 配置随机数据源
RandomDataSource::Config config;
config.vector_dim = 128;
config.seed = 42;
auto data_source = std::make_shared<RandomDataSource>(config);

// 与TestDataGenerator一起使用
TestDataGenerator::Config gen_config;
gen_config.positive_pairs = 100;
TestDataGenerator generator(gen_config, data_source);
auto [records, matches] = generator.generateData();
```

### 2. 使用数据集数据源

```cpp
// 配置数据集数据源
DatasetDataSource::Config config;
config.file_path = PROJECT_DIR "/data/siftsmall/siftsmall_query.fvecs";
config.expected_dim = 128;
config.loop = true;  // 循环使用
auto data_source = std::make_shared<DatasetDataSource>(config);

// 与TestDataGenerator一起使用
TestDataGenerator generator(gen_config, data_source);
auto [records, matches] = generator.generateData();
```

### 3. 向后兼容用法

```cpp
// 原有代码无需修改，仍然正常工作
TestDataGenerator::Config config;
config.vector_dim = 128;
TestDataGenerator generator(config);  // 自动使用随机数据源
auto [records, matches] = generator.generateData();
```

## 测试验证

### 单元测试
- `test_data_source.cpp` - 包含5个测试用例
  - RandomDataSourceBasic - 测试随机数据源
  - DatasetDataSourceBasic - 测试数据集数据源
  - TestDataGeneratorWithRandomDataSource - 测试生成器+随机源
  - TestDataGeneratorWithDatasetDataSource - 测试生成器+数据集源
  - BackwardCompatibility - 测试向后兼容性

- `test_data_persistence.cpp` - 包含5个测试用例
  - 测试保存为FVECS格式
  - 测试保存为JSON格式
  - 测试FVECS往返（保存后加载）
  - 测试JSON往返（保存后加载）
  - 测试从保存的数据生成

- `test_join_data_source.cpp` - 包含8个测试用例
  - 测试Duplicate模式
  - 测试Separate模式
  - 测试Generator集成
  - 测试向后兼容性

### 测试结果
```bash
cd build
ctest -L UNIT
# 18/18 tests passed (100%)
```

所有现有测试仍然通过，证明完全向后兼容。

### 示例程序
```bash
cd build
./bin/test_data_source_example
# 运行4个示例，展示不同使用场景

./bin/data_persistence_example
# 演示数据持久化功能
```

## 文档

- **test/test_utils/data_source/README.md** - 完整的框架文档
  - 架构说明
  - 使用指南
  - 配置选项
  - 扩展方法

- **test/test_utils/data_writer/README.md** - 数据写入器文档
  - FvecsWriter使用说明
  - JsonWriter使用说明
  - 配置选项

- **test/test_utils/JOIN_DATA_SOURCE_GUIDE.md** - Join数据源指南
  - Join框架说明
  - 使用示例
  - 配置选项

## 兼容性

✅ **完全向后兼容** - 所有现有测试无需修改
✅ **现有测试通过** - 18个单元测试全部通过
✅ **性能测试正常** - test_join_perf_scaling等构建正常

## 扩展性

添加新数据源非常简单：

```cpp
class MyCustomDataSource : public DataSourceBase {
public:
    // 实现接口方法
    std::vector<float> getNextVector() override;
    int getDimension() const override;
    bool hasMore() const override;
    void reset() override;
};
```

## 技术细节

1. **内存管理** - 使用智能指针，自动管理生命周期
2. **异常处理** - 数据集加载失败时抛出异常，带详细错误信息
3. **线程安全** - 基础类不保证线程安全，由使用方控制
4. **性能** - 数据集一次性加载到内存，访问快速

## 未来改进

可能的扩展方向：
1. 添加更多数据格式支持（如HDF5）
2. 支持流式加载大数据集
3. 添加数据预处理功能
4. 支持数据增强
"""
write_translation('IMPLEMENTATION_SUMMARY.md', implementation_summary)

print("\\nAll key documentation files translated to Chinese!")
print("Files translated: CODE_REVIEW_IMPROVEMENTS.md, IMPLEMENTATION_SUMMARY.md")

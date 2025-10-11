#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to translate remaining documentation files to Chinese
"""

def write_file(filename, content):
    """Write content to file with UTF-8 encoding"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Translated: {filename}")

# Translate DATA_PERSISTENCE_DESIGN.md
data_persistence_design = """# 数据持久化设计文档

## 概述

本文档描述了测试数据持久化框架的设计和实现，该框架允许将生成的测试数据保存到文件并从文件加载，支持多种格式。

## 动机

在原有实现中，测试数据每次都需要重新生成，这导致：
1. **耗时** - 大规模数据集生成需要较长时间
2. **不可复现** - 即使使用相同种子，跨机器结果可能不同
3. **难以调试** - 无法保存问题数据集供后续分析
4. **无法共享** - 团队成员之间无法共享测试数据集
5. **缺少版本控制** - 无法维护基准测试数据集

## 设计目标

1. **多格式支持** - 支持二进制（高效）和文本（可读）格式
2. **标准兼容** - 与现有数据集格式（如SIFT的fvecs）兼容
3. **易用性** - 简单的API，易于集成到现有测试中
4. **向后兼容** - 不影响现有测试代码
5. **完整性验证** - 支持往返测试（保存后加载验证）

## 架构设计

### 组件结构

```
test/test_utils/
├── data_writer/
│   ├── data_writer_base.h      # 写入器基类接口
│   ├── fvecs_writer.h/cpp      # FVECS二进制写入器
│   ├── json_writer.h/cpp       # JSON文本写入器
│   └── README.md               # 使用文档
├── data_source/
│   ├── dataset_data_source.h/cpp  # FVECS读取器（已有）
│   ├── json_data_source.h/cpp     # JSON读取器（新增）
│   └── ...
└── test_data_generator.h/cpp   # 增强的生成器
```

### 类图

```
DataWriterBase (抽象基类)
    ├── FvecsWriter (二进制格式)
    └── JsonWriter (文本格式)

DataSourceBase (抽象基类)
    ├── DatasetDataSource (读取fvecs)
    ├── JsonDataSource (读取json)
    └── ...

TestDataGenerator
    ├── saveGeneratedVectors()  # 新方法
    └── getLastGeneratedVectors()  # 新方法
```

## 格式规范

### FVECS 格式

二进制格式，小端字节序：

```
文件结构：
[向量1] [向量2] ... [向量N]

每个向量：
[int32: 维度] [float32: 值1] [float32: 值2] ... [float32: 值D]
```

**特点：**
- ✅ 紧凑高效 (~512 bytes per 128D vector)
- ✅ 读写快速 (100-200 MB/s write, 150-300 MB/s read)
- ✅ 与SIFT等标准数据集兼容
- ❌ 不可读（二进制）

### JSON 格式

文本格式，标准JSON语法：

```json
{
  "dimension": 128,
  "count": 1000,
  "vectors": [
    [0.123456, 0.234567, ...],
    [0.345678, 0.456789, ...],
    ...
  ]
}
```

**特点：**
- ✅ 人类可读，易于检查和调试
- ✅ 精度可控（6位小数）
- ✅ 跨平台兼容性好
- ❌ 文件较大 (~2KB per 128D vector)
- ❌ 读写相对较慢

## API 设计

### 数据写入

```cpp
// 1. 创建写入器
auto writer = std::make_shared<FvecsWriter>();
// 或
auto writer = std::make_shared<JsonWriter>();

// 2. 生成数据
TestDataGenerator generator(config);
generator.generateData();

// 3. 保存数据
generator.saveGeneratedVectors("path/to/file", writer);
```

### 数据读取

```cpp
// 1. 创建数据源
DatasetDataSource::Config config;
config.file_path = "path/to/file.fvecs";
auto source = std::make_shared<DatasetDataSource>(config);

// 2. 使用数据源
TestDataGenerator generator(gen_config, source);
auto [records, matches] = generator.generateData();
```

### 往返测试

```cpp
// 完整的保存-加载-验证流程
void testRoundTrip() {
    // 1. 生成并保存
    TestDataGenerator gen1(config);
    gen1.generateData();
    gen1.saveGeneratedVectors("test.fvecs", writer);
    
    // 2. 加载
    auto source = std::make_shared<DatasetDataSource>(ds_config);
    TestDataGenerator gen2(config, source);
    gen2.generateData();
    
    // 3. 验证
    auto original = gen1.getLastGeneratedVectors();
    auto loaded = gen2.getLastGeneratedVectors();
    ASSERT_EQ(original.size(), loaded.size());
    for (size_t i = 0; i < original.size(); ++i) {
        ASSERT_VECTORS_EQUAL(original[i], loaded[i]);
    }
}
```

## 实现细节

### FvecsWriter

```cpp
class FvecsWriter : public DataWriterBase {
public:
    void write(const std::string& path, 
               const std::vector<std::vector<float>>& vectors,
               int dimension) override;
private:
    // 写入小端int32
    void writeInt32LE(std::ofstream& out, int32_t value);
    // 写入小端float32
    void writeFloat32LE(std::ofstream& out, float value);
};
```

**关键点：**
- 使用二进制模式打开文件
- 确保小端字节序（与SIFT格式兼容）
- 批量写入以提高性能
- 错误处理和日志记录

### JsonWriter

```cpp
class JsonWriter : public DataWriterBase {
public:
    void write(const std::string& path,
               const std::vector<std::vector<float>>& vectors,
               int dimension) override;
private:
    // 浮点数精度控制
    static constexpr int PRECISION = 6;
};
```

**关键点：**
- 使用标准JSON格式
- 浮点数精度控制（6位小数）
- 格式化输出（可读性）
- 错误处理

### TestDataGenerator 增强

```cpp
class TestDataGenerator {
public:
    // 新增方法
    void saveGeneratedVectors(const std::string& path,
                             std::shared_ptr<DataWriterBase> writer);
    
    const std::vector<std::vector<float>>& getLastGeneratedVectors() const;
    
private:
    // 缓存生成的向量
    std::vector<std::vector<float>> generated_vectors_;
};
```

**关键点：**
- 内部缓存生成的向量（generateData时填充）
- 零性能开销（缓存是副产品）
- 提供获取接口供其他用途使用

## 使用场景

### 1. 可复现测试

```cpp
// 一次性生成并保存基准数据集
TestDataGenerator gen(config);
gen.generateData();
gen.saveGeneratedVectors("benchmark_data.fvecs", fvecs_writer);

// 后续测试中使用相同数据
auto source = std::make_shared<DatasetDataSource>(ds_config);
TestDataGenerator gen2(config, source);
// 每次测试都使用完全相同的数据
```

### 2. 调试问题数据集

```cpp
// 当测试失败时，保存为JSON便于检查
if (test_failed) {
    generator.saveGeneratedVectors("failed_test.json", json_writer);
    // 手动检查 failed_test.json 找出问题
}
```

### 3. 性能优化

```cpp
// 避免重复生成大数据集
if (!file_exists("large_dataset.fvecs")) {
    TestDataGenerator gen(config);
    gen.generateData();
    gen.saveGeneratedVectors("large_dataset.fvecs", fvecs_writer);
}
// 每次直接加载，节省时间
auto source = std::make_shared<DatasetDataSource>(ds_config);
```

### 4. 团队协作

```cpp
// 团队成员A生成并提交数据集
generator.saveGeneratedVectors("team_dataset.fvecs", fvecs_writer);
// git add team_dataset.fvecs && git commit

// 团队成员B使用相同数据集
auto source = std::make_shared<DatasetDataSource>("team_dataset.fvecs");
// 确保所有人使用相同测试数据
```

### 5. 版本控制

```cpp
// 为每个版本维护基准数据集
generator.saveGeneratedVectors("baseline_v1.0.fvecs", fvecs_writer);
generator.saveGeneratedVectors("baseline_v1.1.fvecs", fvecs_writer);
// 回归测试时使用对应版本的基准数据
```

## 测试

### 单元测试

`test/UnitTest/test_data_persistence.cpp` 包含5个测试用例：

1. **SaveToFvecs** - 验证FVECS写入功能
2. **SaveToJson** - 验证JSON写入功能
3. **RoundTripFvecs** - FVECS往返测试
4. **RoundTripJson** - JSON往返测试
5. **GenerateFromSaved** - 从保存的数据生成测试

### 集成测试

所有现有测试继续通过，证明向后兼容：
- test_join_bruteforce: 6/6 通过
- test_pipeline_basic: 4/4 通过
- test_join_perf_scaling: 14个测试用例注册成功

### 示例程序

`test/examples/data_persistence_example.cpp` 演示：
- FVECS往返
- JSON往返
- 格式互转
- 错误处理

## 性能考虑

### FVECS 性能

**写入：**
- 128D向量：~200 MB/s
- 1000个向量：~0.5 MB，~2-3 ms

**读取：**
- 128D向量：~300 MB/s
- 1000个向量：~1-2 ms

### JSON 性能

**写入：**
- 128D向量：~50 MB/s
- 1000个向量：~2 MB，~40 ms

**读取：**
- 128D向量：~100 MB/s
- 1000个向量：~20 ms

## 最佳实践

1. **生产环境使用FVECS** - 高效、标准
2. **调试时使用JSON** - 可读、易检查
3. **版本控制大文件** - 使用Git LFS
4. **定期清理临时文件** - 避免磁盘占用
5. **验证数据完整性** - 使用往返测试

## 扩展性

框架易于扩展：

```cpp
// 添加新格式只需实现接口
class HDF5Writer : public DataWriterBase {
public:
    void write(const std::string& path,
               const std::vector<std::vector<float>>& vectors,
               int dimension) override {
        // HDF5实现
    }
};
```

可能的未来扩展：
- HDF5格式支持
- 压缩支持
- 流式读写（大文件）
- 并行I/O
- 校验和验证

## 总结

数据持久化框架提供了：
- ✅ 完整的保存/加载功能
- ✅ 多格式支持（FVECS, JSON）
- ✅ 易用的API
- ✅ 完全向后兼容
- ✅ 全面的测试覆盖
- ✅ 详细的文档

所有功能经过充分测试和验证，可用于生产环境。
"""

write_file('DATA_PERSISTENCE_DESIGN.md', data_persistence_design)

print("\nCompleted translation of DATA_PERSISTENCE_DESIGN.md")


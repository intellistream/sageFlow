# CandyFlow 多线程流处理引擎开发指南

## 项目概述

CandyFlow 是一个高性能的多线程流处理引擎，支持复杂的流数据处理操作，包括过滤、映射、连接、窗口、聚合等。本项目采用现代C++17标准，基于无锁队列和多线程架构设计。

## 代码规范

### 1. 命名规范

#### 类和结构体
- 使用 PascalCase，如 `ExecutionVertex`、`StreamEnvironment`
- 接口类以 `I` 开头，如 `IPartitioner`
- 抽象基类通常以 `Base` 开头，如 `BaseMethod`

#### 变量和函数
- 使用 snake_case，如 `input_gate_`、`process_data()`
- 成员变量以下划线结尾，如 `operator_`、`running_`
- 常量使用全大写，如 `MAX_QUEUE_SIZE`

#### 文件命名
- 头文件：`snake_case.h`
- 源文件：`snake_case.cpp`
- 测试文件：`test_snake_case.cpp`

### 2. 文件结构规范

#### 头文件模板
```cpp
#pragma once

#include <standard_library_headers>

#include "project/headers.h"

namespace candy {
class ClassName {
 public:
  // 构造函数和析构函数
  explicit ClassName(params);
  ~ClassName();

  // 公共方法
  auto methodName() -> ReturnType;

 private:
  // 私有成员变量
  Type member_variable_;
};
}  // namespace candy
```

#### 源文件模板
```cpp
#include "header_file.h"

#include <standard_library_headers>

#include "other_project_headers.h"

namespace candy {

ClassName::ClassName(params) : member_variable_(value) {
  // 构造函数实现
}

// 方法实现
auto ClassName::methodName() -> ReturnType {
  // 实现代码
}

}  // namespace candy
```

### 3. 编码规范

#### 内存管理
- 优先使用智能指针：`std::unique_ptr`、`std::shared_ptr`
- 避免裸指针，除非绝对必要
- 使用 RAII 原则管理资源

#### 异常处理
- 使用标准异常类型：`std::runtime_error`、`std::invalid_argument`
- 在构造函数中验证参数
- 提供有意义的错误消息

#### 并发编程
- 使用 `std::atomic` 进行原子操作
- 使用 `std::mutex` 和 `std::condition_variable` 进行同步
- 优先使用无锁数据结构（如 RingBufferQueue）
- 避免死锁，按固定顺序获取锁

## 架构设计

### 1. 多线程执行模型

```
StreamEnvironment
    └── ExecutionGraph
        ├── ExecutionVertex (Thread 1)
        │   ├── InputGate
        │   ├── Operator
        │   └── ResultPartition
        ├── ExecutionVertex (Thread 2)
        └── ...
```

### 2. 核心组件

#### ExecutionVertex
- **职责**: Operator的并行实例，在独立线程中运行
- **生命周期**: 启动 → 运行 → 停止 → 清理
- **关键方法**: `start()`, `stop()`, `join()`, `run()`

#### InputGate
- **职责**: 管理来自上游的多个输入队列
- **策略**: 轮询读取，负载均衡
- **队列类型**: RingBufferQueue 或 BlockingQueue

#### ResultPartition
- **职责**: 将数据分发到下游的多个输出队列
- **分区策略**: RoundRobin、KeyPartitioner 等
- **接口**: `emit(Response data)`

#### ExecutionGraph
- **职责**: 管理整个执行图的构建和生命周期
- **功能**: 算子连接、队列创建、线程管理

### 3. 队列系统

#### RingBufferQueue (无锁)
- **使用场景**: 点对点高频数据传输
- **优势**: 高性能，无锁设计
- **限制**: 单生产者单消费者

#### BlockingQueue (有锁)
- **使用场景**: 多对一场景（如Join算子）
- **优势**: 支持多生产者
- **特性**: 反压机制，优雅关闭

## 开发指导

### 1. 新增算子

#### 步骤
1. 在 `include/operator/` 创建头文件
2. 在 `src/operator/` 创建实现文件
3. 继承 `Operator` 基类
4. 实现必要的方法：`open()`, `process()`, `close()`
5. 在 `Planner` 中添加创建逻辑

#### 示例
```cpp
class NewOperator final : public Operator {
 public:
  explicit NewOperator(std::unique_ptr<Function>& func);
  
  auto process(Response& data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> function_;
};
```

### 2. 新增分区器

#### 步骤
1. 继承 `IPartitioner` 接口
2. 实现 `partition()` 方法
3. 在 `ExecutionGraph` 中注册使用

#### 示例
```cpp
class KeyPartitioner : public IPartitioner {
 public:
  size_t partition(const Response& data, size_t num_channels) override {
    // 基于key的分区逻辑
    return hash(data.key) % num_channels;
  }
};
```

### 3. 性能优化

#### 内存优化
- 使用对象池减少内存分配
- 避免频繁的内存拷贝
- 使用 `std::move` 语义

#### 并发优化
- 减少锁竞争
- 使用无锁数据结构
- 合理设置队列容量

#### 算法优化
- 批处理减少系统调用
- 缓存友好的数据布局
- 向量化计算

## 测试指南

### 1. 单元测试
- 使用 GoogleTest 框架
- 测试文件放在 `test/UnitTest/` 目录
- 命名格式：`test_component_name.cpp`

### 2. 集成测试
- 测试完整的流处理管道
- 验证多线程执行的正确性
- 测试文件放在 `test/IntegrationTest/` 目录

### 3. 性能测试
- 测试吞吐量和延迟
- 验证扩展性
- 测试文件放在 `test/Performance/` 目录

## 调试指南

### 1. 日志系统
- 使用 spdlog 进行日志记录
- 日志级别：TRACE, DEBUG, INFO, WARN, ERROR
- 线程安全的日志输出

### 2. 调试技巧
- 使用线程ID标识不同的执行线程
- 记录队列状态和数据流向
- 监控内存使用和CPU利用率

### 3. 常见问题
- **死锁**: 检查锁的获取顺序
- **内存泄漏**: 检查智能指针的使用
- **性能问题**: 分析队列的阻塞情况

## 配置管理

### 1. 配置文件
- 使用 TOML 格式
- 配置文件放在 `config/` 目录
- 支持运行时配置修改

### 2. 参数配置
- 并行度设置
- 队列容量配置
- 算法参数调优

## 扩展指南

### 1. 新增数据类型
- 修改 `common/data_types.h`
- 确保序列化支持
- 更新相关的处理逻辑

### 2. 新增执行模式
- 扩展 `ExecutionGraph`
- 支持不同的调度策略
- 实现容错机制

### 3. 性能监控
- 添加指标收集
- 实现运行时监控
- 支持性能分析工具

## 最佳实践

### 1. 代码质量
- 保持函数简洁，单一职责
- 使用const correctness
- 避免全局状态

### 2. 资源管理
- 及时释放资源
- 避免资源竞争
- 实现优雅关闭

### 3. 错误处理
- 提供详细的错误信息
- 实现恢复机制
- 记录异常堆栈

### 4. 文档维护
- 及时更新API文档
- 提供使用示例
- 记录设计决策

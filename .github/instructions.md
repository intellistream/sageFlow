# 指南：为 CandyFlow 项目贡献代码

本文档为 GitHub Copilot 提供了与 CandyFlow 项目协作的上下文和指南。

## 1. 项目概述

CandyFlow 是一个用 C++17 编写的高性能、多线程流处理引擎。它旨在提供一个可扩展的框架，用于构建复杂的数据处理管道，支持过滤、映射、连接（Join）、窗口（Windowing）和聚合（Aggregation）等操作。引擎的核心设计基于多线程执行模型，其中每个算子（Operator）的实例都在独立的线程（`ExecutionVertex`）中运行，通过高效的队列系统（无锁 `RingBufferQueue` 和有锁 `BlockingQueue`）进行通信。

## 2. 技术栈与环境

*   **主要语言**: C++ (使用 C++17 标准)
*   **构建系统**: CMake
*   **操作系统**: 跨平台，但当前主要在 Windows 上开发
*   **核心依赖**:
    *   `gtest` 用于单元测试和集成测试
*   **并发模型**: 基于 `std::thread`、`std::mutex`、`std::atomic` 和自定义队列的线程池/Actor 模型。

## 3. 项目结构

以下是关键目录的概览：

*   `src/operator/`: 包含所有流处理算子（Operator）的源文件。
    *   `join_operator_methods/`: Join 算子内部使用的不同连接策略实现。
*   `src/execution/`: 包含多线程执行引擎的核心组件，如 `ExecutionVertex`, `InputGate`, `ResultPartition` 和队列实现。
*   `include/`: 包含所有组件的头文件。
*   `test/`: 包含所有单元测试、集成测试和性能测试。
*   `config/`: 存放运行时配置文件（例如 TOML 格式）。

## 4. 编码规范

所有生成的代码必须严格遵守以下规范。

### 4.1 命名约定

*   **类/结构体**: `PascalCase` (例如: `ExecutionGraph`, `JoinOperator`)。
*   **函数/方法**: `snake_case` (例如: `process_data`, `emit_record`)。
*   **变量**: `snake_case` (例如: `record_count`)。
*   **成员变量**: `snake_case` 并以下划线结尾 (例如: `input_gate_`, `is_running_`)。
*   **常量/枚举**: `ALL_CAPS_SNAKE_CASE` (例如: `MAX_BUFFER_SIZE`)。
*   **文件**: `snake_case.h` / `snake_case.cpp`。

### 4.2 代码风格

*   **智能指针**: 优先使用 `std::unique_ptr` 和 `std::shared_ptr` 管理动态内存。避免使用裸指针和 `new`/`delete`。
*   **RAII**: 严格遵循 RAII 原则管理所有资源（内存、文件、锁等）。
*   **Const Correctness**: 尽可能使用 `const` 来确保不变量。
*   **函数返回类型**: 使用 C++11 的尾置返回类型语法 (`auto function_name() -> ReturnType`)。
*   **命名空间**: 所有代码都应位于 `candy` 命名空间内。

### 4.3 并发编程

*   **线程安全**: 这是项目的核心要求。所有对共享状态的访问都必须是线程安全的。
*   **同步原语**:
    *   使用 `std::mutex` 和 `std::lock_guard` 或 `std::scoped_lock` 来保护临界区。
    *   使用 `std::atomic` 来处理简单的原子计数器或标志。
    *   使用 `std::condition_variable` 进行线程间的等待和通知。
*   **避免死锁**: 当需要获取多个锁时，必须按照固定的全局顺序获取。

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

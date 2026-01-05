# Queue Blocking 问题分析报告

## 问题描述

在性能测试中，StreamEnvironment 已经调用 `stop()` 后，仍然出现大量 "Failed to push to queue after 1000 retries" 警告，持续约 10 分钟。

## 时间线分析

```
07:26:59.137 - StreamEnvironment.stop() 被调用
07:26:59.142 - All ExecutionVertices stopped (running_ = false)
07:26:59.390 - 开始出现 "Failed to push" 警告 ← 249ms 后
07:36:33.946 - 最后一条 "Failed to push" 警告 ← 持续 9分34秒！
```

## 根本原因

### 1. **OutputOperator 不检查 `running_` 标志**

```cpp
// src/operator/output_operator.cpp:63
auto sageFlow::OutputOperator::run(Collector& collector) -> void {
  std::unique_ptr<VectorRecord> record = nullptr;
  while (stream_ && (record = stream_->Next())) {  // ← 没有检查 running_！
    auto resp = Response{ResponseType::Record, std::move(record)};
    apply(std::move(resp), -1, collector);
  }
}
```

**关键问题**: 
- `OutputOperator::run()` 只检查 `stream_->Next()` 是否返回数据
- **不检查** `ExecutionVertex::running_` 标志
- 即使 `stop()` 被调用，OutputOperator 仍然继续从数据源读取和发送数据

### 2. **停止顺序导致的死锁**

```cpp
// src/execution/execution_vertex.cpp:53
if (source_op != nullptr) [[unlikely]] {
    source_op->run(collector);  // ← 阻塞在这里，不会返回
} else {
    while (running_) {  // ← 非源算子会检查 running_
        // ...
    }
}
```

**执行流程**:
1. `StreamEnvironment::stop()` 被调用
2. `ExecutionGraph::stop()` 设置所有 `running_ = false`
3. **非源算子**（JoinOperator, SinkOperator）检测到 `running_ = false`，退出循环
4. **源算子**（OutputOperator）继续在 `stream_->Next()` 循环中
5. 下游队列的 Consumer 已经退出，Queue 快速填满
6. OutputOperator 尝试 push 到满队列，不断重试
7. 直到数据源耗尽（2000+ records），OutputOperator 才退出

### 3. **广播模式放大问题**

ClusteredJoin 在冷启动阶段使用广播模式：
- 每条记录复制到所有 subtask（例如 p=2 → 2倍数据）
- 广播需要更多的队列空间
- ResultPartition::emitBroadcast 多次调用 pushWithRetry

```cpp
// src/execution/result_partition.cpp:73
void ResultPartition::emitBroadcast(Response&& data, int slot, 
                                     const std::vector<QueuePtr>& channels) const {
  for (size_t i = 0; i + 1 < channels.size(); ++i) {
    Response cloned{...};
    pushWithRetry(channels[i], ...);  // ← 每个 channel 都可能阻塞
  }
}
```

### 4. **重试机制不足**

```cpp
// src/execution/result_partition.cpp:118
bool ResultPartition::pushWithRetry(const QueuePtr& queue, TaggedResponse&& tagged) {
  constexpr int kMaxRetries = 10;  // ← 只重试 10 次
  constexpr int kRetryDelayUs = 50;
  
  for (int retry = 0; retry < kMaxRetries; ++retry) {
    if (queue->push(std::move(tagged))) return true;
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
  
  // 超过最大重试次数，数据被丢弃！
  SAGEFLOW_LOG_WARN("ResultPartition", "Failed to push...");
  return false;  // ← 数据丢失，但 OutputOperator 继续发送
}
```

**问题**:
- 重试 10 次 × 50μs = 500μs 后放弃
- 在 Consumer 已停止的情况下，队列永远不会被消费
- 每条记录都会重试失败，产生海量警告日志

## 为什么测试最终完成？

虽然队列阻塞，但测试最终完成的原因：

1. **OutputOperator 最终耗尽数据源**
   - `stream_->Next()` 最终返回 nullptr
   - OutputOperator 退出 `while` 循环
   - `operator_->close()` 被调用
   - `VERTEX finished` 日志输出

2. **丢弃的数据不影响测试结果**
   - 性能测试只关心延迟和吞吐量
   - 丢弃的数据不会被计入统计
   - JoinOperator 已经处理了大部分数据

3. **超时机制触发**
   ```bash
   timeout 600 ./build/bin/test_join_datasource_modes
   ```
   - 如果 OutputOperator 迟迟不退出，超时会强制终止

## 影响分析

### 性能影响
- **CPU 浪费**: 数百万次无效的 push 重试
- **日志膨胀**: 15000+ 条警告日志（12560537 - 12545716）
- **测试时间**: 本应 30 秒的测试拖长到 10 分钟

### 正确性影响
- **数据丢失**: pushWithRetry 失败后直接丢弃数据
- **召回率影响**: 如果在正常运行阶段发生阻塞，会导致匹配对丢失

## 修复方案

### 方案 1: OutputOperator 检查 running_ 标志（推荐）

```cpp
// src/operator/output_operator.cpp
auto sageFlow::OutputOperator::run(Collector& collector) -> void {
  std::unique_ptr<VectorRecord> record = nullptr;
  // 添加 running_ 检查
  while (running_ && stream_ && (record = stream_->Next())) {
    auto resp = Response{ResponseType::Record, std::move(record)};
    apply(std::move(resp), -1, collector);
  }
  SAGEFLOW_LOG_INFO("OUTPUT", "OutputOperator exiting: running_={}", running_.load());
}
```

**优点**:
- 简单直接，一行代码修复
- 响应 stop() 信号，快速退出
- 不再产生无效的 push 尝试

**缺点**:
- OutputOperator 需要访问 ExecutionVertex 的 `running_` 标志（需要传递引用）

### 方案 2: ResultPartition 使用阻塞式 push

```cpp
// src/execution/result_partition.cpp
bool ResultPartition::pushWithRetry(const QueuePtr& queue, TaggedResponse&& tagged) {
  // 检查下游是否已停止（需要新增机制）
  if (downstream_stopped_) {
    SAGEFLOW_LOG_DEBUG("ResultPartition", "Downstream stopped, dropping data");
    return false;
  }
  
  // 使用阻塞式 push（永久等待）
  queue->pushBlocking(std::move(tagged));
  return true;
}
```

**优点**:
- 不会丢失数据
- 不会产生警告日志

**缺点**:
- 需要新增 `downstream_stopped_` 标志传递机制
- 如果队列永久满，会永久阻塞（需要配合方案 1）

### 方案 3: 增大队列容量（不推荐）

```cpp
// 临时缓解，不解决根本问题
constexpr size_t QUEUE_CAPACITY = 10000;  // 从 1000 增大到 10000
```

**优点**:
- 快速缓解症状

**缺点**:
- 不解决根本问题
- 内存消耗增大
- 广播模式下仍然会阻塞

## 推荐修复步骤

1. **立即修复**: 实现方案 1
   - 让 OutputOperator 检查 `running_` 标志
   - 快速响应 stop() 信号

2. **后续优化**: 实现优雅停止机制
   - 数据源先停止（stop producing）
   - 下游排干队列（drain queues）
   - 算子依次关闭

3. **测试验证**:
   ```bash
   # 应该在 1 秒内退出，无警告日志
   timeout 10 ./build/bin/test_join_datasource_modes --gtest_filter="*clustered*"
   ```

## 相关文件

- `src/operator/output_operator.cpp:63` - OutputOperator::run()
- `src/execution/execution_vertex.cpp:53` - ExecutionVertex::run()
- `src/execution/result_partition.cpp:118` - pushWithRetry()
- `src/execution/execution_graph.cpp:148` - ExecutionGraph::stop()

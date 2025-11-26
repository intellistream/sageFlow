# SageFlow 架构重构清单：统一状态分片与上下文感知

## 概述 (Overview)

本清单旨在重构 sageFlow 的执行层与算子层，引入 **RuntimeContext** 实现线程身份识别，并通过 **WindowState** 抽象层将计算逻辑与状态存储解耦，从而在同一套代码中支持 **Share-Nothing（分区）** 和 **Shared-Memory（共享）** 两种架构。

### 重构目标 (Goals)

1. **线程身份识别**: 让算子在运行时知道"我是谁（哪个并发实例）"，以便路由到正确的状态分片
2. **状态存储解耦**: 将计算逻辑与状态存储分离，支持不同的状态管理策略
3. **架构统一**: 在同一套代码中支持分区模型和共享索引模型
4. **向后兼容**: 保持现有测试和代码的兼容性

### 当前进展 (Current Progress)

- ✅ **第零阶段**: 完成连接策略抽象（Connection Strategy Abstraction）
  - 实现 `IConnectionStrategy` 接口
  - 实现 `PartitionedConnectionStrategy` (分区策略)
  - 实现 `SharedQueueConnectionStrategy` (共享队列策略)
  - 所有现有测试通过

---

## 第一阶段：执行上下文注入 (Execution Context Injection)

**目标**: 让算子在运行时知道"我是谁（哪个并发实例）"，以便路由到正确的状态分片。

### 1.1 定义 RuntimeContext

**文件**: `include/execution/runtime_context.h`

**内容**:
```cpp
#pragma once

#include <cstddef>
#include <string>

namespace sageFlow {

/**
 * @brief 运行时上下文，提供算子实例的执行环境信息
 * 
 * RuntimeContext 为算子提供关键的运行时信息：
 * - 子任务索引：标识当前算子实例在并行实例组中的位置
 * - 并行度：当前算子的总并行实例数
 * - 连接策略类型：指示使用的连接策略（分区/共享队列）
 * 
 * 这些信息使算子能够：
 * 1. 识别自己的身份（哪个并发实例）
 * 2. 路由到正确的状态分片
 * 3. 根据连接策略调整行为
 */
class RuntimeContext {
public:
    RuntimeContext(size_t subtask_index, size_t parallelism)
        : subtask_index_(subtask_index)
        , parallelism_(parallelism) {}

    /**
     * @brief 获取当前子任务索引（0-based）
     * @return 子任务索引，范围 [0, parallelism)
     */
    size_t getSubtaskIndex() const { return subtask_index_; }

    /**
     * @brief 获取算子的并行度
     * @return 并行实例总数
     */
    size_t getParallelism() const { return parallelism_; }

    /**
     * @brief 获取任务名称（用于调试和日志）
     * @return 格式化的任务名称，如 "Operator[2/8]"
     */
    std::string getTaskName() const {
        return "Task[" + std::to_string(subtask_index_) + "/" + 
               std::to_string(parallelism_) + "]";
    }

private:
    size_t subtask_index_;  // 当前子任务索引
    size_t parallelism_;     // 并行度
};

} // namespace sageFlow
```

**设计考虑**:
- 轻量级设计，只包含必要的上下文信息
- 不可变对象，线程安全
- 可扩展以支持未来的上下文信息（如连接策略类型）

---

### 1.2 在 ExecutionVertex 中创建并注入 RuntimeContext

**修改文件**: `src/execution/execution_vertex.cpp`

**修改位置**: `ExecutionVertex::run()` 方法

**修改内容**:
```cpp
void ExecutionVertex::run() const {
    SAGEFLOW_LOG_DEBUG("VERTEX", "{} started thread={}", 
                      name_, (size_t)std::hash<std::thread::id>{}(std::this_thread::get_id()));

    // 创建运行时上下文
    RuntimeContext runtime_context(subtask_index_, operator_->get_parallelism());

    auto source_op = dynamic_cast<OutputOperator*>(operator_.get());
    try {
        // 打开算子，传入运行时上下文
        operator_->open(runtime_context);
        
        // 创建collector，将emit操作注册到collector中
        Collector collector([this](std::unique_ptr<Response> response, int slot) {
            if (response) {
                result_partition_->emit(std::move(*response), slot);
            }
        });
        collector.set_slot_size(result_partition_->get_slot_size());
        collector.set_slots(result_partition_->get_slots());

        if (source_op != nullptr) [[unlikely]] {
            source_op->run(collector);
        } else {
            while (running_) {
                std::optional<TaggedResponse> data_opt = input_gate_->read();
                if (!data_opt) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                    continue;
                }

                Response data = std::move(data_opt->response);
                try {
                    // 传入运行时上下文
                    operator_->apply(std::move(data), data_opt->slot, collector, runtime_context);
                } catch (const std::exception& e) {
                    SAGEFLOW_LOG_ERROR("APPLY", "operator={} slot={} what={}", 
                                      operator_->name, data_opt->slot, e.what());
                    throw;
                }
            }

            // 排干剩余队列
            while (true) {
                std::optional<TaggedResponse> data_opt = input_gate_->read();
                if (!data_opt) break;
                Response data = std::move(data_opt->response);
                try {
                    operator_->apply(std::move(data), data_opt->slot, collector, runtime_context);
                } catch (const std::exception& e) {
                    SAGEFLOW_LOG_ERROR("DRAIN", "operator={} slot={} what={}", 
                                      operator_->name, data_opt->slot, e.what());
                    break;
                }
            }
        }
    } catch (const std::exception& e) {
        SAGEFLOW_LOG_ERROR("VERTEX", "Exception name={} what={}", name_, e.what());
    }

    operator_->close();
    SAGEFLOW_LOG_INFO("VERTEX", "{} finished", name_);
}
```

**影响范围**:
- 需要修改 `Operator::open()` 和 `Operator::apply()` 的签名
- 所有继承自 `Operator` 的子类需要更新签名

---

### 1.3 更新 Operator 基类接口

**修改文件**: `include/operator/operator.h`

**修改内容**:
```cpp
class Operator {
public:
    virtual ~Operator();
    explicit Operator(OperatorType type, size_t parallelism = 1);

    auto getType() const -> OperatorType;

    // 添加 RuntimeContext 参数
    virtual auto open(const RuntimeContext& context) -> void;
    virtual auto close() -> void;

    // 保留旧接口用于向后兼容（标记为 deprecated）
    virtual auto process(Response& record, int slot) -> std::optional<Response>;

    // 新接口：添加 RuntimeContext 参数
    virtual auto apply(Response&& record, int slot, Collector& collector, 
                      const RuntimeContext& context) -> void;

    void set_parallelism(size_t p);
    auto get_parallelism() const -> size_t;

    std::unique_ptr<Function> function_ = nullptr;
    OperatorType type_ = OperatorType::NONE;
    bool is_open_ = false;
    size_t parallelism_ = 1;
    bool is_available_ = true;
    std::string name = "Operator";
};
```

**实现文件**: `src/operator/operator.cpp`

```cpp
auto Operator::open(const RuntimeContext& context) -> void {
    is_open_ = true;
    // 子类可以重写此方法以使用 RuntimeContext
}

auto Operator::apply(Response&& record, int slot, Collector& collector, 
                    const RuntimeContext& context) -> void {
    // 默认实现：调用旧的 process 方法（向后兼容）
    auto result = process(record, slot);
    if (result) {
        collector.collect(std::make_unique<Response>(std::move(*result)), slot);
    }
}
```

---

## 第二阶段：窗口状态抽象 (Window State Abstraction)

**目标**: 将计算逻辑与状态存储解耦，支持分区状态和共享状态两种模式。

### 2.1 定义 WindowState 抽象接口

**文件**: `include/state/window_state.h`

**内容**:
```cpp
#pragma once

#include <memory>
#include <vector>
#include <deque>
#include <mutex>
#include "common/data_types.h"

namespace sageFlow {

/**
 * @brief 窗口状态抽象接口
 * 
 * WindowState 提供统一的窗口状态访问接口，支持：
 * 1. 分区状态（Partitioned State）：每个子任务有独立的状态
 * 2. 共享状态（Shared State）：所有子任务共享同一状态，需要同步
 */
class WindowState {
public:
    virtual ~WindowState() = default;

    /**
     * @brief 添加记录到窗口
     * @param record 待添加的记录
     * @param subtask_index 子任务索引（用于分区状态）
     */
    virtual void addRecord(std::unique_ptr<VectorRecord> record, 
                          size_t subtask_index) = 0;

    /**
     * @brief 获取窗口中的所有记录
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 窗口记录的引用（只读）
     */
    virtual const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const = 0;

    /**
     * @brief 清理过期记录
     * @param current_timestamp 当前时间戳
     * @param window_size 窗口大小
     * @param subtask_index 子任务索引（用于分区状态）
     */
    virtual void evictExpired(int64_t current_timestamp, 
                            int64_t window_size,
                            size_t subtask_index) = 0;

    /**
     * @brief 获取窗口大小
     * @param subtask_index 子任务索引（用于分区状态）
     * @return 当前窗口中的记录数
     */
    virtual size_t size(size_t subtask_index) const = 0;

    /**
     * @brief 检查状态是否为共享状态
     * @return true 表示共享状态，false 表示分区状态
     */
    virtual bool isShared() const = 0;
};

} // namespace sageFlow
```

---

### 2.2 实现分区窗口状态

**文件**: `include/state/partitioned_window_state.h`

**内容**:
```cpp
#pragma once

#include "state/window_state.h"
#include <vector>
#include <shared_mutex>

namespace sageFlow {

/**
 * @brief 分区窗口状态实现
 * 
 * 每个子任务有独立的状态分片，无需跨任务同步。
 * 适用于基于分区的 Join 方法。
 */
class PartitionedWindowState : public WindowState {
public:
    explicit PartitionedWindowState(size_t parallelism);

    void addRecord(std::unique_ptr<VectorRecord> record, 
                  size_t subtask_index) override;

    const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const override;

    void evictExpired(int64_t current_timestamp, 
                    int64_t window_size,
                    size_t subtask_index) override;

    size_t size(size_t subtask_index) const override;

    bool isShared() const override { return false; }

private:
    // 每个子任务一个独立的窗口
    std::vector<std::deque<std::unique_ptr<VectorRecord>>> partitions_;
    
    // 每个分区一个独立的互斥锁
    mutable std::vector<std::shared_mutex> mutexes_;
};

} // namespace sageFlow
```

**实现文件**: `src/state/partitioned_window_state.cpp`

```cpp
#include "state/partitioned_window_state.h"

namespace sageFlow {

PartitionedWindowState::PartitionedWindowState(size_t parallelism)
    : partitions_(parallelism), mutexes_(parallelism) {}

void PartitionedWindowState::addRecord(std::unique_ptr<VectorRecord> record, 
                                       size_t subtask_index) {
    std::unique_lock lock(mutexes_[subtask_index]);
    partitions_[subtask_index].push_back(std::move(record));
}

const std::deque<std::unique_ptr<VectorRecord>>& 
PartitionedWindowState::getRecords(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return partitions_[subtask_index];
}

void PartitionedWindowState::evictExpired(int64_t current_timestamp, 
                                         int64_t window_size,
                                         size_t subtask_index) {
    std::unique_lock lock(mutexes_[subtask_index]);
    auto& partition = partitions_[subtask_index];
    
    while (!partition.empty() && 
           partition.front()->timestamp_ < current_timestamp - window_size) {
        partition.pop_front();
    }
}

size_t PartitionedWindowState::size(size_t subtask_index) const {
    std::shared_lock lock(mutexes_[subtask_index]);
    return partitions_[subtask_index].size();
}

} // namespace sageFlow
```

---

### 2.3 实现共享窗口状态

**文件**: `include/state/shared_window_state.h`

**内容**:
```cpp
#pragma once

#include "state/window_state.h"
#include <shared_mutex>

namespace sageFlow {

/**
 * @brief 共享窗口状态实现
 * 
 * 所有子任务共享同一状态，需要跨任务同步。
 * 适用于共享索引的 Join 方法。
 */
class SharedWindowState : public WindowState {
public:
    SharedWindowState();

    void addRecord(std::unique_ptr<VectorRecord> record, 
                  size_t subtask_index) override;

    const std::deque<std::unique_ptr<VectorRecord>>& 
        getRecords(size_t subtask_index) const override;

    void evictExpired(int64_t current_timestamp, 
                    int64_t window_size,
                    size_t subtask_index) override;

    size_t size(size_t subtask_index) const override;

    bool isShared() const override { return true; }

private:
    // 所有子任务共享的窗口
    std::deque<std::unique_ptr<VectorRecord>> shared_window_;
    
    // 共享状态的读写锁
    mutable std::shared_mutex mutex_;
};

} // namespace sageFlow
```

**实现文件**: `src/state/shared_window_state.cpp`

```cpp
#include "state/shared_window_state.h"

namespace sageFlow {

SharedWindowState::SharedWindowState() = default;

void SharedWindowState::addRecord(std::unique_ptr<VectorRecord> record, 
                                  size_t subtask_index) {
    // subtask_index 在共享状态中被忽略
    std::unique_lock lock(mutex_);
    shared_window_.push_back(std::move(record));
}

const std::deque<std::unique_ptr<VectorRecord>>& 
SharedWindowState::getRecords(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return shared_window_;
}

void SharedWindowState::evictExpired(int64_t current_timestamp, 
                                    int64_t window_size,
                                    size_t subtask_index) {
    // subtask_index 在共享状态中被忽略
    std::unique_lock lock(mutex_);
    
    while (!shared_window_.empty() && 
           shared_window_.front()->timestamp_ < current_timestamp - window_size) {
        shared_window_.pop_front();
    }
}

size_t SharedWindowState::size(size_t subtask_index) const {
    // subtask_index 在共享状态中被忽略
    std::shared_lock lock(mutex_);
    return shared_window_.size();
}

} // namespace sageFlow
```

---

## 第三阶段：JoinOperator 重构

**目标**: 重构 JoinOperator 以使用 RuntimeContext 和 WindowState，支持分区和共享两种模式。

### 3.1 更新 JoinOperator 类定义

**修改文件**: `include/operator/join_operator.h`

**主要变更**:
```cpp
class JoinOperator final : public Operator {
public:
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                         const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                         const std::string& join_method_name = "bruteforce_lazy",
                         double join_similarity_threshold = 0.8,
                         bool enable_profiling = false,
                         const std::string& profile_output_path = "",
                         bool use_shared_state = false);  // 新增参数

    auto open(const RuntimeContext& context) -> void override;
    ~JoinOperator() override;

    auto apply(Response&& record, int slot, Collector& collector,
              const RuntimeContext& context) -> void override;

    // 设置左右两侧的 slot id
    void setSlots(int left_slot_id, int right_slot_id);

private:
    // 使用 WindowState 替代直接的 deque
    std::unique_ptr<WindowState> left_state_;
    std::unique_ptr<WindowState> right_state_;
    
    // 保留其他成员变量...
    bool use_shared_state_;  // 标识是否使用共享状态
    
    // 新增：存储 RuntimeContext（在 open() 时设置）
    std::unique_ptr<RuntimeContext> runtime_context_;
};
```

### 3.2 实现 JoinOperator 的状态管理

**修改文件**: `src/operator/join_operator.cpp`

**构造函数修改**:
```cpp
JoinOperator::JoinOperator(
    std::unique_ptr<Function> &join_func,
    const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
    const std::string& join_method_name,
    double join_similarity_threshold,
    bool enable_profiling,
    const std::string& profile_output_path,
    bool use_shared_state)
    : Operator(OperatorType::JOIN)
    , use_shared_state_(use_shared_state)
    , concurrency_manager_(concurrency_manager)
    , join_similarity_threshold_(join_similarity_threshold)
    , enable_profiling_(enable_profiling) {
    
    // 初始化 join function
    join_func_ = std::unique_ptr<JoinFunction>(
        dynamic_cast<JoinFunction*>(join_func.release()));
    
    // 根据配置创建状态对象（在 open() 中完成，此处延迟初始化）
}
```

**open() 方法实现**:
```cpp
auto JoinOperator::open(const RuntimeContext& context) -> void {
    Operator::open(context);
    
    // 保存 RuntimeContext
    runtime_context_ = std::make_unique<RuntimeContext>(context);
    
    // 根据配置创建窗口状态
    if (use_shared_state_) {
        left_state_ = std::make_unique<SharedWindowState>();
        right_state_ = std::make_unique<SharedWindowState>();
    } else {
        left_state_ = std::make_unique<PartitionedWindowState>(parallelism_);
        right_state_ = std::make_unique<PartitionedWindowState>(parallelism_);
    }
    
    // 创建索引（如果需要）
    if (use_index_) {
        createIndexPair(/*...*/);
    }
    
    SAGEFLOW_LOG_INFO("JOIN", "JoinOperator opened: subtask={}/{}, shared_state={}", 
                     context.getSubtaskIndex(), context.getParallelism(), 
                     use_shared_state_);
}
```

**apply() 方法实现**:
```cpp
auto JoinOperator::apply(Response&& record, int slot, Collector& collector,
                        const RuntimeContext& context) -> void {
    if (!record.record_) return;
    
    size_t subtask_index = context.getSubtaskIndex();
    auto data_ptr = std::move(record.record_);
    int64_t current_timestamp = data_ptr->timestamp_;
    
    // 确定记录属于哪一侧
    WindowState* current_state = (slot == left_slot_id_) ? left_state_.get() : right_state_.get();
    WindowState* opposite_state = (slot == left_slot_id_) ? right_state_.get() : left_state_.get();
    
    // 清理过期记录
    current_state->evictExpired(current_timestamp, window_time_ms_, subtask_index);
    opposite_state->evictExpired(current_timestamp, window_time_ms_, subtask_index);
    
    // 执行 Join 操作
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> join_results;
    
    // 根据 join 方法获取候选项并执行 join
    auto candidates = getCandidatesFromState(data_ptr.get(), opposite_state, subtask_index);
    executeJoinWithCandidates(data_ptr.get(), candidates, opposite_state, 
                             subtask_index, join_results);
    
    // 添加当前记录到窗口
    current_state->addRecord(std::move(data_ptr), subtask_index);
    
    // 发送 Join 结果
    for (auto& [result_slot, result_record] : join_results) {
        auto response = std::make_unique<Response>(
            ResponseType::DATA, std::move(result_record));
        collector.collect(std::move(response), result_slot);
    }
}
```

---

## 第四阶段：连接策略与状态管理集成

**目标**: 将连接策略（PartitionedConnectionStrategy / SharedQueueConnectionStrategy）与状态管理（PartitionedWindowState / SharedWindowState）关联起来。

### 4.1 在 ExecutionGraph 中传递配置

**修改文件**: `include/execution/execution_graph.h`

**添加方法**:
```cpp
// 添加算子并指定连接策略和状态类型
void addOperator(std::shared_ptr<Operator> op, 
                ConnectionType connection_type,
                bool use_shared_state = false);
```

**修改 OperatorInfo 结构**:
```cpp
struct OperatorInfo {
    std::shared_ptr<Operator> op;
    size_t parallelism;
    std::vector<std::unique_ptr<ExecutionVertex>> vertices;
    ConnectionType connection_type = ConnectionType::PARTITIONED;
    bool use_shared_state = false;  // 新增：标识是否使用共享状态
};
```

### 4.2 自动配置策略

**建议**: 在 StreamEnvironment 或 Planner 层添加逻辑，自动为 JoinOperator 选择合适的连接策略和状态类型：

```cpp
// 伪代码示例
if (join_method == "ivf_shared_index") {
    graph.addOperator(join_op, ConnectionType::SHARED_QUEUE, true);
} else {
    graph.addOperator(join_op, ConnectionType::PARTITIONED, false);
}
```

---

## 第五阶段：测试与验证

### 5.1 单元测试

**新增测试文件**: `test/UnitTest/test_runtime_context.cpp`
- 测试 RuntimeContext 的创建和访问
- 验证线程身份识别

**新增测试文件**: `test/UnitTest/test_window_state.cpp`
- 测试 PartitionedWindowState 的并发访问
- 测试 SharedWindowState 的并发访问
- 验证状态的正确性和线程安全性

### 5.2 集成测试

**扩展测试文件**: `test/IntegrationTest/test_pipeline_execution.cpp`
- 添加使用 SharedWindowState 的 JoinOperator 测试
- 验证分区模型和共享模型的正确性
- 性能对比测试

### 5.3 性能测试

**新增测试文件**: `test/Performance/test_state_performance.cpp`
- 对比分区状态和共享状态的性能
- 测试不同并行度下的 Recall 和吞吐量
- 验证共享状态模式下 Recall 保持在 95% 以上

---

## 第六阶段：文档与示例

### 6.1 更新 API 文档

- 更新 `Operator::open()` 和 `Operator::apply()` 的文档
- 添加 RuntimeContext 使用指南
- 添加 WindowState 使用指南

### 6.2 添加使用示例

**新增文件**: `examples/shared_state_join_example.cpp`
- 演示如何使用共享状态的 JoinOperator
- 展示连接策略的配置

---

## 实施时间表与优先级

| 阶段 | 优先级 | 预计工作量 | 依赖关系 |
|------|--------|-----------|---------|
| 第一阶段：RuntimeContext | **高** | 2-3 天 | 无 |
| 第二阶段：WindowState | **高** | 3-4 天 | 第一阶段 |
| 第三阶段：JoinOperator 重构 | **高** | 4-5 天 | 第一、二阶段 |
| 第四阶段：策略集成 | **中** | 2-3 天 | 第三阶段 |
| 第五阶段：测试验证 | **高** | 3-4 天 | 第三、四阶段 |
| 第六阶段：文档示例 | **中** | 1-2 天 | 第五阶段 |

**总计预估**: 3-4 周

---

## 风险与注意事项

### 技术风险

1. **向后兼容性**: 需要保持现有代码的兼容性，建议保留旧接口并标记为 deprecated
2. **性能影响**: RuntimeContext 的注入和 WindowState 的抽象可能带来轻微的性能开销，需要通过性能测试验证
3. **并发安全**: 共享状态的实现需要仔细设计锁策略，避免死锁和性能瓶颈

### 实施建议

1. **分阶段实施**: 严格按照阶段顺序实施，确保每个阶段完成后进行充分测试
2. **增量式重构**: 不要一次性修改所有算子，先从 JoinOperator 开始，逐步扩展到其他有状态算子
3. **性能基准**: 在重构前建立性能基准，重构后对比验证
4. **代码审查**: 关键的并发代码需要进行仔细的代码审查

---

## 后续扩展

完成本次重构后，可以考虑以下扩展：

1. **动态状态管理**: 支持运行时动态切换分区状态和共享状态
2. **状态快照与恢复**: 为 WindowState 添加快照和恢复功能，支持故障恢复
3. **状态后端抽象**: 支持不同的状态后端（内存、RocksDB 等）
4. **状态清理策略**: 支持更复杂的状态清理策略（基于时间、基于大小等）

---

## 参考资料

- [Apache Flink State Documentation](https://flink.apache.org/docs/stable/dev/datastream_api.html#working-with-state)
- [PIM-Tree: A Scalable Parallel In-Memory Index](https://dl.acm.org/doi/10.1145/3318464.3389705)
- sageFlow 当前实现：`src/operator/join_operator.cpp`
- 连接策略实现：`src/execution/partitioned_connection_strategy.cpp`, `src/execution/shared_queue_connection_strategy.cpp`

---

## 更新日志

- **2025-11-24**: 初始版本，基于当前的连接策略重构工作
- **待更新**: 各阶段实施进展

---

## 联系与反馈

如有问题或建议，请在项目 Issue 中讨论或联系开发团队。

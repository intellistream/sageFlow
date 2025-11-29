# Task C-01: JoinOperator VSJoin 集成

**优先级**: 🔴 高  
**预估工时**: 4-5 天  
**依赖**: B-01 ⏳, B-02 ⏳, B-03 ⏳, B-04 ⏳  
**输出文件**:
- 修改 `include/operator/join_operator.h`
- 修改 `src/operator/join_operator.cpp`
- `test/IntegrationTest/test_vsjoin_integration.cpp`

---

## ⚠️ 注意

此任务依赖所有 B 组任务完成，是第三批集成任务。
**请等待 B-01 ~ B-04 全部完成后再开始此任务。**

---

## 任务描述

将 VSJoin 组件集成到 JoinOperator，实现完整的 VSJoin 流式向量连接算法。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要将 VSJoin 组件集成到 JoinOperator。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase
- 方法名: camelBack
- 成员变量: lower_case_ 带尾部下划线
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
前面的任务实现了 VSJoin 的各个组件：
- TwoTierWindowState (A-01) - include/state/two_tier_window_state.h
- LSHPartitioner (A-02) - include/execution/vector_space_partitioner.h
- BoundaryTracker (A-03) - include/coordination/boundary_tracker.h
- LateArrivalHandler (A-04) - include/coordination/late_arrival_handler.h
- DistanceVerifier (A-05) - include/operator/distance_verifier.h
- PartitionedIndex (B-01) - include/index/partitioned_index.h
- PartitionedVectorState (B-02) - include/state/partitioned_vector_state.h
- PartitionCoordinator (B-03) - include/coordination/partition_coordinator.h
- AsyncCandidateGenerator (B-04) - include/operator/async_candidate_generator.h

现在需要将它们集成到 JoinOperator 中。

## 任务目标
扩展 JoinOperator，支持 VSJoin 模式，同时保持向后兼容。

## 修改文件
- include/operator/join_operator.h
- src/operator/join_operator.cpp
- 新增 test/IntegrationTest/test_vsjoin_integration.cpp

## 新增配置结构

```cpp
// 在 include/operator/join_operator.h 中添加
struct VSJoinConfig {
    bool enabled = false;                    ///< 是否启用 VSJoin 模式
    int num_partitions = 8;                  ///< 向量空间分区数
    size_t compact_threshold = 100;          ///< 双层窗口压缩阈值
    bool enable_boundary_tracking = true;    ///< 启用边界向量追踪
    int64_t allowed_lateness = 0;            ///< 允许的延迟（0=不处理延迟）
    int64_t watermark_delay = 1000;          ///< watermark 延迟
    size_t async_generator_threads = 4;      ///< 异步候选生成线程数
    size_t num_probes = 2;                   ///< 跨分区探测数
};
```

## 修改点清单

### 1. 添加 VSJoin 成员变量

```cpp
// JoinOperator.h private 部分添加
private:
    // VSJoin 配置
    VSJoinConfig vsjoin_config_;
    
    // VSJoin 组件
    std::shared_ptr<VectorSpacePartitioner> partitioner_;
    std::unique_ptr<PartitionedVectorState> left_vsjoin_state_;
    std::unique_ptr<PartitionedVectorState> right_vsjoin_state_;
    std::unique_ptr<PartitionedIndex> left_vsjoin_index_;
    std::unique_ptr<PartitionedIndex> right_vsjoin_index_;
    std::unique_ptr<PartitionCoordinator> coordinator_;
    std::unique_ptr<AsyncCandidateGenerator> left_async_generator_;
    std::unique_ptr<AsyncCandidateGenerator> right_async_generator_;
    std::shared_ptr<DistanceVerifier> verifier_;
```

### 2. 添加构造函数/配置方法

```cpp
public:
    /**
     * @brief 设置 VSJoin 配置
     */
    void setVSJoinConfig(const VSJoinConfig& config);
    
    /**
     * @brief 获取 VSJoin 配置
     */
    const VSJoinConfig& getVSJoinConfig() const { return vsjoin_config_; }
    
    /**
     * @brief 检查是否启用 VSJoin
     */
    bool isVSJoinEnabled() const { return vsjoin_config_.enabled; }
```

### 3. 修改 open()

```cpp
void JoinOperator::open(const RuntimeContext& context) {
    // 现有逻辑...
    
    if (vsjoin_config_.enabled) {
        initVSJoinComponents(context);
    }
}

void JoinOperator::initVSJoinComponents(const RuntimeContext& context) {
    SAGEFLOW_LOG_INFO("JoinOperator", "Initializing VSJoin components with {} partitions",
                      vsjoin_config_.num_partitions);
    
    // 1. 初始化分区器
    partitioner_ = std::make_shared<LSHPartitioner>(
        dimension_, /*num_hash_functions=*/8);
    
    // 2. 初始化分区向量状态
    left_vsjoin_state_ = std::make_unique<PartitionedVectorState>(
        vsjoin_config_.num_partitions,
        partitioner_,
        vsjoin_config_.compact_threshold,
        vsjoin_config_.enable_boundary_tracking);
    
    right_vsjoin_state_ = std::make_unique<PartitionedVectorState>(
        vsjoin_config_.num_partitions,
        partitioner_,
        vsjoin_config_.compact_threshold,
        vsjoin_config_.enable_boundary_tracking);
    
    // 3. 初始化分区索引
    left_vsjoin_index_ = std::make_unique<PartitionedIndex>(
        vsjoin_config_.num_partitions, dimension_, partitioner_);
    right_vsjoin_index_ = std::make_unique<PartitionedIndex>(
        vsjoin_config_.num_partitions, dimension_, partitioner_);
    
    // 4. 初始化协调器
    coordinator_ = std::make_unique<PartitionCoordinator>(
        vsjoin_config_.num_partitions,
        partitioner_,
        vsjoin_config_.allowed_lateness,
        vsjoin_config_.watermark_delay);
    
    // 5. 初始化异步候选生成器
    left_async_generator_ = std::make_unique<AsyncCandidateGenerator>(
        left_vsjoin_index_, vsjoin_config_.async_generator_threads);
    right_async_generator_ = std::make_unique<AsyncCandidateGenerator>(
        right_vsjoin_index_, vsjoin_config_.async_generator_threads);
    
    // 6. 初始化距离验证器
    verifier_ = std::make_shared<DistanceVerifier>(threshold_);
}
```

### 4. 修改 apply()

```cpp
void JoinOperator::apply(Response&& record, int slot, 
                         Collector& collector, 
                         const RuntimeContext& context) {
    if (vsjoin_config_.enabled) {
        applyVSJoin(std::move(record), slot, collector, context);
    } else {
        applyLegacy(std::move(record), slot, collector, context);
    }
}

void JoinOperator::applyVSJoin(Response&& record, int slot,
                                Collector& collector,
                                const RuntimeContext& context) {
    auto vec_record = extractVectorRecord(record);
    if (!vec_record) return;
    
    // 1. 处理延迟到达
    auto process_result = coordinator_->processRecord(*vec_record);
    
    if (process_result.status == ArrivalStatus::TOO_LATE) {
        SAGEFLOW_LOG_DEBUG("JoinOperator", "Dropping too late record uid={}",
                           vec_record->getUid());
        return;
    }
    
    if (process_result.status == ArrivalStatus::LATE) {
        coordinator_->bufferLateRecord(vec_record->clone());
        // 延迟记录仍然处理，但单独记录
    }
    
    // 2. 更新状态和索引
    auto record_ptr = vec_record.get();
    if (slot == 0) {
        left_vsjoin_state_->addRecord(std::move(vec_record), context.getSubtaskIndex());
        left_vsjoin_index_->insert(record_ptr->clone());
    } else {
        right_vsjoin_state_->addRecord(std::move(vec_record), context.getSubtaskIndex());
        right_vsjoin_index_->insert(record_ptr->clone());
    }
    
    // 3. 执行 join（eager 模式）
    if (is_eager_) {
        executeVSJoinEager(*record_ptr, slot, collector, context);
    }
}

void JoinOperator::executeVSJoinEager(const VectorRecord& query, int slot,
                                       Collector& collector,
                                       const RuntimeContext& context) {
    // 确定查询的目标侧
    auto& target_index = (slot == 0) ? right_vsjoin_index_ : left_vsjoin_index_;
    auto& target_state = (slot == 0) ? right_vsjoin_state_ : left_vsjoin_state_;
    
    // 获取候选分区
    auto candidate_partitions = coordinator_->routeQuery(query, vsjoin_config_.num_probes);
    
    // 跨分区查询
    auto candidates = target_index->queryMultiPartition(
        query, /*k=*/100, vsjoin_config_.num_probes);
    
    // 验证候选
    for (const auto& candidate : candidates) {
        auto result = verifier_->verify(query, *candidate);
        if (result.passed) {
            // 生成 join 结果
            auto join_result = function_->Join(query, *candidate);
            if (join_result) {
                collector.collect(Response(std::move(join_result)));
            }
        }
    }
}
```

### 5. 修改 close()

```cpp
void JoinOperator::close() {
    // 关闭 VSJoin 组件
    if (vsjoin_config_.enabled) {
        if (left_async_generator_) left_async_generator_->shutdown();
        if (right_async_generator_) right_async_generator_->shutdown();
    }
    
    // 现有的 close 逻辑...
}
```

### 6. 添加新的 join 方法类型

```cpp
// 在 parseMethodType() 或相关位置添加
if (method_name == "vsjoin_eager") {
    vsjoin_config_.enabled = true;
    is_eager_ = true;
    return JoinMethodType::VSJOIN_EAGER;
} else if (method_name == "vsjoin_lazy") {
    vsjoin_config_.enabled = true;
    is_eager_ = false;
    return JoinMethodType::VSJOIN_LAZY;
}
```

## 向后兼容要求

- 保留所有现有接口和行为
- 只有显式配置 vsjoin_config_.enabled = true 时才启用新模式
- 使用 "vsjoin_eager" 或 "vsjoin_lazy" 方法名时自动启用
- 现有测试应继续通过

## 测试要求

创建 test/IntegrationTest/test_vsjoin_integration.cpp:

```cpp
#include <gtest/gtest.h>
#include "operator/join_operator.h"
#include "stream/simple_stream_source.h"

class VSJoinIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建测试数据
    }
    
    std::vector<std::unique_ptr<VectorRecord>> createTestData(int count, int dimension);
};

// 基本功能测试
TEST_F(VSJoinIntegrationTest, BasicFunctionality) {
    // 测试 VSJoin 模式基本功能
}

TEST_F(VSJoinIntegrationTest, VSJoinConfigApplication) {
    // 测试配置正确应用
}

// 与 legacy 模式对比
TEST_F(VSJoinIntegrationTest, CompareWithLegacy) {
    // VSJoin 与现有模式结果对比（应该相同或更好）
}

TEST_F(VSJoinIntegrationTest, RecallComparison) {
    // 召回率对比
}

// 延迟到达处理
TEST_F(VSJoinIntegrationTest, LateArrivalHandling) {
    // 测试延迟到达处理
}

TEST_F(VSJoinIntegrationTest, TooLateDropped) {
    // 测试过期记录丢弃
}

// 跨分区 join
TEST_F(VSJoinIntegrationTest, CrossPartitionJoin) {
    // 测试跨分区 join 正确性
}

TEST_F(VSJoinIntegrationTest, BoundaryVectorJoin) {
    // 测试边界向量正确 join
}

// 可扩展性
TEST_F(VSJoinIntegrationTest, ScalabilityWithPartitions) {
    // 测试不同分区数下的可扩展性
}

TEST_F(VSJoinIntegrationTest, ScalabilityWithParallelism) {
    // 测试不同并行度下的可扩展性
}

// 边界条件
TEST_F(VSJoinIntegrationTest, EmptyStream) {
    // 测试空流
}

TEST_F(VSJoinIntegrationTest, SingleRecord) {
    // 测试单条记录
}

// 向后兼容
TEST_F(VSJoinIntegrationTest, LegacyModeUnchanged) {
    // 测试 legacy 模式行为不变
}
```

## 验收标准
1. 所有现有测试继续通过
2. VSJoin 模式功能正确
3. 与 legacy 模式结果一致性 > 99%
4. 性能不低于 legacy 模式
5. 代码通过 clang-tidy 检查
```

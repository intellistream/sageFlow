/**
 * @file test_pipeline_execution.cpp
 * @brief 集成测试：验证带 RuntimeContext 的算子在执行管道中的正确性
 * 
 * 本测试文件验证 Task 3 中改造后的各类算子是否能正确接收和使用 RuntimeContext
 * 测试内容包括：
 * 1. 各类算子带 RuntimeContext 的 apply() 方法的正确调用
 * 2. 多并行实例场景下 RuntimeContext 信息的正确传递
 * 3. 管道端到端执行的正确性
 */

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <cstring>

#include "utils/logger.h"
#include "execution/runtime_context.h"
#include "execution/collector.h"
#include "common/data_types.h"

// Operator headers
#include "operator/operator.h"
#include "operator/map_operator.h"
#include "operator/filter_operator.h"
#include "operator/window_operator.h"
#include "operator/aggregate_operator.h"
#include "operator/output_operator.h"
#include "operator/sink_operator.h"

// Function headers
#include "function/map_function.h"
#include "function/filter_function.h"
#include "function/window_function.h"
#include "function/aggregate_function.h"
#include "function/sink_function.h"

namespace sageFlow {
namespace test {

/**
 * @brief 测试用的简单 Collector 实现
 * 收集算子输出的数据，支持线程安全操作
 */
class TestCollector {
public:
    TestCollector() {
        collector_ = std::make_unique<Collector>(
            [this](std::unique_ptr<Response> resp, int slot) {
                std::lock_guard<std::mutex> lock(mutex_);
                collected_responses_.push_back(std::move(resp));
                collected_slots_.push_back(slot);
            }
        );
    }

    Collector& get() { return *collector_; }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return collected_responses_.size();
    }

    std::vector<std::unique_ptr<Response>> getResponses() {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::move(collected_responses_);
    }

    std::vector<int> getSlots() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return collected_slots_;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        collected_responses_.clear();
        collected_slots_.clear();
    }

private:
    std::unique_ptr<Collector> collector_;
    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<Response>> collected_responses_;
    std::vector<int> collected_slots_;
};

/**
 * @brief 创建测试用的 VectorRecord
 */
std::unique_ptr<VectorRecord> createTestRecord(uint64_t uid, int64_t timestamp, int dim = 64) {
    char* raw_data = new char[dim * sizeof(float)];
    float* float_data = reinterpret_cast<float*>(raw_data);
    for (int i = 0; i < dim; ++i) {
        float_data[i] = static_cast<float>(uid + i) / 100.0f;
    }
    return std::make_unique<VectorRecord>(uid, timestamp, dim, DataType::Float32, raw_data);
}

/**
 * @brief 从 VectorRecord 提取 float 向量
 */
std::vector<float> extractFloatVector(const VectorRecord& record) {
    std::vector<float> result;
    const float* data = reinterpret_cast<const float*>(record.data_.data_.get());
    result.assign(data, data + record.data_.dim_);
    return result;
}

// ============================================================================
// Pipeline Execution Test Fixture
// ============================================================================

class PipelineExecutionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 基本测试设置
    }

    void TearDown() override {
        // 清理
    }
};

// ============================================================================
// MapOperator Tests with RuntimeContext
// ============================================================================

TEST_F(PipelineExecutionTest, MapOperatorWithRuntimeContext) {
    // 创建 MapFunction - 将向量每个元素乘以2 (in-place 修改)
    auto map_func = std::make_unique<MapFunction>(
        "DoubleMap",
        [](std::unique_ptr<VectorRecord>& record) -> void {
            // In-place 修改记录中的向量数据
            float* data = reinterpret_cast<float*>(record->data_.data_.get());
            for (int i = 0; i < record->data_.dim_; ++i) {
                data[i] *= 2.0f;
            }
        }
    );

    std::unique_ptr<Function> func = std::move(map_func);
    MapOperator map_op(func);
    
    // 创建测试数据
    auto record = createTestRecord(1, 1000);
    Response input{ResponseType::Record, std::move(record)};
    
    // 创建 RuntimeContext
    RuntimeContext ctx(0, 4);  // 子任务0，总并行度4
    
    // 创建 Collector
    TestCollector test_collector;
    
    // 调用带 RuntimeContext 的 apply 方法
    map_op.apply(std::move(input), 0, test_collector.get(), ctx);
    
    // 验证输出
    ASSERT_EQ(test_collector.size(), 1);
    auto responses = test_collector.getResponses();
    ASSERT_EQ(responses[0]->type_, ResponseType::Record);
    ASSERT_NE(responses[0]->record_, nullptr);
    
    // 验证数据被正确映射 - 原始值 (1 + 0) / 100 = 0.01，乘以2后 = 0.02
    auto output_vec = extractFloatVector(*responses[0]->record_);
    EXPECT_FLOAT_EQ(output_vec[0], 0.02f);
}

// ============================================================================
// FilterOperator Tests with RuntimeContext
// ============================================================================

TEST_F(PipelineExecutionTest, FilterOperatorWithRuntimeContext) {
    // 创建 FilterFunction - 过滤 uid > 5 的记录
    auto filter_func = std::make_unique<FilterFunction>(
        "UidFilter",
        [](std::unique_ptr<VectorRecord>& record) -> bool {
            return record->uid_ > 5;
        }
    );

    std::unique_ptr<Function> func = std::move(filter_func);
    FilterOperator filter_op(func);
    
    // 创建 RuntimeContext
    RuntimeContext ctx(2, 8);  // 子任务2，总并行度8
    
    TestCollector test_collector;
    
    // 测试被过滤的记录 (uid = 3 <= 5，应该被过滤)
    auto record1 = createTestRecord(3, 1000);
    Response input1{ResponseType::Record, std::move(record1)};
    filter_op.apply(std::move(input1), 0, test_collector.get(), ctx);
    EXPECT_EQ(test_collector.size(), 0);  // 应该被过滤掉
    
    // 测试通过的记录 (uid = 10 > 5，应该通过)
    auto record2 = createTestRecord(10, 2000);
    Response input2{ResponseType::Record, std::move(record2)};
    filter_op.apply(std::move(input2), 0, test_collector.get(), ctx);
    EXPECT_EQ(test_collector.size(), 1);  // 应该通过
}

// ============================================================================
// WindowOperator Tests with RuntimeContext
// ============================================================================

TEST_F(PipelineExecutionTest, TumblingWindowOperatorWithRuntimeContext) {
    // 创建窗口大小为3的滚动窗口
    auto window_func = std::make_unique<WindowFunction>("TumblingWindow3", 3, 3, WindowType::Tumbling);
    std::unique_ptr<Function> func = std::move(window_func);
    TumblingWindowOperator window_op(func);
    
    // 创建 RuntimeContext
    RuntimeContext ctx(1, 4);
    
    TestCollector test_collector;
    
    // 发送3条记录以填满窗口
    for (int i = 1; i <= 3; ++i) {
        auto record = createTestRecord(i, i * 1000);
        Response input{ResponseType::Record, std::move(record)};
        window_op.apply(std::move(input), 0, test_collector.get(), ctx);
    }
    
    // 窗口大小为3，应该触发一次窗口输出
    ASSERT_EQ(test_collector.size(), 1);
    auto responses = test_collector.getResponses();
    ASSERT_EQ(responses[0]->type_, ResponseType::List);
    ASSERT_NE(responses[0]->records_, nullptr);
    EXPECT_EQ(responses[0]->records_->size(), 3);
}

TEST_F(PipelineExecutionTest, SlidingWindowOperatorWithRuntimeContext) {
    // 创建窗口大小为3，滑动步长为1的滑动窗口
    auto window_func = std::make_unique<WindowFunction>("SlidingWindow3_1", 3, 1, WindowType::Sliding);
    std::unique_ptr<Function> func = std::move(window_func);
    SlidingWindowOperator window_op(func);
    
    // 创建 RuntimeContext
    RuntimeContext ctx(0, 2);
    
    TestCollector test_collector;
    
    // 发送4条记录
    for (int i = 1; i <= 4; ++i) {
        auto record = createTestRecord(i, i * 1000);
        Response input{ResponseType::Record, std::move(record)};
        window_op.apply(std::move(input), 0, test_collector.get(), ctx);
    }
    
    // 滑动窗口在窗口满时每次滑动都输出
    // 第3条记录时窗口满，第4条记录时滑动输出
    EXPECT_GE(test_collector.size(), 1);
}

// ============================================================================
// AggregateOperator Tests with RuntimeContext
// ============================================================================

TEST_F(PipelineExecutionTest, AggregateOperatorWithRuntimeContext) {
    // 创建聚合函数 (AVG)
    auto agg_func = std::make_unique<AggregateFunction>("AvgAgg", AggregateType::Avg);
    std::unique_ptr<Function> func = std::move(agg_func);
    AggregateOperator agg_op(func);
    
    // 创建 RuntimeContext
    RuntimeContext ctx(3, 4);
    
    TestCollector test_collector;
    
    // 创建一个 List 类型的输入（模拟窗口输出）
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    for (int i = 1; i <= 3; ++i) {
        records->push_back(createTestRecord(i, i * 1000, 4));  // 使用小维度便于验证
    }
    Response input{ResponseType::List, std::move(records)};
    
    // 调用带 RuntimeContext 的 apply 方法
    agg_op.apply(std::move(input), 0, test_collector.get(), ctx);
    
    // 验证输出
    ASSERT_EQ(test_collector.size(), 1);
    auto responses = test_collector.getResponses();
    ASSERT_EQ(responses[0]->type_, ResponseType::Record);
}

// ============================================================================
// SinkOperator Tests with RuntimeContext
// ============================================================================

TEST_F(PipelineExecutionTest, SinkOperatorWithRuntimeContext) {
    std::atomic<int> sink_count{0};
    std::vector<uint64_t> sunk_uids;
    std::mutex uids_mutex;
    
    // 创建 SinkFunction
    auto sink_func = std::make_unique<SinkFunction>(
        "CountingSink",
        [&sink_count, &sunk_uids, &uids_mutex](std::unique_ptr<VectorRecord>& record) {
            sink_count++;
            std::lock_guard<std::mutex> lock(uids_mutex);
            sunk_uids.push_back(record->uid_);
        }
    );
    
    std::unique_ptr<Function> func = std::move(sink_func);
    SinkOperator sink_op(func);
    
    // 创建 RuntimeContext
    RuntimeContext ctx(0, 1);
    
    TestCollector test_collector;
    
    // 发送多条记录
    for (int i = 1; i <= 5; ++i) {
        auto record = createTestRecord(i, i * 1000);
        Response input{ResponseType::Record, std::move(record)};
        sink_op.apply(std::move(input), 0, test_collector.get(), ctx);
    }
    
    // 验证 sink 被正确调用
    EXPECT_EQ(sink_count.load(), 5);
    EXPECT_EQ(sunk_uids.size(), 5);
    
    // Sink 不应该向下游发送数据
    EXPECT_EQ(test_collector.size(), 0);
}

// ============================================================================
// Multi-Parallel Instance Tests
// ============================================================================

TEST_F(PipelineExecutionTest, MultipleParallelInstancesWithRuntimeContext) {
    // 测试多个并行实例，每个实例使用不同的 RuntimeContext
    const size_t parallelism = 4;
    
    // 创建一个 MapFunction (identity map - 不修改数据)
    auto create_map_func = []() {
        return std::make_unique<MapFunction>(
            "IdentityMap",
            [](std::unique_ptr<VectorRecord>& record) -> void {
                // Identity map - 不做任何修改
            }
        );
    };
    
    // 为每个并行实例创建独立的算子和收集器
    std::vector<std::unique_ptr<MapOperator>> operators;
    std::vector<std::unique_ptr<TestCollector>> collectors;
    
    for (size_t i = 0; i < parallelism; ++i) {
        auto map_func = create_map_func();
        std::unique_ptr<Function> func = std::move(map_func);
        operators.push_back(std::make_unique<MapOperator>(func));
        collectors.push_back(std::make_unique<TestCollector>());
    }
    
    // 每个实例处理不同的数据
    std::vector<std::thread> threads;
    for (size_t i = 0; i < parallelism; ++i) {
        threads.emplace_back([i, &operators, &collectors, parallelism]() {
            RuntimeContext ctx(i, parallelism);
            
            // 每个实例处理10条记录
            for (int j = 0; j < 10; ++j) {
                auto record = createTestRecord(i * 100 + j, j * 1000);
                Response input{ResponseType::Record, std::move(record)};
                operators[i]->apply(std::move(input), 0, collectors[i]->get(), ctx);
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    // 验证每个实例都处理了正确数量的记录
    for (size_t i = 0; i < parallelism; ++i) {
        EXPECT_EQ(collectors[i]->size(), 10) 
            << "Instance " << i << " should have processed 10 records";
    }
}

// ============================================================================
// Pipeline Chain Tests
// ============================================================================

TEST_F(PipelineExecutionTest, OperatorChainWithRuntimeContext) {
    // 测试算子链：Filter -> Map -> Sink
    // 由于 uid 是 const，我们使用 Filter 基于 uid 过滤，Map 修改向量数据
    RuntimeContext ctx(0, 1);
    
    // 创建 FilterOperator - 只保留 uid >= 5 的记录
    auto filter_func = std::make_unique<FilterFunction>(
        "UidFilter5",
        [](std::unique_ptr<VectorRecord>& record) -> bool {
            return record->uid_ >= 5;
        }
    );
    std::unique_ptr<Function> filter_f = std::move(filter_func);
    FilterOperator filter_op(filter_f);
    
    // 创建 MapOperator - 将向量数据乘以2 (in-place)
    auto map_func = std::make_unique<MapFunction>(
        "DoubleDataMap",
        [](std::unique_ptr<VectorRecord>& record) -> void {
            float* data = reinterpret_cast<float*>(record->data_.data_.get());
            for (int i = 0; i < record->data_.dim_; ++i) {
                data[i] *= 2.0f;
            }
        }
    );
    std::unique_ptr<Function> map_f = std::move(map_func);
    MapOperator map_op(map_f);
    
    // 创建 SinkOperator
    std::vector<uint64_t> final_uids;
    std::mutex uids_mutex;
    auto sink_func = std::make_unique<SinkFunction>(
        "CollectSink",
        [&final_uids, &uids_mutex](std::unique_ptr<VectorRecord>& record) {
            std::lock_guard<std::mutex> lock(uids_mutex);
            final_uids.push_back(record->uid_);
        }
    );
    std::unique_ptr<Function> sink_f = std::move(sink_func);
    SinkOperator sink_op(sink_f);
    
    // 创建中间收集器
    TestCollector filter_collector;
    TestCollector map_collector;
    TestCollector sink_collector;
    
    // 处理 uid = 1 到 10 的记录
    for (int i = 1; i <= 10; ++i) {
        auto record = createTestRecord(i, i * 1000);
        Response input{ResponseType::Record, std::move(record)};
        
        // Filter 阶段：过滤掉 uid < 5 的记录
        filter_op.apply(std::move(input), 0, filter_collector.get(), ctx);
    }
    
    // 传递 Filter 输出到 Map
    auto filter_responses = filter_collector.getResponses();
    for (auto& resp : filter_responses) {
        map_op.apply(std::move(*resp), 0, map_collector.get(), ctx);
    }
    
    // 传递 Map 输出到 Sink
    auto map_responses = map_collector.getResponses();
    for (auto& resp : map_responses) {
        sink_op.apply(std::move(*resp), 0, sink_collector.get(), ctx);
    }
    
    // 验证结果
    // uid 1-10 经过 Filter >= 5 保留 5, 6, 7, 8, 9, 10
    EXPECT_EQ(final_uids.size(), 6);
    
    // 验证正确的 uid 被收集
    std::sort(final_uids.begin(), final_uids.end());
    std::vector<uint64_t> expected = {5, 6, 7, 8, 9, 10};
    EXPECT_EQ(final_uids, expected);
}

// ============================================================================
// Backward Compatibility Tests
// ============================================================================

TEST_F(PipelineExecutionTest, BackwardCompatibilityWithoutRuntimeContext) {
    // 测试旧的 apply() 方法（不带 RuntimeContext）仍然工作
    auto map_func = std::make_unique<MapFunction>(
        "IdentityMap",
        [](std::unique_ptr<VectorRecord>& record) -> void {
            // Identity map - 不做任何修改
        }
    );
    
    std::unique_ptr<Function> func = std::move(map_func);
    MapOperator map_op(func);
    
    TestCollector test_collector;
    
    auto record = createTestRecord(1, 1000);
    Response input{ResponseType::Record, std::move(record)};
    
    // 调用旧的 apply 方法（不带 RuntimeContext）
    map_op.apply(std::move(input), 0, test_collector.get());
    
    // 验证输出
    ASSERT_EQ(test_collector.size(), 1);
}

// ============================================================================
// RuntimeContext Information Tests
// ============================================================================

TEST_F(PipelineExecutionTest, RuntimeContextInformationAccuracy) {
    // 测试 RuntimeContext 信息的准确性
    std::vector<std::pair<size_t, size_t>> contexts = {
        {0, 1}, {0, 4}, {3, 4}, {7, 8}, {15, 16}
    };
    
    for (const auto& [index, parallelism] : contexts) {
        RuntimeContext ctx(index, parallelism);
        
        EXPECT_EQ(ctx.getSubtaskIndex(), index);
        EXPECT_EQ(ctx.getParallelism(), parallelism);
        
        std::string expected_name = "Task[" + std::to_string(index) + "/" + 
                                    std::to_string(parallelism) + "]";
        EXPECT_EQ(ctx.getTaskName(), expected_name);
    }
}

} // namespace test
} // namespace sageFlow

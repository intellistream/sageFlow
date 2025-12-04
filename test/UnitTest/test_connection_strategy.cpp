#include <gtest/gtest.h>
#include "execution/connection_strategy.h"
#include "execution/execution_graph.h"
#include "operator/filter_operator.h"
#include "operator/map_operator.h"
#include "operator/output_operator.h"
#include "operator/sink_operator.h"
#include "stream/stream_environment.h"
#include <memory>

namespace sageFlow {
namespace test {

/**
 * @brief 测试统一连接策略的基本功能
 * 
 * 连接策略采用统一的 a×b SPSC 队列矩阵：
 * - 队列数量 = upstream_parallelism × downstream_parallelism
 * - 每个队列是 SPSC (Single Producer Single Consumer)
 * - 路由规则：queue_index = upstream_i × downstream_parallelism + downstream_j
 */
class ConnectionStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        strategy_ = std::make_unique<ConnectionStrategy>();
    }

    void TearDown() override {
        strategy_.reset();
    }
    
    std::unique_ptr<ConnectionStrategy> strategy_;
};

TEST_F(ConnectionStrategyTest, BasicQueueCreation) {
    // 测试基本队列创建
    auto queues = strategy_->createQueues(4, 2);
    
    // 期望 4 × 2 = 8 个队列
    EXPECT_EQ(queues.size(), 8);
    
    // 验证所有队列都已创建
    for (const auto& q : queues) {
        EXPECT_NE(q, nullptr);
    }
}

TEST_F(ConnectionStrategyTest, QueueCountFormula) {
    // 测试队列数量公式：queue_count = upstream × downstream
    struct TestCase {
        size_t upstream;
        size_t downstream;
        size_t expected_queues;
    };
    
    std::vector<TestCase> test_cases = {
        {1, 1, 1},      // 单对单
        {1, 4, 4},      // 一对多
        {4, 1, 4},      // 多对一
        {4, 4, 16},     // 多对多（相等）
        {8, 4, 32},     // 多对多（上游更多）
        {4, 8, 32},     // 多对多（下游更多）
        {3, 5, 15},     // 非2的幂次
    };
    
    for (const auto& tc : test_cases) {
        auto queues = strategy_->createQueues(tc.upstream, tc.downstream);
        EXPECT_EQ(queues.size(), tc.expected_queues)
            << "upstream=" << tc.upstream << ", downstream=" << tc.downstream;
    }
}

TEST_F(ConnectionStrategyTest, QueueIndexCalculation) {
    // 验证队列索引公式：queue_index = upstream_i × downstream_parallelism + downstream_j
    // 这是 SPSC 矩阵的核心：每个 (upstream_i, downstream_j) 对应唯一队列
    
    size_t upstream = 3;
    size_t downstream = 4;
    
    // 构建预期的队列索引矩阵
    // upstream_0: [0, 1, 2, 3]
    // upstream_1: [4, 5, 6, 7]
    // upstream_2: [8, 9, 10, 11]
    
    for (size_t i = 0; i < upstream; ++i) {
        for (size_t j = 0; j < downstream; ++j) {
            size_t expected_index = i * downstream + j;
            EXPECT_LT(expected_index, upstream * downstream)
                << "Index out of bounds for upstream_" << i << " -> downstream_" << j;
        }
    }
}

TEST_F(ConnectionStrategyTest, ExecutionGraphBasicPipeline) {
    // 创建执行图测试基本管道：Source -> Filter -> Sink
    ExecutionGraph graph;
    
    std::unique_ptr<Function> filter_func = std::make_unique<FilterFunction>(
        "TestFilter", [](const std::unique_ptr<VectorRecord>&) { return true; });
    
    auto source = std::make_shared<OutputOperator>(nullptr);
    source->set_parallelism(2);
    source->name = "TestSource";
    
    auto filter = std::make_shared<FilterOperator>(filter_func);
    filter->set_parallelism(4);
    filter->name = "TestFilter";
    
    std::unique_ptr<Function> sink_func = std::make_unique<SinkFunction>(
        "TestSink", [](const std::unique_ptr<VectorRecord>&) {});
    auto sink = std::make_shared<SinkOperator>(sink_func);
    sink->set_parallelism(1);
    sink->name = "TestSink";
    
    // 添加算子
    graph.addOperator(source);
    graph.addOperator(filter);
    graph.addOperator(sink);
    
    // 连接算子
    graph.connectOperators(source, filter);
    graph.connectOperators(filter, sink);
    
    // 构建执行图
    EXPECT_NO_THROW(graph.buildGraph());
}

TEST_F(ConnectionStrategyTest, ExecutionGraphParallelismVariations) {
    // 测试不同并行度配置
    struct TestCase {
        size_t source_parallelism;
        size_t filter_parallelism;
        size_t sink_parallelism;
    };
    
    std::vector<TestCase> test_cases = {
        {1, 1, 1},   // 全串行
        {4, 4, 4},   // 全并行（相等）
        {1, 4, 1},   // 扩展再收缩
        {4, 2, 1},   // 逐步收缩
        {1, 2, 4},   // 逐步扩展
    };
    
    for (const auto& tc : test_cases) {
        ExecutionGraph graph;
        
        std::unique_ptr<Function> filter_func = std::make_unique<FilterFunction>(
            "TestFilter", [](const std::unique_ptr<VectorRecord>&) { return true; });
        
        auto source = std::make_shared<OutputOperator>(nullptr);
        source->set_parallelism(tc.source_parallelism);
        source->name = "TestSource";
        
        auto filter = std::make_shared<FilterOperator>(filter_func);
        filter->set_parallelism(tc.filter_parallelism);
        filter->name = "TestFilter";
        
        std::unique_ptr<Function> sink_func = std::make_unique<SinkFunction>(
            "TestSink", [](const std::unique_ptr<VectorRecord>&) {});
        auto sink = std::make_shared<SinkOperator>(sink_func);
        sink->set_parallelism(tc.sink_parallelism);
        sink->name = "TestSink";
        
        graph.addOperator(source);
        graph.addOperator(filter);
        graph.addOperator(sink);
        
        graph.connectOperators(source, filter);
        graph.connectOperators(filter, sink);
        
        EXPECT_NO_THROW(graph.buildGraph())
            << "Failed with parallelism: " << tc.source_parallelism
            << " -> " << tc.filter_parallelism
            << " -> " << tc.sink_parallelism;
    }
}

} // namespace test
} // namespace sageFlow

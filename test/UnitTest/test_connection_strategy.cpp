#include <gtest/gtest.h>
#include "execution/connection_strategy.h"
#include "execution/partitioned_connection_strategy.h"
#include "execution/shared_queue_connection_strategy.h"
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
 * @brief 测试连接策略的基本功能
 * 验证分区策略和共享队列策略都能正确创建队列并配置连接
 */
class ConnectionStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 测试基础设施
    }

    void TearDown() override {
        // 清理
    }
};

TEST_F(ConnectionStrategyTest, PartitionedStrategyBasics) {
    auto strategy = std::make_unique<PartitionedConnectionStrategy>();
    
    // 验证策略类型
    EXPECT_EQ(strategy->getType(), ConnectionType::PARTITIONED);
    
    // 测试队列创建
    auto queues = strategy->createQueues(4, 2, false);
    // 分区策略：队列数量应等于上游并行度
    EXPECT_EQ(queues.size(), 4);
    
    // 验证队列不为空
    for (const auto& q : queues) {
        EXPECT_NE(q, nullptr);
    }
}

TEST_F(ConnectionStrategyTest, SharedQueueStrategyBasics) {
    auto strategy = std::make_unique<SharedQueueConnectionStrategy>();
    
    // 验证策略类型
    EXPECT_EQ(strategy->getType(), ConnectionType::SHARED_QUEUE);
    
    // 测试队列创建
    auto queues = strategy->createQueues(4, 2, false);
    // 共享队列策略：队列数量应等于下游并行度
    EXPECT_EQ(queues.size(), 2);
    
    // 验证队列不为空
    for (const auto& q : queues) {
        EXPECT_NE(q, nullptr);
    }
}

TEST_F(ConnectionStrategyTest, DifferentParallelismConfigurations) {
    auto partitioned = std::make_unique<PartitionedConnectionStrategy>();
    auto shared = std::make_unique<SharedQueueConnectionStrategy>();
    
    // 测试不同的上下游并行度配置
    struct TestCase {
        size_t upstream_parallelism;
        size_t downstream_parallelism;
        size_t expected_partitioned_queues;
        size_t expected_shared_queues;
    };
    
    std::vector<TestCase> test_cases = {
        {1, 1, 1, 1},    // 单对单
        {1, 4, 1, 4},    // 一对多
        {4, 1, 4, 1},    // 多对一
        {4, 4, 4, 4},    // 多对多（相等）
        {8, 4, 8, 4},    // 多对多（上游更多）
        {4, 8, 4, 8},    // 多对多（下游更多）
    };
    
    for (const auto& tc : test_cases) {
        auto p_queues = partitioned->createQueues(
            tc.upstream_parallelism, tc.downstream_parallelism, false);
        EXPECT_EQ(p_queues.size(), tc.expected_partitioned_queues)
            << "Partitioned: upstream=" << tc.upstream_parallelism
            << ", downstream=" << tc.downstream_parallelism;
        
        auto s_queues = shared->createQueues(
            tc.upstream_parallelism, tc.downstream_parallelism, false);
        EXPECT_EQ(s_queues.size(), tc.expected_shared_queues)
            << "Shared: upstream=" << tc.upstream_parallelism
            << ", downstream=" << tc.downstream_parallelism;
    }
}

TEST_F(ConnectionStrategyTest, ExecutionGraphWithPartitionedStrategy) {
    // 创建执行图并使用默认的分区策略
    ExecutionGraph graph;
    
    // 创建简单的算子链：Source -> Filter -> Sink
    std::unique_ptr<Function> filter_func = std::make_unique<FilterFunction>(
        "TestFilter", [](const std::unique_ptr<VectorRecord>&) { return true; });
    
    auto source = std::make_shared<OutputOperator>(nullptr);
    source->set_parallelism(2);
    source->name = "TestSource";
    
    auto filter = std::make_shared<FilterOperator>(filter_func);
    filter->set_parallelism(2);
    filter->name = "TestFilter";
    
    std::unique_ptr<Function> sink_func = std::make_unique<SinkFunction>(
        "TestSink", [](const std::unique_ptr<VectorRecord>&) {});
    auto sink = std::make_shared<SinkOperator>(sink_func);
    sink->set_parallelism(1);
    sink->name = "TestSink";
    
    // 添加算子（默认使用分区策略）
    graph.addOperator(source);
    graph.addOperator(filter);
    graph.addOperator(sink);
    
    // 连接算子
    graph.connectOperators(source, filter);
    graph.connectOperators(filter, sink);
    
    // 构建执行图
    EXPECT_NO_THROW(graph.buildGraph());
}

TEST_F(ConnectionStrategyTest, ExecutionGraphWithSharedQueueStrategy) {
    // 创建执行图并使用共享队列策略
    ExecutionGraph graph;
    
    // 创建简单的算子链：Source -> Filter -> Sink
    std::unique_ptr<Function> filter_func = std::make_unique<FilterFunction>(
        "TestFilter", [](const std::unique_ptr<VectorRecord>&) { return true; });
    
    auto source = std::make_shared<OutputOperator>(nullptr);
    source->set_parallelism(2);
    source->name = "TestSource";
    
    auto filter = std::make_shared<FilterOperator>(filter_func);
    filter->set_parallelism(2);
    filter->name = "TestFilter";
    
    std::unique_ptr<Function> sink_func = std::make_unique<SinkFunction>(
        "TestSink", [](const std::unique_ptr<VectorRecord>&) {});
    auto sink = std::make_shared<SinkOperator>(sink_func);
    sink->set_parallelism(1);
    sink->name = "TestSink";
    
    // 添加算子，为filter指定共享队列策略
    graph.addOperator(source);
    graph.addOperator(filter, ConnectionType::SHARED_QUEUE);
    graph.addOperator(sink);
    
    // 连接算子
    graph.connectOperators(source, filter);
    graph.connectOperators(filter, sink);
    
    // 构建执行图
    EXPECT_NO_THROW(graph.buildGraph());
}

} // namespace test
} // namespace sageFlow

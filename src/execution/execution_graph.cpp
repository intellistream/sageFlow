//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/execution_graph.h"
#include "execution/connection_strategy.h"
#include <iostream>
#include <algorithm>
#include "utils/logger.h"

namespace sageFlow {

ExecutionGraph::~ExecutionGraph() {
    stop();
    join();
}

void ExecutionGraph::addOperator(std::shared_ptr<Operator> op) {
    if (!op) return;

    operators_.push_back(op);

    OperatorInfo info;
    info.op = op;
    info.parallelism = op->get_parallelism();
    operator_infos_[op] = std::move(info);
}

void ExecutionGraph::connectOperators(std::shared_ptr<Operator> upstream,
                                     std::shared_ptr<Operator> downstream,
                                     int slot) {
    if (!upstream || !downstream) return;

    // 存储连接关系
    connections_.emplace_back(upstream, downstream, slot);
}

void ExecutionGraph::buildGraph() {
    // 1. 为每个算子创建执行顶点
    for (auto& op : operators_) {
        createVerticesForOperator(op);
    }

    // 2. 创建上下游之间的队列连接
    createConnections();
}

void ExecutionGraph::createVerticesForOperator(std::shared_ptr<Operator> op) {
    auto it = operator_infos_.find(op);
    if (it == operator_infos_.end()) return;

    OperatorInfo& info = it->second;
    info.vertices.clear();

    // 为每个并行度创建一个ExecutionVertex
    for (size_t i = 0; i < info.parallelism; ++i) {
        auto vertex = std::make_unique<ExecutionVertex>(op, i);
        info.vertices.push_back(std::move(vertex));
    }
}

void ExecutionGraph::createConnections() {
    for (const auto& [upstream_op, downstream_op, slot] : connections_) {
        auto upstream_it = operator_infos_.find(upstream_op);
        auto downstream_it = operator_infos_.find(downstream_op);

        if (upstream_it == operator_infos_.end() || downstream_it == operator_infos_.end()) {
            continue;
        }

        OperatorInfo& upstream_info = upstream_it->second;
        OperatorInfo& downstream_info = downstream_it->second;

        // 使用统一的连接策略创建队列
        auto queues = connection_strategy_.createQueues(
            upstream_info.parallelism,
            downstream_info.parallelism);
        
        // 记录到 all_queues_
        for (auto &q : queues) {
            all_queues_.push_back(q);
        }

        // 为上游的每个ExecutionVertex配置ResultPartition
        // 从下游算子获取期望的分区器（如果有的话）
        for (size_t i = 0; i < upstream_info.vertices.size(); ++i) {
            auto& upstream_vertex = upstream_info.vertices[i];
            auto result_partition = upstream_vertex->getResultPartition();

            // 从下游算子获取期望的分区器
            auto partitioner = downstream_op->getPreferredPartitioner(
                0,  // dimension - 可以从配置获取
                static_cast<int>(downstream_info.parallelism));

            connection_strategy_.setupResultPartition(
                result_partition,
                queues,
                i,
                upstream_info.parallelism,
                downstream_info.parallelism,
                slot,
                std::move(partitioner));
        }

        // 为下游的每个ExecutionVertex配置InputGate
        for (size_t i = 0; i < downstream_info.vertices.size(); ++i) {
            auto& downstream_vertex = downstream_info.vertices[i];
            auto input_gate = downstream_vertex->getInputGate();

            bool is_first_setup = (input_gate->size() == 0);
            connection_strategy_.setupInputGate(
                input_gate,
                queues,
                i,
                upstream_info.parallelism,
                downstream_info.parallelism,
                is_first_setup);
        }
    }
}

void ExecutionGraph::start() {
    SAGEFLOW_LOG_INFO("GRAPH", "Starting ExecutionGraph operators={} ", operators_.size());

    // 启动所有ExecutionVertex
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->start();
        }
    }

    SAGEFLOW_LOG_INFO("GRAPH", "All ExecutionVertices started");
}

void ExecutionGraph::stop() {
    SAGEFLOW_LOG_INFO("GRAPH", "Stopping ExecutionGraph...");
    // 先尝试按拓扑顺序：优先停止 Source(OutputOperator) 以停止生产；
    // 再停止非 Source 以允许其排干剩余数据（ExecutionVertex 内部已有 drain 逻辑）。
    std::vector<std::shared_ptr<Operator>> sources;
    std::vector<std::shared_ptr<Operator>> others;
    for (auto &op : operators_) {
        if (op->getType() == OperatorType::OUTPUT) sources.push_back(op); else others.push_back(op);
    }
    auto stop_group = [this](const std::vector<std::shared_ptr<Operator>>& group){
        for (auto &op : group) {
            auto it = operator_infos_.find(op);
            if (it == operator_infos_.end()) continue;
            for (auto &vertex : it->second.vertices) {
                vertex->stop();
            }
        }
    };
    // 停止 source，阻断新数据
    stop_group(sources);
    // 稍作等待给下游消费
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    // 再停止其他
    stop_group(others);
    // 停止所有队列，唤醒阻塞的消费者/生产者
    for (auto &q : all_queues_) {
        if (q) q->stop();
    }
    SAGEFLOW_LOG_INFO("GRAPH", "All ExecutionVertices stopped");
}

void ExecutionGraph::join() {
    // 等待所有ExecutionVertex完成
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->join();
        }
    }

    SAGEFLOW_LOG_INFO("GRAPH", "All ExecutionVertices finished");
}

} // namespace sageFlow

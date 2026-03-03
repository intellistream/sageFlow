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
            // 每个 upstream vertex 需要独立的分区器实例（因为分区器可能有内部状态如轮询计数器）
            auto partitioner = downstream_op->getPreferredPartitioner(
                0,  // dimension - 从下游算子配置获取
                static_cast<int>(downstream_info.parallelism));

            // 日志记录分区器类型，便于调试
            if (i == 0) {  // 只记录一次
                if (partitioner) {
                    SAGEFLOW_LOG_INFO("GRAPH", "Using {} partitioner for connection {} -> {} (slot={})",
                                     partitioner->supportsMulticast() ? "multicast-capable" : "unicast",
                                     upstream_op->name, downstream_op->name, slot);
                } else {
                    SAGEFLOW_LOG_DEBUG("GRAPH", "Using default RoundRobin partitioner for connection {} -> {} (slot={})",
                                      upstream_op->name, downstream_op->name, slot);
                }
            }

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
    // 统一停止所有 vertex，并唤醒其输入队列上可能阻塞的线程
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->stopAndWake();
        }
    }
    // 再次停止所有队列，确保无遗漏（例如 ResultPartition 的队列）
    for (auto &q : all_queues_) {
        if (q) q->stop();
    }
    SAGEFLOW_LOG_INFO("GRAPH", "All ExecutionVertices stopped");
}

void ExecutionGraph::join() {
    // 等待所有ExecutionVertex完成
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            SAGEFLOW_LOG_INFO("GRAPH_JOIN", "Joining vertex op={} idx={}",
                             op ? op->name : "<null>",
                             vertex ? vertex->getSubtaskIndex() : 0);
            vertex->join();
            SAGEFLOW_LOG_INFO("GRAPH_JOIN", "Joined vertex op={} idx={}",
                             op ? op->name : "<null>",
                             vertex ? vertex->getSubtaskIndex() : 0);
        }
    }

    SAGEFLOW_LOG_INFO("GRAPH", "All ExecutionVertices finished");
}

} // namespace sageFlow

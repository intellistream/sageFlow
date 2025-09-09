//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/execution_graph.h"
#include <iostream>
#include <algorithm>
#include "utils/logger.h"

namespace candy {

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

auto ExecutionGraph::createQueues(
  size_t upstream_parallelism,
  size_t downstream_parallelism,
  bool is_join_operator) -> std::vector<QueuePtr> {
    std::vector<QueuePtr> queues;

    // 计算需要创建的队列数量
    size_t queue_count = upstream_parallelism;

    // 队列容量配置
    const size_t ring_buffer_capacity = 1024;
    const size_t blocking_queue_capacity = 512;

    for (size_t i = 0; i < queue_count; ++i) {
        QueuePtr queue;

        if (is_join_operator) {
            // Join算子需要接受多个上游，使用阻塞队列 (若有其他接受多个上游的算子则需要在此添加）
            queue = std::make_shared<BlockingQueue>(blocking_queue_capacity);
        } else {
            // 其他情况使用环形缓冲队列
            queue = std::make_shared<RingBufferQueue>(ring_buffer_capacity);
        }

        queues.emplace_back(std::move(queue));
    }

    return queues;
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

        // 判断下游是否为Join算子
        bool is_join_operator = (downstream_op->getType() == OperatorType::JOIN);

    // // 连接概览日志
    // std::cout << "Connecting " << upstream_op->name
    //       << " -> " << downstream_op->name
    //       << ", slot=" << slot
    //       << ", upstream_parallelism=" << upstream_info.parallelism
    //       << ", downstream_parallelism=" << downstream_info.parallelism
    //       << (is_join_operator ? " (JOIN)" : "")
    //       << "\n";

        // 创建队列
        auto queues = createQueues(upstream_info.parallelism,
                               downstream_info.parallelism,
                                                  is_join_operator);
        // 记录到 all_queues_
        for (auto &q : queues) {
            all_queues_.push_back(q);
        }

        // 为上游的每个ExecutionVertex配置ResultPartition
        for (size_t i = 0; i < upstream_info.vertices.size(); ++i) {
            auto& upstream_vertex = upstream_info.vertices[i];
            auto result_partition = upstream_vertex->getResultPartition();

            // 创建分区器
            std::unique_ptr<IPartitioner> partitioner = std::make_unique<RoundRobinPartitioner>();

            // 设置输出通道
            std::vector<QueuePtr> output_channels;
            if (downstream_info.parallelism == 1) {
                // 下游只有一个并行度，所有上游都连接到同一个队列
                output_channels.push_back(queues[i % queues.size()]);
            } else {
                // 下游有多个并行度，需要分发到多个队列
                for (size_t j = 0; j < downstream_info.parallelism; ++j) {
                    output_channels.push_back(queues[j % queues.size()]);
                }
            }

            result_partition->setup(std::move(partitioner), std::move(output_channels), slot);

            // // 每个上游子任务的输出通道统计（针对当前 slot）
            // std::cout << "  " << upstream_op->name << "[" << i << "] -> slot=" << slot
            //           << ", channels-for-slot=" << upstream_vertex->getResultPartition()->get_channel_count(slot)
            //           << ", total-slots=" << upstream_vertex->getResultPartition()->get_slot_size()
            //           << "\n";
        }

        // 为下游的每个ExecutionVertex配置InputGate（注意：同一下游顶点可能在多条连接中被多次配置）
        for (size_t i = 0; i < downstream_info.vertices.size(); ++i) {
            auto& downstream_vertex = downstream_info.vertices[i];
            auto input_gate = downstream_vertex->getInputGate();

            // 配置输入队列
            std::vector<QueuePtr> input_queues;
            if (upstream_info.parallelism == 1) {
                // 上游只有一个并行度
                input_queues.push_back(queues[0]);
            } else {
                // 上游有多个并行度，当前下游顶点需要接收来自多个上游的数据
                for (size_t j = 0; j < upstream_info.parallelism; ++j) {
                    input_queues.push_back(queues[j]);
                }
            }

            // 如果是首次配置，setup；否则追加
            if (input_gate->size() == 0) {
                input_gate->setup(std::move(input_queues));
            } else {
                input_gate->addQueues(std::move(input_queues));
            }
            //
            // // 每个下游子任务的输入队列统计
            // std::cout << "  " << downstream_op->name << "[" << i << "] <- slot=" << slot
            //           << ", input-queues=" << downstream_vertex->getInputGate()->size() << "\n";
        }
    }
}

void ExecutionGraph::start() {
    CANDY_LOG_INFO("GRAPH", "Starting ExecutionGraph operators={} ", operators_.size());

    // 启动所有ExecutionVertex
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->start();
        }
    }

    CANDY_LOG_INFO("GRAPH", "All ExecutionVertices started");
}

void ExecutionGraph::stop() {
    CANDY_LOG_INFO("GRAPH", "Stopping ExecutionGraph...");
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
    // 停止所有队列，唤醒阻塞的消费者/生产者（BlockingQueue 有效，RingBufferQueue 为 no-op）
    for (auto &q : all_queues_) {
        if (q) q->stop();
    }
    CANDY_LOG_INFO("GRAPH", "All ExecutionVertices stopped");
}

void ExecutionGraph::join() {
    // 等待所有ExecutionVertex完成
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->join();
        }
    }

    CANDY_LOG_INFO("GRAPH", "All ExecutionVertices finished");
}

} // namespace candy

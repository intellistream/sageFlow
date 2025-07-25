//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/execution_graph.h"
#include <iostream>
#include <algorithm>

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

    // 同时在原有的Operator结构中维护连接关系
    upstream->addChild(downstream, slot);
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

std::vector<QueuePtr> ExecutionGraph::createQueues(size_t upstream_parallelism,
                                                   size_t downstream_parallelism,
                                                   bool is_join_operator) {
    std::vector<QueuePtr> queues;

    // 计算需要创建的队列数量
    size_t queue_count = upstream_parallelism;

    // 队列容量配置
    const size_t ring_buffer_capacity = 1024;
    const size_t blocking_queue_capacity = 512;

    for (size_t i = 0; i < queue_count; ++i) {
        QueuePtr queue;

        if (is_join_operator && upstream_parallelism > 1) {
            // Join算子需要接受多个上游，使用阻塞队列
            queue = std::make_shared<RingBufferQueue>(blocking_queue_capacity);
        } else if (upstream_parallelism == 1 && downstream_parallelism == 1) {
            // 点对点连接，使用无锁环形队列
            queue = std::make_shared<RingBufferQueue>(ring_buffer_capacity);
        } else {
            // 其他情况使用环形缓冲队列
            queue = std::make_shared<RingBufferQueue>(ring_buffer_capacity);
        }

        queues.push_back(queue);
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

        // 创建队列
        auto queues = createQueues(upstream_info.parallelism,
                                  downstream_info.parallelism,
                                  is_join_operator);

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

            result_partition->setup(std::move(partitioner), output_channels);
        }

        // 为下游的每个ExecutionVertex配置InputGate
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

            input_gate->setup(input_queues);
        }
    }
}

void ExecutionGraph::start() {
    std::cout << "Starting ExecutionGraph with " << operators_.size() << " operators" << std::endl;

    // 启动所有ExecutionVertex
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->start();
        }
    }

    std::cout << "All ExecutionVertices started" << std::endl;
}

void ExecutionGraph::stop() {
    std::cout << "Stopping ExecutionGraph..." << std::endl;

    // 停止所有ExecutionVertex
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->stop();
        }
    }
}

void ExecutionGraph::join() {
    // 等待所有ExecutionVertex完成
    for (const auto& [op, info] : operator_infos_) {
        for (const auto& vertex : info.vertices) {
            vertex->join();
        }
    }

    std::cout << "All ExecutionVertices finished" << std::endl;
}

} // namespace candy

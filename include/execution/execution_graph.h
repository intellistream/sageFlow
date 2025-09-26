#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include "execution/execution_vertex.h"
#include "execution/ring_buffer_queue.h"
#include "execution/blocking_queue.h"
#include "execution/partitioner.h"
#include "operator/operator.h"

namespace candy {

struct OperatorInfo {
    std::shared_ptr<Operator> op;
    size_t parallelism;
    std::vector<std::unique_ptr<ExecutionVertex>> vertices;
};

class ExecutionGraph {
public:
    ExecutionGraph() = default;
    ~ExecutionGraph();

    // 添加算子到执行图
    void addOperator(std::shared_ptr<Operator> op);

    // 连接两个算子之间的数据流
    void connectOperators(std::shared_ptr<Operator> upstream,
                         std::shared_ptr<Operator> downstream,
                         int slot = 0);

    // 构建执行图
    void buildGraph();

    // 启动所有执行顶点
    void start();

    // 停止所有执行顶点
    void stop();

    // 等待所有执行顶点完成
    void join();

private:
    std::vector<std::shared_ptr<Operator>> operators_;
    std::unordered_map<std::shared_ptr<Operator>, OperatorInfo> operator_infos_;

    // 存储算子之间的连接关系
    std::vector<std::tuple<std::shared_ptr<Operator>, std::shared_ptr<Operator>, int>> connections_;

    // 执行期持有的所有队列，用于统一 stop()
    std::vector<QueuePtr> all_queues_;

    // 创建队列连接上下游
    void createConnections();

    // 为特定算子创建执行顶点
    void createVerticesForOperator(std::shared_ptr<Operator> op);

    // 获取适当的队列类型（阻塞队列或环形缓冲队列）
    std::vector<QueuePtr> createQueues(size_t upstream_parallelism,
                                      size_t downstream_parallelism,
                                      bool is_join_operator = false);
};

} // namespace candy

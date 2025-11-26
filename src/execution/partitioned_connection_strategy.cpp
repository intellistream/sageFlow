//
// Created for sageFlow refactoring
//

#include "execution/partitioned_connection_strategy.h"

namespace sageFlow {

std::vector<QueuePtr> PartitionedConnectionStrategy::createQueues(
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    bool is_join_operator) {
  std::vector<QueuePtr> queues;

  // 计算需要创建的队列数量（与上游并行度一致）
  size_t queue_count = upstream_parallelism;

  for (size_t i = 0; i < queue_count; ++i) {
    QueuePtr queue;

    if (is_join_operator) {
      // Join算子需要接受多个上游，使用阻塞队列
      queue = std::make_shared<BlockingQueue>(BLOCKING_QUEUE_CAPACITY);
    } else {
      // 其他情况使用环形缓冲队列
      queue = std::make_shared<RingBufferQueue>(RING_BUFFER_CAPACITY);
    }

    queues.emplace_back(std::move(queue));
  }

  return queues;
}

void PartitionedConnectionStrategy::setupResultPartition(
    ResultPartition* result_partition,
    const std::vector<QueuePtr>& queues,
    size_t upstream_index,
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    int slot) {
  // 创建分区器（使用轮询分区）
  std::unique_ptr<IPartitioner> partitioner = std::make_unique<RoundRobinPartitioner>();

  // 设置输出通道
  std::vector<QueuePtr> output_channels;
  if (downstream_parallelism == 1) {
    // 下游只有一个并行度，所有上游都连接到同一个队列
    output_channels.push_back(queues[upstream_index % queues.size()]);
  } else {
    // 下游有多个并行度，需要分发到多个队列
    for (size_t j = 0; j < downstream_parallelism; ++j) {
      output_channels.push_back(queues[j % queues.size()]);
    }
  }

  result_partition->setup(std::move(partitioner), std::move(output_channels), slot);
}

void PartitionedConnectionStrategy::setupInputGate(
    InputGate* input_gate,
    const std::vector<QueuePtr>& queues,
    size_t downstream_index,
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    bool is_first_setup) {
  // 配置输入队列
  std::vector<QueuePtr> input_queues;
  if (upstream_parallelism == 1) {
    // 上游只有一个并行度
    input_queues.push_back(queues[0]);
  } else {
    // 上游有多个并行度，当前下游顶点需要接收来自多个上游的数据
    for (size_t j = 0; j < upstream_parallelism; ++j) {
      input_queues.push_back(queues[j]);
    }
  }

  // 如果是首次配置，setup；否则追加
  if (is_first_setup) {
    input_gate->setup(std::move(input_queues));
  } else {
    input_gate->addQueues(std::move(input_queues));
  }
}

} // namespace sageFlow

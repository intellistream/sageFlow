//
// Created for sageFlow refactoring
//

#include "execution/shared_queue_connection_strategy.h"

namespace sageFlow {

std::vector<QueuePtr> SharedQueueConnectionStrategy::createQueues(
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    bool is_join_operator) {
  std::vector<QueuePtr> queues;

  // 共享队列模型：创建与下游并行度相同数量的共享队列
  // 这样每个下游工作线程对应一个共享队列，所有上游实例向这些队列发送数据
  size_t queue_count = std::max(downstream_parallelism, size_t(1));

  for (size_t i = 0; i < queue_count; ++i) {
    // 共享队列必须使用阻塞队列以支持多生产者-多消费者场景
    QueuePtr queue = std::make_shared<BlockingQueue>(SHARED_QUEUE_CAPACITY);
    queues.emplace_back(std::move(queue));
  }

  return queues;
}

void SharedQueueConnectionStrategy::setupResultPartition(
    ResultPartition* result_partition,
    const std::vector<QueuePtr>& queues,
    size_t upstream_index,
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    int slot) {
  // 共享队列模型：使用轮询分区器将数据分发到共享队列
  // 所有上游实例都连接到所有共享队列
  std::unique_ptr<IPartitioner> partitioner = std::make_unique<RoundRobinPartitioner>();

  // 每个上游实例都可以向所有共享队列发送数据
  std::vector<QueuePtr> output_channels;
  for (const auto& queue : queues) {
    output_channels.push_back(queue);
  }

  result_partition->setup(std::move(partitioner), std::move(output_channels), slot);
}

void SharedQueueConnectionStrategy::setupInputGate(
    InputGate* input_gate,
    const std::vector<QueuePtr>& queues,
    size_t downstream_index,
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    bool is_first_setup) {
  // 共享队列模型：每个下游实例从对应的共享队列读取数据
  // 下游实例索引对应一个特定的共享队列
  std::vector<QueuePtr> input_queues;
  
  if (downstream_index < queues.size()) {
    // 每个下游工作线程对应一个共享队列
    input_queues.push_back(queues[downstream_index]);
  } else {
    // 如果下游实例数超过队列数（不应该发生），使用轮询分配
    input_queues.push_back(queues[downstream_index % queues.size()]);
  }

  // 如果是首次配置，setup；否则追加
  if (is_first_setup) {
    input_gate->setup(std::move(input_queues));
  } else {
    input_gate->addQueues(std::move(input_queues));
  }
}

} // namespace sageFlow

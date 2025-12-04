//
// Created for sageFlow refactoring
//

#include "execution/connection_strategy.h"
#include "execution/ring_buffer_queue.h"

namespace sageFlow {

std::vector<QueuePtr> ConnectionStrategy::createQueues(
    size_t upstream_parallelism,
    size_t downstream_parallelism) {
  std::vector<QueuePtr> queues;

  // ============================================================
  // SPSC 队列矩阵模式：upstream × downstream 个独立队列
  // ============================================================
  // 
  // 队列布局（假设 upstream=2, downstream=3）：
  //   Q[0] = 上游0 → 下游0
  //   Q[1] = 上游0 → 下游1
  //   Q[2] = 上游0 → 下游2
  //   Q[3] = 上游1 → 下游0
  //   Q[4] = 上游1 → 下游1
  //   Q[5] = 上游1 → 下游2
  // 
  // 索引计算：
  //   queue_index(upstream_i, downstream_j) = upstream_i * downstream_parallelism + downstream_j
  // 
  // 特点：
  // - 每个队列是 SPSC（单生产者单消费者），无锁高性能
  // - RoundRobin Partitioner 实现负载均衡
  // - 可搭配 SharedWindowState 或 PartitionedWindowState
  
  size_t queue_count = upstream_parallelism * downstream_parallelism;

  for (size_t i = 0; i < queue_count; ++i) {
    // 使用无锁 SPSC 环形缓冲队列
    QueuePtr queue = std::make_shared<RingBufferQueue>(RING_BUFFER_CAPACITY);
    queues.emplace_back(std::move(queue));
  }

  return queues;
}

void ConnectionStrategy::setupResultPartition(
    ResultPartition* result_partition,
    const std::vector<QueuePtr>& queues,
    size_t upstream_index,
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    int slot,
    std::unique_ptr<IPartitioner> partitioner) {
  // ============================================================
  // 为上游实例配置输出通道
  // ============================================================
  // 
  // 上游 i 的输出队列范围：
  //   [i * downstream_parallelism, (i+1) * downstream_parallelism)
  // 
  // 使用传入的 Partitioner 或默认 RoundRobin 在这些队列中选择目标
  
  if (!partitioner) {
    // 默认使用 RoundRobin 分区器（适用于共享索引 Join）
    partitioner = std::make_unique<RoundRobinPartitioner>();
  }

  std::vector<QueuePtr> output_channels;
  size_t base_index = upstream_index * downstream_parallelism;
  
  for (size_t j = 0; j < downstream_parallelism; ++j) {
    size_t queue_index = base_index + j;
    if (queue_index < queues.size()) {
      output_channels.push_back(queues[queue_index]);
    }
  }

  result_partition->setup(std::move(partitioner), std::move(output_channels), slot);
}

void ConnectionStrategy::setupInputGate(
    InputGate* input_gate,
    const std::vector<QueuePtr>& queues,
    size_t downstream_index,
    size_t upstream_parallelism,
    size_t downstream_parallelism,
    bool is_first_setup) {
  // ============================================================
  // 为下游实例配置输入队列
  // ============================================================
  // 
  // 下游 j 需要读取的队列：
  //   从每个上游 i 读取 queue[i * downstream_parallelism + j]
  // 
  // 例如 downstream_index=1, upstream_parallelism=2, downstream_parallelism=3:
  //   读取 Q[0*3+1]=Q[1] 和 Q[1*3+1]=Q[4]
  
  std::vector<QueuePtr> input_queues;
  
  for (size_t i = 0; i < upstream_parallelism; ++i) {
    size_t queue_index = i * downstream_parallelism + downstream_index;
    if (queue_index < queues.size()) {
      input_queues.push_back(queues[queue_index]);
    }
  }

  // 如果是首次配置，setup；否则追加（用于 Join 有多个上游的情况）
  if (is_first_setup) {
    input_gate->setup(std::move(input_queues));
  } else {
    input_gate->addQueues(std::move(input_queues));
  }
}

} // namespace sageFlow

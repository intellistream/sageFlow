//
// Created for sageFlow refactoring
//

#pragma once

#include "execution/connection_strategy.h"
#include "execution/blocking_queue.h"
#include "execution/ring_buffer_queue.h"

namespace sageFlow {

/**
 * @brief 基于分区的点对点连接策略
 * 
 * 这是当前sageFlow的默认连接方式：
 * - 上游根据IPartitioner分区规则将数据发送到下游对应的算子实例
 * - 每个上游实例都有自己独立的输出队列
 * - 适用于基于分区的Join方法
 */
class PartitionedConnectionStrategy : public IConnectionStrategy {
public:
  PartitionedConnectionStrategy() = default;
  ~PartitionedConnectionStrategy() override = default;

  std::vector<QueuePtr> createQueues(
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      bool is_join_operator) override;

  void setupResultPartition(
      ResultPartition* result_partition,
      const std::vector<QueuePtr>& queues,
      size_t upstream_index,
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      int slot) override;

  void setupInputGate(
      InputGate* input_gate,
      const std::vector<QueuePtr>& queues,
      size_t downstream_index,
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      bool is_first_setup) override;

  ConnectionType getType() const override {
    return ConnectionType::PARTITIONED;
  }

private:
  // 队列容量配置
  static constexpr size_t RING_BUFFER_CAPACITY = 1024;
  static constexpr size_t BLOCKING_QUEUE_CAPACITY = 512;
};

} // namespace sageFlow

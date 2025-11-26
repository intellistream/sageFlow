//
// Created for sageFlow refactoring
//

#pragma once

#include "execution/connection_strategy.h"
#include "execution/blocking_queue.h"

namespace sageFlow {

/**
 * @brief 基于共享任务队列的连接策略
 * 
 * 这是为支持共享索引模型而设计的新连接方式：
 * - 所有上游实例共享少量的任务队列（通常与下游并行度一致）
 * - 下游工作线程从共享队列中领取任务，避免竞态条件
 * - 类似PIM-Tree等Index-Based Join的任务获取规则
 * - 适用于共享索引的Join方法
 * 
 * 核心区别：
 * - 分区模型：每个上游实例有独立队列，数据根据分区规则路由
 * - 共享队列模型：上游实例共享队列，下游线程竞争获取任务
 */
class SharedQueueConnectionStrategy : public IConnectionStrategy {
public:
  SharedQueueConnectionStrategy() = default;
  ~SharedQueueConnectionStrategy() override = default;

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
    return ConnectionType::SHARED_QUEUE;
  }

private:
  // 共享队列模型使用阻塞队列以支持多生产者-多消费者场景
  static constexpr size_t SHARED_QUEUE_CAPACITY = 2048;
};

} // namespace sageFlow

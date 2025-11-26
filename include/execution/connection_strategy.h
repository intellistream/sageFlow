//
// Created for sageFlow refactoring
//

#pragma once

#include <memory>
#include <vector>
#include "execution/iqueue.h"
#include "execution/partitioner.h"
#include "execution/input_gate.h"
#include "execution/result_partition.h"

namespace sageFlow {

/**
 * @brief 连接策略的枚举类型
 * 
 * PARTITIONED: 基于分区的点对点连接（适用于分区模型）
 * SHARED_QUEUE: 共享任务队列连接（适用于共享索引模型）
 */
enum class ConnectionType {
  PARTITIONED,
  SHARED_QUEUE
};

/**
 * @brief 上下游算子连接的抽象策略接口
 * 
 * 该接口定义了如何连接上游和下游算子实例的逻辑。
 * 不同的连接策略适用于不同的场景：
 * - 分区策略：适用于基于分区的Join方法，上游根据分区规则直接发送到下游对应实例
 * - 共享队列策略：适用于共享索引的Join方法，使用共享任务队列避免竞态条件
 */
class IConnectionStrategy {
public:
  virtual ~IConnectionStrategy() = default;

  /**
   * @brief 创建上下游之间的队列连接
   * 
   * @param upstream_parallelism 上游算子的并行度
   * @param downstream_parallelism 下游算子的并行度
   * @param is_join_operator 下游是否为Join算子
   * @return 创建的队列列表
   */
  virtual std::vector<QueuePtr> createQueues(
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      bool is_join_operator) = 0;

  /**
   * @brief 为上游执行顶点配置ResultPartition
   * 
   * @param result_partition 待配置的ResultPartition
   * @param queues 已创建的队列列表
   * @param upstream_index 上游顶点的索引
   * @param upstream_parallelism 上游算子的并行度
   * @param downstream_parallelism 下游算子的并行度
   * @param slot 连接使用的slot标识
   */
  virtual void setupResultPartition(
      ResultPartition* result_partition,
      const std::vector<QueuePtr>& queues,
      size_t upstream_index,
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      int slot) = 0;

  /**
   * @brief 为下游执行顶点配置InputGate
   * 
   * @param input_gate 待配置的InputGate
   * @param queues 已创建的队列列表
   * @param downstream_index 下游顶点的索引
   * @param upstream_parallelism 上游算子的并行度
   * @param downstream_parallelism 下游算子的并行度
   * @param is_first_setup 是否为首次配置（true时调用setup，false时调用addQueues）
   */
  virtual void setupInputGate(
      InputGate* input_gate,
      const std::vector<QueuePtr>& queues,
      size_t downstream_index,
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      bool is_first_setup) = 0;

  /**
   * @brief 获取连接策略类型
   */
  virtual ConnectionType getType() const = 0;
};

} // namespace sageFlow

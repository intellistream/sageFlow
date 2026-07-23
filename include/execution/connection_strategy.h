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
 * @brief 统一的连接策略实现
 * 
 * 采用 SPSC 队列矩阵模式连接上下游算子：
 * - 队列数量: upstream × downstream
 * - 每对(上游i, 下游j)有独立 SPSC 队列
 * - 数据通过 Partitioner 确定性路由到目标下游
 * 
 * 队列布局（upstream=2, downstream=3 时）：
 * ```
 *              下游0    下游1    下游2
 *   上游0  →   Q[0]     Q[1]     Q[2]
 *   上游1  →   Q[3]     Q[4]     Q[5]
 * 
 *   queue_index = upstream_i * downstream_parallelism + downstream_j
 * ```
 * 
 * 数据流：
 * - 上游 i 通过 Partitioner 选择目标下游 j，写入 Q[i*D+j]
 * - 下游 j 从所有上游的对应队列 Q[0*D+j], Q[1*D+j], ... 轮询读取
 * 
 * 性能特点：
 * - SPSC 队列无锁，高吞吐量
 * - RoundRobin Partitioner 实现负载均衡
 * - 可搭配 SharedWindowState 或 PartitionedWindowState
 */
class ConnectionStrategy {
public:
  ConnectionStrategy() = default;
  ~ConnectionStrategy() = default;

  /**
   * @brief 创建上下游之间的队列连接
   * 
   * @param upstream_parallelism 上游算子的并行度
   * @param downstream_parallelism 下游算子的并行度
   * @return 创建的队列列表，数量为 upstream × downstream
   */
  std::vector<QueuePtr> createQueues(
      size_t upstream_parallelism,
      size_t downstream_parallelism);

  /**
   * @brief 为上游执行顶点配置ResultPartition
   * 
   * 上游 i 连接到队列 [i*D, i*D+1, ..., i*D+D-1]，
   * 通过 Partitioner 选择目标下游。
   * 
   * @param result_partition 待配置的ResultPartition
   * @param queues 已创建的队列列表
   * @param upstream_index 上游顶点的索引
   * @param upstream_parallelism 上游算子的并行度
   * @param downstream_parallelism 下游算子的并行度
   * @param slot 连接使用的slot标识
   * @param partitioner 可选的自定义分区器（nullptr 时使用 RoundRobin）
   */
  void setupResultPartition(
      ResultPartition* result_partition,
      const std::vector<QueuePtr>& queues,
      size_t upstream_index,
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      int slot,
      std::unique_ptr<IPartitioner> partitioner = nullptr);

  /**
   * @brief 为下游执行顶点配置InputGate
   * 
   * 下游 j 读取队列 [0*D+j, 1*D+j, 2*D+j, ...]，
   * 即从每个上游读取路由给自己的队列。
   * 
   * @param input_gate 待配置的InputGate
   * @param queues 已创建的队列列表
   * @param downstream_index 下游顶点的索引
   * @param upstream_parallelism 上游算子的并行度
   * @param downstream_parallelism 下游算子的并行度
   * @param is_first_setup 是否为首次配置（true时调用setup，false时调用addQueues）
   */
  void setupInputGate(
      InputGate* input_gate,
      const std::vector<QueuePtr>& queues,
      size_t downstream_index,
      size_t upstream_parallelism,
      size_t downstream_parallelism,
      bool is_first_setup);

private:
  // SPSC 环形缓冲队列容量
  // TODO: 根据流速或上游数据量动态调整队列大小
  // Issue URL: https://github.com/DataSysResearch/BriskFlow/issues/81
  //       可考虑的方案：
  //       1. 基于背压(backpressure)的动态扩容
  //       2. 根据上游算子的预估输出量在 buildGraph 时计算合适容量
  //       3. 使用可增长的队列实现替代固定大小的环形缓冲
  static constexpr size_t RING_BUFFER_CAPACITY = 8192;
};

} // namespace sageFlow

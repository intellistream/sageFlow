#pragma once

#include <memory>

#include "execution/partitioner.h"
#include "operator/utils/join_strategy_config.h"

namespace sageFlow {

/**
 * @brief Creates JoinOperator's preferred upstream partitioner.
 *
 * Shared-state strategies return nullptr so the execution graph uses the
 * default round-robin routing. Clustered/S3J/VSJoin strategies can return a
 * vector-space partitioner with multicast support.
 */
class JoinPartitionerFactory {
 public:
  /**
   * @brief Create the preferred partitioner for a strategy config.
   *
   * @param config Strategy config.
   * @param use_strategy_config False when the operator is not config-driven.
   * @param dimension Vector dimension override. Uses config dimension when 0.
   * @param num_partitions Partition count override. Uses config defaults when 0.
   * @return Partitioner instance, or nullptr for default graph routing.
   */
  static std::unique_ptr<IPartitioner> createPreferred(
      const JoinStrategyConfig& config,
      bool use_strategy_config,
      int dimension,
      int num_partitions);
};

}  // namespace sageFlow

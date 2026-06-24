#pragma once

#include <cstddef>
#include <vector>

#include "common/data_types.h"
#include "execution/runtime_context.h"
#include "operator/utils/join_strategy_config.h"

namespace sageFlow {

/**
 * @brief Computes VSJoin target subtasks and records optional routing diagnostics.
 *
 * Routing is evaluated while the input record is still owned by the transport
 * `Response`, before JoinOperator promotes it to `RecordView`.
 */
class VSJoinRouter {
 public:
  /**
   * @brief Compute deduplicated physical target subtasks for one record.
   *
   * Returns at least one subtask; if no partitioner is available, the current
   * subtask is used as a fallback.
   */
  static std::vector<size_t> computeTargetSubtasks(
      const Response& record,
      const RuntimeContext& context,
      const JoinStrategyConfig& config,
      bool use_strategy_config,
      int dimension,
      size_t subtask_index);

  /**
   * @brief Record sampled VSJoin input distribution diagnostics when enabled.
   */
  static void recordSubtaskDebugStats(
      int slot,
      int left_slot_id,
      size_t subtask_index,
      const RuntimeContext& context,
      JoinAlgorithm algorithm);
};

}  // namespace sageFlow

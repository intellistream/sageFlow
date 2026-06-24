#pragma once

#include <memory>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "function/join_function.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "state/window_state.h"

namespace sageFlow {

/**
 * @brief Builds and wires JoinOperator runtime components from a strategy config.
 *
 * This component owns initialization-time decisions only. It does not process
 * records and does not retain references after returning `Result`.
 */
class JoinOperatorInitializer {
 public:
  /**
   * @brief Fully initialized runtime bundle consumed by JoinOperator.
   */
  struct Result {
    JoinStrategyConfig strategy_config;
    std::unique_ptr<BaseMethod> join_method;
    std::unique_ptr<WindowState> left_state;
    std::unique_ptr<WindowState> right_state;
    int left_index_id = -1;
    int right_index_id = -1;
    int vsjoin_global_left_id = -1;
    int vsjoin_global_right_id = -1;
    std::vector<int> vsjoin_local_left_ids;
    std::vector<int> vsjoin_local_right_ids;
    bool use_index = false;
    bool use_shared_state = false;
    size_t batch_delete_threshold = 0;
    size_t num_logical_partitions = 0;
  };

  /**
   * @brief Validate config, create strategy components, and wire method dependencies.
   *
   * @param config Strategy config copied by value so runtime-derived adjustments
   *               do not mutate caller-owned config until JoinOperator accepts the result.
   * @param concurrency_manager Shared ConcurrencyManager used by Join methods and indexes.
   * @param join_func Non-owning pointer owned by JoinOperator.
   * @param context Runtime subtask and parallelism context.
   * @param virtual_nodes_per_partition VSJoin logical partition fanout.
   * @param min_batch_delete_threshold Lower bound for expired UID batch deletion.
   * @param batch_delete_divisor Divisor used to derive batch deletion threshold.
   * @return Runtime bundle ready to install into JoinOperator.
   */
  static Result initialize(
      JoinStrategyConfig config,
      const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
      JoinFunction* join_func,
      const RuntimeContext& context,
      size_t virtual_nodes_per_partition,
      size_t min_batch_delete_threshold,
      size_t batch_delete_divisor);
};

}  // namespace sageFlow

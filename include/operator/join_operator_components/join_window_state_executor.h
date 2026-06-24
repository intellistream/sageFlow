#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "function/join_function.h"
#include "operator/join_operator_components/join_result_emitter.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/utils/join_strategy_config.h"
#include "state/window_state.h"

namespace sageFlow {

/**
 * @brief Executes the active WindowState Insert-then-Query join path.
 *
 * The executor owns no state. It receives non-owning pointers from JoinOperator
 * and applies a single per-record/per-target-subtask IQ step against WindowState
 * and ConcurrencyManager.
 */
class JoinWindowStateExecutor {
 public:
  /**
   * @brief Execution settings copied from JoinOperator at call time.
   */
  struct Config {
    JoinAlgorithm algorithm = JoinAlgorithm::BRUTEFORCE;
    bool use_index = false;
    size_t batch_delete_threshold = 50;
    int left_slot_id = 0;
    int right_slot_id = 1;
  };

  /**
   * @brief Construct a stateless executor view over JoinOperator-owned objects.
   *
   * Raw pointers are non-owning and must remain valid for the duration of the
   * executor call. The ConcurrencyManager is shared to keep index/storage access alive.
   */
  JoinWindowStateExecutor(
      Config config,
      JoinFunction* join_func,
      BaseMethod* join_method,
      WindowState* left_state,
      WindowState* right_state,
      std::shared_ptr<ConcurrencyManager> concurrency_manager,
      int left_index_id,
      int right_index_id,
      std::vector<int> local_left_ids,
      std::vector<int> local_right_ids);

  /**
   * @brief Resolve the shared/global index id for a non-VSJoin input slot.
   */
  int indexIdForSlot(int slot) const;

  /**
   * @brief Resolve the VSJoin local index id for an input slot and target subtask.
   */
  int localIndexIdForSlotAndSubtask(int slot, size_t subtask_index) const;

  /**
   * @brief Insert the current record into state/index and perform safe eviction.
   */
  bool updateSide(
      WindowState* state,
      WindowState* opposite_state,
      int index_id_for_cc,
      RecordView data_ptr,
      int64_t now_time_stamp,
      int slot,
      size_t subtask_index) const;

  /**
   * @brief Query the opposite side, apply event-time filtering, and materialize results.
   */
  void executeJoin(
      const VectorRecord* data_ptr,
      WindowState* opposite_state,
      int slot,
      size_t subtask_index,
      std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& output) const;

 private:
  /**
   * @brief Fetch candidates through the configured JoinMethod.
   */
  std::vector<RecordView> getCandidates(
      const VectorRecord* data_ptr,
      WindowState* state,
      size_t subtask_index) const;

  Config config_;
  JoinFunction* join_func_;
  BaseMethod* join_method_;
  WindowState* left_state_;
  WindowState* right_state_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  int left_index_id_;
  int right_index_id_;
  std::vector<int> local_left_ids_;
  std::vector<int> local_right_ids_;
};

}  // namespace sageFlow

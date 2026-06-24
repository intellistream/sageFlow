#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/data_types.h"
#include "execution/collector.h"
#include "function/join_function.h"

namespace sageFlow {

/**
 * @brief Materializes join-function outputs and emits them through Collector.
 *
 * Input records are copied into `Response` objects before calling
 * `JoinFunction::Execute`; output ownership remains
 * `std::unique_ptr<VectorRecord>` at the stream boundary.
 */
class JoinResultEmitter {
 public:
  /**
   * @param join_func Non-owning pointer owned by JoinOperator.
   * @param left_slot_id Slot id used for emitted joined records.
   */
  explicit JoinResultEmitter(JoinFunction* join_func, int left_slot_id);

  /**
   * @brief Execute the join function for one validated candidate pair.
   *
   * @param current The input record currently being processed.
   * @param candidate Candidate from the opposite side.
   * @param input_slot Slot of `current`; determines left/right argument order.
   * @param output Local output pool receiving materialized result records.
   */
  void appendJoinedResult(
      const VectorRecord& current,
      const VectorRecord& candidate,
      int input_slot,
      std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& output) const;

  /**
   * @brief Emit all materialized results and record emit/e2e metrics.
   */
  void emit(
      std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& output,
      Collector& collector,
      uint64_t apply_enter_ns) const;

 private:
  JoinFunction* join_func_;
  int left_slot_id_;
};

}  // namespace sageFlow

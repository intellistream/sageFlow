#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/data_types.h"
#include "execution/collector.h"
#include "function/join_function.h"
#include "operator/utils/join_strategy_config.h"

namespace sageFlow {

// Active output element for the join result pool. A Response carries either a
// CONCAT record (ResponseType::Record) or a pair payload (ResponseType::RecordPair),
// so both materialization modes share one zero-copy-capable transport path.
using JoinOutputItem = std::pair<int, Response>;

/**
 * @brief Materializes join hits and emits them through Collector.
 *
 * Two materialization modes:
 * - CONCAT: copies both records into the join function, which produces one new
 *   record (legacy behavior; e.g. concatenating the two vectors).
 * - PAIR_PASSTHROUGH: packages the two records as read-only shared references
 *   (RecordView) plus similarity, with zero VectorData deep copy on emit.
 */
class JoinResultEmitter {
 public:
  /**
   * @param join_func Non-owning pointer owned by JoinOperator.
   * @param left_slot_id Slot id used for emitted joined records.
   * @param mode Materialization mode (default CONCAT preserves legacy behavior).
   */
  explicit JoinResultEmitter(JoinFunction* join_func, int left_slot_id,
                             MaterializationMode mode = MaterializationMode::CONCAT);

  /**
   * @brief CONCAT path: execute the join function for one validated pair.
   *
   * @param current The input record currently being processed.
   * @param candidate Candidate from the opposite side.
   * @param input_slot Slot of `current`; determines left/right argument order.
   * @param output Local output pool receiving materialized results.
   */
  void appendJoinedResult(
      const VectorRecord& current,
      const VectorRecord& candidate,
      int input_slot,
      std::vector<JoinOutputItem>& output) const;

  /**
   * @brief PAIR_PASSTHROUGH path: package (left, right, similarity) as shared
   *        references, with no VectorData deep copy.
   *
   * @param probe The input record currently being processed (shared view).
   * @param candidate Candidate from the opposite side (shared view).
   * @param input_slot Slot of `probe`; determines left/right orientation.
   * @param similarity Similarity score for the matched pair (sentinel if unknown).
   * @param output Local output pool receiving materialized pair results.
   */
  void appendPair(
      const RecordView& probe,
      const RecordView& candidate,
      int input_slot,
      double similarity,
      std::vector<JoinOutputItem>& output) const;

  /**
   * @brief Emit all materialized results and record emit/e2e metrics.
   */
  void emit(
      std::vector<JoinOutputItem>& output,
      Collector& collector,
      uint64_t apply_enter_ns) const;

 private:
  JoinFunction* join_func_;
  int left_slot_id_;
  MaterializationMode mode_;
};

}  // namespace sageFlow

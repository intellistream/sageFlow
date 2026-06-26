#include "operator/join_operator_components/join_result_emitter.h"

#include "operator/join_metrics.h"
#include "utils/logger.h"

namespace sageFlow {

JoinResultEmitter::JoinResultEmitter(JoinFunction* join_func, int left_slot_id,
                                     MaterializationMode mode)
    : join_func_(join_func), left_slot_id_(left_slot_id), mode_(mode) {}

void JoinResultEmitter::appendJoinedResult(
    const VectorRecord& current,
    const VectorRecord& candidate,
    int input_slot,
    std::vector<JoinOutputItem>& output) const {
  std::unique_ptr<VectorRecord> left_copy;
  std::unique_ptr<VectorRecord> right_copy;

  if (input_slot == left_slot_id_) {
    left_copy = std::make_unique<VectorRecord>(current);
    right_copy = std::make_unique<VectorRecord>(candidate);
  } else {
    left_copy = std::make_unique<VectorRecord>(candidate);
    right_copy = std::make_unique<VectorRecord>(current);
  }

  Response lhs{ResponseType::Record, std::move(left_copy)};
  Response rhs{ResponseType::Record, std::move(right_copy)};

  try {
    MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
    metrics_increment(JoinMetrics::instance().join_function_count);
    auto res = join_func_->Execute(lhs, rhs);
    t_joinF.stop();
    if (res.record_) {
      output.emplace_back(left_slot_id_, std::move(res));
    }
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("JOIN_RESULT", "Exception while materializing join result: what={}", e.what());
    throw;
  }
}

void JoinResultEmitter::appendPair(
    const RecordView& probe,
    const RecordView& candidate,
    int input_slot,
    double similarity,
    std::vector<JoinOutputItem>& output) const {
  // Orient left/right by the probe's slot; only refcount bumps, no VectorData copy.
  RecordView left = (input_slot == left_slot_id_) ? probe : candidate;
  RecordView right = (input_slot == left_slot_id_) ? candidate : probe;

  MetricsTimer t_joinF(JoinMetrics::instance().join_function_ns);
  metrics_increment(JoinMetrics::instance().join_function_count);
  auto payload = std::make_unique<RecordPairPayload>(std::move(left), std::move(right), similarity);
  t_joinF.stop();
  output.emplace_back(left_slot_id_, Response{ResponseType::RecordPair, std::move(payload)});
}

void JoinResultEmitter::emit(
    std::vector<JoinOutputItem>& output,
    Collector& collector,
    uint64_t apply_enter_ns) const {
  MetricsTimer t_emit(JoinMetrics::instance().emit_ns);
  for (auto& p : output) {
    collector.collect(std::make_unique<Response>(std::move(p.second)), p.first);
    metrics_increment(JoinMetrics::instance().total_emits);
    metrics_increment(JoinMetrics::instance().emit_count);
    metrics_record_e2e_latency(apply_enter_ns);
  }
}

}  // namespace sageFlow

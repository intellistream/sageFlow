#include "operator/join_operator_components/join_window_state_executor.h"

#include <limits>
#include <utility>

#include "compute_engine/compute_engine.h"
#include "operator/join_metrics.h"
#include "operator/join_operator_methods/lsh_method.h"
#include "utils/logger.h"

namespace sageFlow {

JoinWindowStateExecutor::JoinWindowStateExecutor(
    Config config,
    JoinFunction* join_func,
    BaseMethod* join_method,
    WindowState* left_state,
    WindowState* right_state,
    std::shared_ptr<ConcurrencyManager> concurrency_manager,
    int left_index_id,
    int right_index_id,
    std::vector<int> local_left_ids,
    std::vector<int> local_right_ids)
    : config_(config),
      join_func_(join_func),
      join_method_(join_method),
      left_state_(left_state),
      right_state_(right_state),
      concurrency_manager_(std::move(concurrency_manager)),
      left_index_id_(left_index_id),
      right_index_id_(right_index_id),
      local_left_ids_(std::move(local_left_ids)),
      local_right_ids_(std::move(local_right_ids)) {}

int JoinWindowStateExecutor::indexIdForSlot(int slot) const {
  if (config_.algorithm == JoinAlgorithm::VSJOIN) {
    return -1;
  }
  return (slot == config_.left_slot_id) ? left_index_id_ : right_index_id_;
}

int JoinWindowStateExecutor::localIndexIdForSlotAndSubtask(int slot, size_t subtask_index) const {
  const auto& local_ids = (slot == config_.left_slot_id)
      ? local_left_ids_
      : local_right_ids_;
  return (subtask_index < local_ids.size()) ? local_ids[subtask_index] : -1;
}

std::vector<RecordView> JoinWindowStateExecutor::getCandidates(
    const VectorRecord* data_ptr,
    WindowState* state,
    size_t subtask_index) const {
  MetricsTimer t_fetch(JoinMetrics::instance().candidate_fetch_ns);
  metrics_increment(JoinMetrics::instance().candidate_fetch_count);

  int query_slot = (state == right_state_) ? config_.left_slot_id : config_.right_slot_id;
  return join_method_->ExecuteEager(*data_ptr, query_slot, subtask_index);
}

bool JoinWindowStateExecutor::updateSide(
    WindowState* state,
    WindowState* opposite_state,
    int index_id_for_cc,
    RecordView data_ptr,
    int64_t now_time_stamp,
    int slot,
    size_t subtask_index) const {
  if (!data_ptr) {
    return false;
  }

  if (slot == config_.left_slot_id) {
    JoinMetrics::instance().total_records_left.fetch_add(1, std::memory_order_relaxed);
  } else {
    JoinMetrics::instance().total_records_right.fetch_add(1, std::memory_order_relaxed);
  }

  if (auto* lsh = dynamic_cast<LSHMethod*>(join_method_)) {
    lsh->onRecordAdded(*data_ptr, slot);
  }

  {
    MetricsTimer t_window_ins(JoinMetrics::instance().window_insert_ns);
    state->addRecord(data_ptr, subtask_index);
  }
  metrics_increment(JoinMetrics::instance().window_insert_count);

  if (config_.use_index && concurrency_manager_ && index_id_for_cc != -1) {
    MetricsTimer t_idx(JoinMetrics::instance().index_insert_ns);

    if (config_.algorithm == JoinAlgorithm::VSJOIN) {
      const int local_index_id = localIndexIdForSlotAndSubtask(slot, subtask_index);
      if (local_index_id >= 0) {
        concurrency_manager_->insert(local_index_id, data_ptr);
      }
      SAGEFLOW_LOG_DEBUG("VSJOIN", "subtask_{} inserted to local_id={}", subtask_index, local_index_id);
    } else {
      concurrency_manager_->insert(index_id_for_cc, data_ptr);
    }

    metrics_increment(JoinMetrics::instance().index_op_count);
  }

  state->updateMaxSeenTimestamp(now_time_stamp, subtask_index);
  const int64_t safe_evict_ts =
      state->getSafeEvictTimestamp(subtask_index, opposite_state);

  if (safe_evict_ts != std::numeric_limits<int64_t>::min()) {
    size_t before_size = state->size(subtask_index);
    MetricsTimer t_window_evict(JoinMetrics::instance().expire_ns);
    state->evictExpired(safe_evict_ts, join_func_->getWindowSize(), subtask_index);
    size_t after_size = state->size(subtask_index);
    if (before_size > after_size) {
      metrics_increment(JoinMetrics::instance().expire_count, before_size - after_size);
    }
  }

  const int erase_index_id = (config_.algorithm == JoinAlgorithm::VSJOIN)
      ? localIndexIdForSlotAndSubtask(slot, subtask_index)
      : index_id_for_cc;
  if (config_.use_index && concurrency_manager_ && erase_index_id != -1) {
    size_t expired_count = state->getExpiredCount(subtask_index);
    if (expired_count >= config_.batch_delete_threshold) {
      auto expired_uids = state->flushExpiredUids(subtask_index);

      for (uint64_t uid : expired_uids) {
        concurrency_manager_->erase(erase_index_id, uid);
        metrics_increment(JoinMetrics::instance().index_op_count);
      }

      SAGEFLOW_LOG_DEBUG("JOIN_STATE",
                         "Batch deleted {} expired records from index {}",
                         expired_uids.size(),
                         erase_index_id);
    }
  }

  return true;
}

void JoinWindowStateExecutor::executeJoin(
    const RecordView& data_view,
    WindowState* opposite_state,
    int slot,
    size_t subtask_index,
    std::vector<JoinOutputItem>& output) const {
  const VectorRecord* data_ptr = data_view.get();
  auto candidates = getCandidates(data_ptr, opposite_state, subtask_index);

  MetricsTimer t_similarity(JoinMetrics::instance().similarity_ns);

  int64_t window_size = join_func_->getWindowSize();
  int64_t window_lower_bound = data_ptr->timestamp_ - window_size;
  int64_t window_upper_bound = data_ptr->timestamp_ + window_size;

  JoinResultEmitter emitter(join_func_, config_.left_slot_id, config_.materialization_mode);
  const bool pair_mode = config_.materialization_mode == MaterializationMode::PAIR_PASSTHROUGH;
  ComputeEngine compute_engine;
  for (const auto& cand : candidates) {
    if (cand->timestamp_ < window_lower_bound || cand->timestamp_ > window_upper_bound) {
      continue;
    }
    metrics_increment(JoinMetrics::instance().similarity_count);

    try {
      t_similarity.pause();
      if (pair_mode) {
        t_similarity.resume();
        const double similarity =
            config_.similarity_mode == SimilarityMode::NORMALIZED
                ? compute_engine.NormalizedSimilarity(
                      data_ptr->data_, cand->data_, config_.similarity_alpha)
                : compute_engine.Similarity(
                      data_ptr->data_, cand->data_, config_.similarity_alpha);
        t_similarity.pause();
        emitter.appendPair(data_view, cand, slot, similarity, output);
      } else {
        emitter.appendJoinedResult(*data_ptr, *cand, slot, output);
      }
      t_similarity.resume();
    } catch (const std::exception& e) {
      SAGEFLOW_LOG_ERROR("JOIN_STATE", "Exception in JoinWindowStateExecutor::executeJoin: what={}", e.what());
      throw;
    }
  }
}

}  // namespace sageFlow

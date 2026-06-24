#include "operator/join_operator_components/vsjoin_global_index_rebuilder.h"

#include <chrono>
#include <limits>
#include <unordered_set>
#include <vector>

#include "index/index.h"
#include "utils/logger.h"

namespace sageFlow {

VSJoinGlobalIndexRebuilder::VSJoinGlobalIndexRebuilder(
    const JoinStrategyConfig& strategy_config,
    const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
    JoinFunction* join_func,
    WindowState* left_state,
    WindowState* right_state,
    int global_left_id,
    int global_right_id,
    size_t parallelism)
    : strategy_config_(strategy_config),
      concurrency_manager_(concurrency_manager),
      join_func_(join_func),
      left_state_(left_state),
      right_state_(right_state),
      global_left_id_(global_left_id),
      global_right_id_(global_right_id),
      parallelism_(parallelism) {}

VSJoinGlobalIndexRebuilder::~VSJoinGlobalIndexRebuilder() {
  stop();
}

void VSJoinGlobalIndexRebuilder::start() {
  std::call_once(started_, [this]() {
    running_.store(true, std::memory_order_release);
    interval_ms_.store(strategy_config_.vsjoin_rebuild_interval_ms, std::memory_order_release);
    thread_ = std::make_unique<std::thread>(&VSJoinGlobalIndexRebuilder::rebuildLoop, this);

    SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER",
                      "Background rebuild thread started (interval={}ms, parallelism={})",
                      interval_ms_.load(),
                      parallelism_);
  });
}

void VSJoinGlobalIndexRebuilder::stop() {
  if (running_.exchange(false)) {
    if (thread_ && thread_->joinable()) {
      thread_->join();
    }
    SAGEFLOW_LOG_INFO("VSJOIN_REBUILDER", "Background rebuild thread stopped");
  }
}

int64_t VSJoinGlobalIndexRebuilder::logicalWindowLowerBound(
    int64_t reference_timestamp) const {
  const int64_t window_size = join_func_ ? join_func_->getWindowSize() : 0;
  if (window_size <= 0) {
    return std::numeric_limits<int64_t>::min();
  }
  if (reference_timestamp <= std::numeric_limits<int64_t>::min() + window_size) {
    return std::numeric_limits<int64_t>::min();
  }
  return reference_timestamp - window_size;
}

void VSJoinGlobalIndexRebuilder::rebuildLoop() {
  while (running_.load(std::memory_order_acquire)) {
    const int64_t interval_ms = interval_ms_.load(std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

    if (!running_.load(std::memory_order_acquire)) {
      break;
    }

    if (!left_state_ || !right_state_) {
      SAGEFLOW_LOG_WARN("VSJOIN_REBUILD", "WindowState not ready, skip rebuild");
      continue;
    }

    std::vector<std::vector<RecordView>> left_snapshots;
    std::vector<std::vector<RecordView>> right_snapshots;
    left_snapshots.reserve(parallelism_);
    right_snapshots.reserve(parallelism_);

    std::unordered_set<uint64_t> seen_left_uids;
    std::unordered_set<uint64_t> seen_right_uids;
    std::vector<const VectorRecord*> unique_left_records;
    std::vector<const VectorRecord*> unique_right_records;

    for (size_t p = 0; p < parallelism_; ++p) {
      left_snapshots.push_back(left_state_->getRecordsSnapshot(p));
      right_snapshots.push_back(right_state_->getRecordsSnapshot(p));

      for (const auto& r : left_snapshots.back()) {
        if (r && seen_left_uids.insert(r->uid_).second) {
          unique_left_records.push_back(r.get());
        }
      }
      for (const auto& r : right_snapshots.back()) {
        if (r && seen_right_uids.insert(r->uid_).second) {
          unique_right_records.push_back(r.get());
        }
      }
    }

    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const int64_t window_lower = logicalWindowLowerBound(now_ms);

    std::vector<const VectorRecord*> valid_left_records;
    std::vector<const VectorRecord*> valid_right_records;
    valid_left_records.reserve(unique_left_records.size());
    valid_right_records.reserve(unique_right_records.size());

    for (const auto* r : unique_left_records) {
      if (r && r->timestamp_ >= window_lower) {
        valid_left_records.push_back(r);
      }
    }
    for (const auto* r : unique_right_records) {
      if (r && r->timestamp_ >= window_lower) {
        valid_right_records.push_back(r);
      }
    }

    if (concurrency_manager_ && global_left_id_ >= 0 && global_right_id_ >= 0) {
      IVFParameters global_ivf_params;
      global_ivf_params.nlist = strategy_config_.ivf_nlist;
      global_ivf_params.nprobes = strategy_config_.ivf_nprobes;
      global_ivf_params.rebuild_threshold = strategy_config_.ivf_rebuild_threshold;

      const int dimension = join_func_ ? join_func_->getDim() : strategy_config_.dimension;
      const int new_left_id = concurrency_manager_->build_index_from_records(
          "vsjoin_global_left_rebuilt",
          IndexType::IVF,
          dimension,
          global_ivf_params,
          valid_left_records);

      const int new_right_id = concurrency_manager_->build_index_from_records(
          "vsjoin_global_right_rebuilt",
          IndexType::IVF,
          dimension,
          global_ivf_params,
          valid_right_records);

      bool left_swapped = false;
      bool right_swapped = false;
      if (new_left_id >= 0) {
        left_swapped = concurrency_manager_->replace_index_by_id(global_left_id_, new_left_id);
      }
      if (new_right_id >= 0) {
        right_swapped = concurrency_manager_->replace_index_by_id(global_right_id_, new_right_id);
      }

      SAGEFLOW_LOG_INFO(
          "VSJOIN_REBUILD",
          "Global index rebuilt: {} unique left ({} valid), {} unique right ({} valid), swapped(L={}, R={})",
          unique_left_records.size(),
          valid_left_records.size(),
          unique_right_records.size(),
          valid_right_records.size(),
          left_swapped ? 1 : 0,
          right_swapped ? 1 : 0);
    } else {
      SAGEFLOW_LOG_INFO(
          "VSJOIN_REBUILD",
          "Global index rebuild tick: {} unique left ({} valid), {} unique right ({} valid) (skip swap: cm/global_id not ready)",
          unique_left_records.size(),
          valid_left_records.size(),
          unique_right_records.size(),
          valid_right_records.size());
    }
  }
}

}  // namespace sageFlow

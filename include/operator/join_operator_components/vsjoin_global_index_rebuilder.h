#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <mutex>

#include "concurrency/concurrency_manager.h"
#include "function/join_function.h"
#include "operator/utils/join_strategy_config.h"
#include "state/window_state.h"

namespace sageFlow {

/**
 * @brief Owns the VSJoin background global-index rebuild thread.
 *
 * The rebuilder periodically snapshots WindowState records, keeps the RecordView
 * snapshots alive while building new indexes, and atomically replaces the global
 * index controllers through ConcurrencyManager.
 */
class VSJoinGlobalIndexRebuilder {
 public:
  /**
   * @brief Construct a rebuilder over JoinOperator-owned runtime objects.
   *
   * WindowState and JoinFunction pointers are non-owning. The JoinOperator must
   * stop or destroy the rebuilder before those objects are destroyed.
   */
  VSJoinGlobalIndexRebuilder(
      const JoinStrategyConfig& strategy_config,
      const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
      JoinFunction* join_func,
      WindowState* left_state,
      WindowState* right_state,
      int global_left_id,
      int global_right_id,
      size_t parallelism);

  ~VSJoinGlobalIndexRebuilder();

  VSJoinGlobalIndexRebuilder(const VSJoinGlobalIndexRebuilder&) = delete;
  VSJoinGlobalIndexRebuilder& operator=(const VSJoinGlobalIndexRebuilder&) = delete;

  /**
   * @brief Start the rebuild thread at most once.
   */
  void start();

  /**
   * @brief Stop the rebuild thread and join it if running.
   */
  void stop();

 private:
  /**
   * @brief Compute the event-time lower bound for records retained in rebuild snapshots.
   */
  int64_t logicalWindowLowerBound(int64_t reference_timestamp) const;

  /**
   * @brief Thread body that snapshots states, rebuilds indexes, and swaps controllers.
   */
  void rebuildLoop();

  JoinStrategyConfig strategy_config_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  JoinFunction* join_func_;
  WindowState* left_state_;
  WindowState* right_state_;
  int global_left_id_;
  int global_right_id_;
  size_t parallelism_;

  std::once_flag started_;
  std::unique_ptr<std::thread> thread_;
  std::atomic<bool> running_{false};
  std::atomic<int64_t> interval_ms_{5000};
};

}  // namespace sageFlow

#pragma once
#include <atomic>
#include <cstdint>
#include <string>
#include <filesystem>
#include <fstream>

// Include general monitoring and metrics utilities
#include "utils/monitoring.h"

namespace sageFlow {

/**
 * @brief Join operator-specific metrics container
 * 
 * This structure holds all performance metrics for the join operator.
 * Metrics are accessible globally via the singleton instance() method.
 * 
 * Works in conjunction with:
 * - GPERFTOOLS (ENABLE_GPERFTOOLS): For detailed CPU/heap profiling
 * - Fine-grained metrics (SAGEFLOW_ENABLE_METRICS): For real-time statistics
 */
struct JoinMetrics {
  // Timing metrics (in nanoseconds)
  std::atomic<uint64_t> window_insert_ns{0};      ///< Time spent on window insert/expire operations
  std::atomic<uint64_t> index_insert_ns{0};       ///< Time spent on index insert/delete operations
  std::atomic<uint64_t> expire_ns{0};             ///< Time spent on expiration logic
  std::atomic<uint64_t> candidate_fetch_ns{0};    ///< Time spent fetching join candidates
  std::atomic<uint64_t> similarity_ns{0};         ///< Time spent on similarity computation
  std::atomic<uint64_t> join_function_ns{0};      ///< Time spent executing join function
  std::atomic<uint64_t> emit_ns{0};               ///< Time spent emitting results
  std::atomic<uint64_t> lock_wait_ns{0};          ///< Time spent waiting for locks
  
  // Counter metrics
  std::atomic<uint64_t> total_records_left{0};    ///< Total records processed on left side
  std::atomic<uint64_t> total_records_right{0};   ///< Total records processed on right side
  std::atomic<uint64_t> total_emits{0};           ///< Total results emitted
  std::atomic<uint64_t> window_records_left_completed{0};   ///< Records expired from left window
  std::atomic<uint64_t> window_records_right_completed{0};  ///< Records expired from right window
  
  // Apply processing metrics
  std::atomic<uint64_t> apply_processing_ns{0};   ///< Total time in apply() method
  std::atomic<uint64_t> apply_processing_count{0}; ///< Number of apply() calls
  
  // End-to-end latency metrics
  std::atomic<uint64_t> e2e_latency_ns{0};        ///< Cumulative end-to-end latency
  std::atomic<uint64_t> e2e_latency_count{0};     ///< Number of latency measurements

  /**
   * @brief Get singleton instance of JoinMetrics
   * @return Reference to the global JoinMetrics instance
   */
  static JoinMetrics& instance() {
    static JoinMetrics inst;
    return inst;
  }
  
  /**
   * @brief Reset all metrics to zero
   */
  void reset() {
    window_insert_ns = index_insert_ns = expire_ns = candidate_fetch_ns = similarity_ns = 
      join_function_ns = emit_ns = lock_wait_ns = 0;
    total_records_left = total_records_right = total_emits = 0;
    window_records_left_completed = window_records_right_completed = 0;
    apply_processing_ns = apply_processing_count = e2e_latency_ns = e2e_latency_count = 0;
  }
  
  /**
   * @brief Export metrics to TSV file
   * @param path Output file path
   */
  void dump_tsv(const std::string& path) {
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    std::ofstream ofs(path, std::ios::out | std::ios::trunc);
    if (!ofs) return;
    
    ofs << "metric\tvalue\n";
#define EMIT(m) ofs << #m "\t" << m.load() << "\n";
    EMIT(window_insert_ns) EMIT(index_insert_ns) EMIT(expire_ns) EMIT(candidate_fetch_ns)
    EMIT(similarity_ns) EMIT(join_function_ns) EMIT(emit_ns) EMIT(lock_wait_ns)
    EMIT(total_records_left) EMIT(total_records_right) EMIT(total_emits)
    EMIT(window_records_left_completed) EMIT(window_records_right_completed)
    EMIT(apply_processing_ns) EMIT(apply_processing_count)
    EMIT(e2e_latency_ns) EMIT(e2e_latency_count)
#undef EMIT
  }
};

// ============================================================================
// Join-Specific Metrics Helper Functions
// ============================================================================

/**
 * @brief Record lock wait time to the join operator's lock_wait_ns metric
 * @param start_time Start timestamp from metrics_timestamp()
 */
inline void metrics_record_lock_wait(uint64_t start_time) {
#ifdef SAGEFLOW_ENABLE_METRICS
  if (start_time > 0) {
    metrics_record_elapsed(JoinMetrics::instance().lock_wait_ns, start_time);
  }
#else
  (void)start_time;
#endif
}

/**
 * @brief Record lock wait time to both lock_wait_ns and another metric
 * @param start_time Start timestamp from metrics_timestamp()
 * @param additional_metric Additional metric to update (e.g., window_insert_ns)
 */
inline void metrics_record_lock_wait_dual(uint64_t start_time, std::atomic<uint64_t>& additional_metric) {
#ifdef SAGEFLOW_ENABLE_METRICS
  if (start_time > 0) {
    metrics_record_elapsed_dual(JoinMetrics::instance().lock_wait_ns, additional_metric, start_time);
  }
#else
  (void)start_time;
  (void)additional_metric;
#endif
}

/**
 * @brief Record end-to-end latency for join operator
 * @param start_time Start timestamp from metrics_timestamp()
 */
inline void metrics_record_e2e_latency(uint64_t start_time) {
#ifdef SAGEFLOW_ENABLE_METRICS
  if (start_time > 0) {
    uint64_t latency = ScopedAccumulateAtomic::now_ns() - start_time;
    JoinMetrics::instance().e2e_latency_ns.fetch_add(latency, std::memory_order_relaxed);
    JoinMetrics::instance().e2e_latency_count.fetch_add(1, std::memory_order_relaxed);
  }
#else
  (void)start_time;
#endif
}

} // namespace sageFlow

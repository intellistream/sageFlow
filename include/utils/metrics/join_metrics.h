#pragma once
#include <atomic>
#include <cstdint>
#include <string>
#include <filesystem>
#include <fstream>
#include <array>
#include <vector>
#include <algorithm>

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

  // Stage counters
  std::atomic<uint64_t> window_insert_count{0};   ///< Number of window insert operations
  std::atomic<uint64_t> index_op_count{0};        ///< Number of index insert/delete operations
  std::atomic<uint64_t> expire_count{0};          ///< Number of expired records processed
  std::atomic<uint64_t> candidate_fetch_count{0}; ///< Number of candidate fetch operations
  std::atomic<uint64_t> similarity_count{0};      ///< Number of similarity comparisons
  std::atomic<uint64_t> join_function_count{0};   ///< Number of join function executions
  std::atomic<uint64_t> emit_count{0};            ///< Number of emit operations
  std::atomic<uint64_t> lock_wait_count{0};       ///< Number of lock wait events
  
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

  // End-to-end latency samples (ring buffer) for percentile estimation
  static constexpr size_t kE2ELatencySampleSize = 8192;
  std::array<std::atomic<uint64_t>, kE2ELatencySampleSize> e2e_latency_samples_ns{};
  std::atomic<uint64_t> e2e_latency_sample_index{0};

  // QIQ 策略三阶段统计（包括锁等待时间）
  std::atomic<uint64_t> qiq_q1_ns{0};             ///< Query1 阶段总耗时（含锁等待）
  std::atomic<uint64_t> qiq_q1_count{0};          ///< Query1 调用次数
  std::atomic<uint64_t> qiq_insert_ns{0};         ///< Insert 阶段总耗时（含锁等待）
  std::atomic<uint64_t> qiq_insert_count{0};      ///< Insert 调用次数
  std::atomic<uint64_t> qiq_q2_ns{0};             ///< Query2 阶段总耗时（含锁等待）
  std::atomic<uint64_t> qiq_q2_count{0};          ///< Query2 调用次数

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
    window_insert_count = index_op_count = expire_count = candidate_fetch_count = similarity_count =
      join_function_count = emit_count = lock_wait_count = 0;
    total_records_left = total_records_right = total_emits = 0;
    window_records_left_completed = window_records_right_completed = 0;
    apply_processing_ns = apply_processing_count = e2e_latency_ns = e2e_latency_count = 0;
    qiq_q1_ns = qiq_q1_count = qiq_insert_ns = qiq_insert_count = qiq_q2_ns = qiq_q2_count = 0;
    e2e_latency_sample_index = 0;
    for (auto& v : e2e_latency_samples_ns) {
      v.store(0, std::memory_order_relaxed);
    }
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
    EMIT(window_insert_count) EMIT(index_op_count) EMIT(expire_count) EMIT(candidate_fetch_count)
    EMIT(similarity_count) EMIT(join_function_count) EMIT(emit_count) EMIT(lock_wait_count)
    EMIT(total_records_left) EMIT(total_records_right) EMIT(total_emits)
    EMIT(window_records_left_completed) EMIT(window_records_right_completed)
    EMIT(apply_processing_ns) EMIT(apply_processing_count)
    EMIT(e2e_latency_ns) EMIT(e2e_latency_count)
    EMIT(qiq_q1_ns) EMIT(qiq_q1_count) EMIT(qiq_insert_ns) EMIT(qiq_insert_count)
    EMIT(qiq_q2_ns) EMIT(qiq_q2_count)
#undef EMIT
  }

  /**
   * @brief Record a single end-to-end latency sample (ns) into ring buffer
   */
  void recordE2ELatencySample(uint64_t latency_ns) {
    uint64_t idx = e2e_latency_sample_index.fetch_add(1, std::memory_order_relaxed);
    e2e_latency_samples_ns[idx % kE2ELatencySampleSize].store(latency_ns, std::memory_order_relaxed);
  }

  /**
   * @brief Get a copy of latency samples for percentile calculation
   */
  [[nodiscard]] std::vector<uint64_t> getE2ELatencySamples() const {
    const uint64_t count = e2e_latency_count.load(std::memory_order_relaxed);
    if (count == 0) {
      return {};
    }
    const size_t sample_count = static_cast<size_t>(
        std::min<uint64_t>(count, kE2ELatencySampleSize));
    std::vector<uint64_t> samples;
    samples.reserve(sample_count);
    if (count < kE2ELatencySampleSize) {
      for (size_t i = 0; i < sample_count; ++i) {
        samples.push_back(e2e_latency_samples_ns[i].load(std::memory_order_relaxed));
      }
    } else {
      for (size_t i = 0; i < kE2ELatencySampleSize; ++i) {
        samples.push_back(e2e_latency_samples_ns[i].load(std::memory_order_relaxed));
      }
    }
    return samples;
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
    metrics_increment(JoinMetrics::instance().lock_wait_count);
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
    metrics_increment(JoinMetrics::instance().lock_wait_count);
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
    JoinMetrics::instance().recordE2ELatencySample(latency);
  }
#else
  (void)start_time;
#endif
}

} // namespace sageFlow

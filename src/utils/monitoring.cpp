#include <utils/monitoring.h>
#include <fstream>
#ifdef ENABLE_GPERFTOOLS
#include <gperftools/profiler.h>
#endif
#include <utility>
#include "utils/logger.h"

namespace sageFlow {

PerformanceMonitor::PerformanceMonitor(std::string profile_output)
    : profile_output_file_(std::move(profile_output)), profiling_(false) {}

PerformanceMonitor::~PerformanceMonitor() {
  if (profiling_) {
    StopProfiling();
  }
}

void PerformanceMonitor::StartProfiling() {
#ifdef ENABLE_GPERFTOOLS
  if (!profiling_) {
    ProfilerStart(profile_output_file_.c_str());
    profiling_ = true;
    SAGEFLOW_LOG_INFO("MONITOR", "profiling_started file={} ", profile_output_file_);
  } else {
    SAGEFLOW_LOG_WARN("MONITOR", "profiling_already_running file={} ", profile_output_file_);
  }
#else
  SAGEFLOW_LOG_ERROR("MONITOR", "Profiling not available: gperftools not found.");
#endif
}

void PerformanceMonitor::StopProfiling() {
#ifdef ENABLE_GPERFTOOLS
  if (profiling_) {
    ProfilerStop();
    profiling_ = false;
    SAGEFLOW_LOG_INFO("MONITOR", "profiling_stopped file={} ", profile_output_file_);
  } else {
    SAGEFLOW_LOG_WARN("MONITOR", "profiling_not_running file={} ", profile_output_file_);
  }
#else
  SAGEFLOW_LOG_ERROR("MONITOR", "Profiling not available: gperftools not found.");
#endif
}

void PerformanceMonitor::StartTimer() {
  start_time_ = std::chrono::high_resolution_clock::now();
  SAGEFLOW_LOG_INFO("MONITOR", "timer_started");
}

void PerformanceMonitor::StopTimer(const std::string &task_name) {
  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_).count();
  SAGEFLOW_LOG_INFO("MONITOR", "task_done name={} duration_ms={} ", task_name, duration);
}

}  // namespace sageFlow

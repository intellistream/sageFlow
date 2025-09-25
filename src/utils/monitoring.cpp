#include <utils/monitoring.h>
#include <fstream>
#ifdef ENABLE_GPERFTOOLS
#include <gperftools/profiler.h>
#endif
#include <utility>
#include "utils/logger.h"

namespace candy {

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
  CANDY_LOG_INFO("MONITOR", "profiling_started file={} ", profile_output_file_);
  } else {
  CANDY_LOG_WARN("MONITOR", "profiling_already_running file={} ", profile_output_file_);
  }
#else
  std::cerr << "Profiling not available: gperftools not found." << '\n';
#endif
}

void PerformanceMonitor::StopProfiling() {
#ifdef ENABLE_GPERFTOOLS
  if (profiling_) {
    ProfilerStop();
    profiling_ = false;
  CANDY_LOG_INFO("MONITOR", "profiling_stopped file={} ", profile_output_file_);
  } else {
  CANDY_LOG_WARN("MONITOR", "profiling_not_running file={} ", profile_output_file_);
  }
#else
  std::cerr << "Profiling not available: gperftools not found." << '\n';
#endif
}

void PerformanceMonitor::StartTimer() {
  start_time_ = std::chrono::high_resolution_clock::now();
  CANDY_LOG_INFO("MONITOR", "timer_started");
}

void PerformanceMonitor::StopTimer(const std::string &task_name) {
  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_).count();
  CANDY_LOG_INFO("MONITOR", "task_done name={} duration_ms={} ", task_name, duration);
}

}  // namespace candy

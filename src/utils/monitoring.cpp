#include <utils/monitoring.h>
#include <fstream>
#ifdef ENABLE_GPERFTOOLS
#include <gperftools/profiler.h>
#endif
#include <utility>

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
    std::cout << "Profiling started: " << profile_output_file_ << '\n';
  } else {
    std::cerr << "Profiling is already running." << '\n';
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
    std::cout << "Profiling stopped and saved to: " << profile_output_file_ << '\n';
  } else {
    std::cerr << "Profiling is not running." << '\n';
  }
#else
  std::cerr << "Profiling not available: gperftools not found." << '\n';
#endif
}

void PerformanceMonitor::StartTimer() {
  start_time_ = std::chrono::high_resolution_clock::now();
  std::cout << "Timer started." << '\n';
}

void PerformanceMonitor::StopTimer(const std::string &task_name) {
  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_).count();
  std::cout << "Task [" << task_name << "] completed in " << duration << " ms." << '\n';
}

}  // namespace candy

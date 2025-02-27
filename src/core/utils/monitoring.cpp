#include <core/utils/monitoring.h>

#include <utility>

namespace candy {

PerformanceMonitor::PerformanceMonitor(std::string profileOutput)
    : profile_output_file_(std::move(profileOutput)), profiling_(false) {}

PerformanceMonitor::~PerformanceMonitor() {
  if (profiling_) {
    StopProfiling();
  }
}

void PerformanceMonitor::StartProfiling() {
  if (!profiling_) {
    ProfilerStart(profile_output_file_.c_str());
    profiling_ = true;
    std::cout << "Profiling started: " << profile_output_file_ << '\n';
  } else {
    std::cerr << "Profiling is already running." << '\n';
  }
}

void PerformanceMonitor::StopProfiling() {
  if (profiling_) {
    ProfilerStop();
    profiling_ = false;
    std::cout << "Profiling stopped and saved to: " << profile_output_file_ << '\n';
  } else {
    std::cerr << "Profiling is not running." << '\n';
  }
}

void PerformanceMonitor::StartTimer() {
  start_time_ = std::chrono::high_resolution_clock::now();
  std::cout << "Timer started." << '\n';
}

void PerformanceMonitor::StopTimer(const std::string &taskName) {
  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_).count();
  std::cout << "Task [" << taskName << "] completed in " << duration << " ms." << '\n';
}

}  // namespace candy




#include <chrono>

#include <iostream>
#include <string>

namespace candy {

class PerformanceMonitor {
public:
  explicit PerformanceMonitor(std::string profileOutput = "profile.prof");
  ~PerformanceMonitor();

  // Start profiling
  void StartProfiling();

  // Stop profiling and save the results
  void StopProfiling();

  // Start the timer for measuring elapsed time
  void StartTimer();

  // Stop the timer and print elapsed time
  void StopTimer(const std::string &taskName);

private:
  std::string profile_output_file_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
  bool profiling_;
};

} // namespace candy

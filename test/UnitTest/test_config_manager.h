#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include "toml++/toml.hpp"

namespace candy {
namespace test {

struct TestCaseConfig {
  std::string name;
  std::string method;
  double similarity_threshold;
  int vector_dim;
  int records_count;
  int64_t window_time_ms;
  int positive_pairs;
  int near_threshold_pairs = 0;
  int negative_pairs;
  int random_tail = 0;
  uint32_t seed;
};

struct PerformanceTestConfig {
  std::string name;
  std::vector<std::string> methods;
  double similarity_threshold;
  int vector_dim;
  int records_count;
  std::vector<int> parallelism;
  int64_t window_time_ms;
  uint32_t seed;
};

struct PipelineConfig {
  int parallelism_source;
  int parallelism_join;
  int parallelism_sink;
  int records_per_source;
  int vector_dim;
  double similarity_threshold;
  std::string join_method;
  
  struct Window {
    int64_t time_ms;
    int64_t trigger_interval_ms;
  } window;
  
  struct IVFConfig {
    int nlist;
    int nprobes;
    double rebuild_threshold;
  } ivf;
  
  struct DataPattern {
    int positive_pairs;
    int near_threshold_pairs;
    int negative_pairs;
    int random_tail;
    uint32_t seed;
  } data_pattern;
};

class TestConfigManager {
public:
  static bool loadTestCases(const std::string& config_path, std::vector<TestCaseConfig>& test_cases);
  static bool loadPerformanceTests(const std::string& config_path, std::vector<PerformanceTestConfig>& perf_tests);
  static bool loadPipelineConfig(const std::string& config_path, PipelineConfig& pipeline_config);
  
private:
  static void extractTestCase(const toml::node& case_node, TestCaseConfig& config);
  static void extractPerfTest(const toml::node& perf_node, PerformanceTestConfig& config);
  static void extractPipelineConfig(const toml::table& root, PipelineConfig& config);
};

} // namespace test
} // namespace candy
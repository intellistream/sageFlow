#include "test_utils/test_config_manager.h"
#include <iostream>
#include <filesystem>

namespace candy { namespace test {

namespace {
  template <class T>
  T require_value(const toml::table& tbl, std::string_view key) {
    if (auto node = tbl.get(key)) { if (auto v = node->template value<T>()) return *v; }
    throw std::runtime_error(std::string("Missing or invalid key: ") + std::string(key));
  }
  template <class T>
  T optional_value(const toml::table& tbl, std::string_view key, T def_val) {
    if (auto node = tbl.get(key)) { if (auto v = node->template value<T>()) return *v; }
    return def_val;
  }
}

static toml::table parse_file_with_fallback(const std::string& path) {
  std::vector<std::string> candidates{path};
#ifdef PROJECT_DIR
  try { candidates.push_back((std::filesystem::path(PROJECT_DIR)/path).string()); } catch(...) {}
#endif
  for (const auto& p : candidates) {
    try { auto result = toml::parse_file(p); const toml::table& root = result; return root; } catch(const std::exception&) {}
  }
  throw std::runtime_error(std::string("Failed to open config file: ")+path);
}

bool TestConfigManager::loadTestCases(const std::string& config_path, std::vector<TestCaseConfig>& test_cases) {
  try {
    const toml::table& root = parse_file_with_fallback(config_path);
    if (auto* arr = root["test_case"].as_array()) {
      for (auto& node : *arr) if (auto* t = node.as_table()) { TestCaseConfig c{}; extractTestCase(*t, c); test_cases.push_back(std::move(c)); }
    }
    return true;
  } catch (const std::exception& e) { std::cerr << "Failed to load test cases from " << config_path << ": " << e.what() << std::endl; return false; }
}

bool TestConfigManager::loadPerformanceTests(const std::string& config_path, std::vector<PerformanceTestConfig>& perf_tests) {
  try {
    const toml::table& root = parse_file_with_fallback(config_path);
    if (auto* arr = root["performance_test"].as_array()) {
      for (auto& node : *arr) if (auto* t = node.as_table()) { PerformanceTestConfig c{}; extractPerfTest(*t, c); perf_tests.push_back(std::move(c)); }
    }
    return true;
  } catch (const std::exception& e) { std::cerr << "Failed to load performance tests from " << config_path << ": " << e.what() << std::endl; return false; }
}

bool TestConfigManager::loadPipelineConfig(const std::string& config_path, PipelineConfig& pipeline_config) {
  try { const toml::table& root = parse_file_with_fallback(config_path); extractPipelineConfig(root, pipeline_config); return true; }
  catch (const std::exception& e) { std::cerr << "Failed to load pipeline config from " << config_path << ": " << e.what() << std::endl; return false; }
}

void TestConfigManager::extractTestCase(const toml::node& case_node, TestCaseConfig& config) {
  const auto* tbl = case_node.as_table(); if (!tbl) throw std::runtime_error("test_case entry is not a table");
  config.name = require_value<std::string>(*tbl, "name");
  config.method = require_value<std::string>(*tbl, "method");
  config.similarity_threshold = require_value<double>(*tbl, "similarity_threshold");
  config.vector_dim = require_value<int>(*tbl, "vector_dim");
  config.records_count = require_value<int>(*tbl, "records_count");
  config.window_time_ms = require_value<int64_t>(*tbl, "window_time_ms");
  config.positive_pairs = require_value<int>(*tbl, "positive_pairs");
  config.negative_pairs = require_value<int>(*tbl, "negative_pairs");
  config.seed = require_value<uint32_t>(*tbl, "seed");
  config.near_threshold_pairs = optional_value<int>(*tbl, "near_threshold_pairs", config.near_threshold_pairs);
  config.random_tail = optional_value<int>(*tbl, "random_tail", config.random_tail);
}

void TestConfigManager::extractPerfTest(const toml::node& perf_node, PerformanceTestConfig& config) {
  const auto* tbl = perf_node.as_table(); if (!tbl) throw std::runtime_error("performance_test entry is not a table");
  try { config.name = require_value<std::string>(*tbl, "name"); } catch(...) { config.name = "perf_case"; }
  if (auto* arr = tbl->get_as<toml::array>("methods")) { config.methods.clear(); for (auto& n : *arr) if (auto v = n.value<std::string>()) config.methods.emplace_back(*v); }
  else { throw std::runtime_error("performance_test.methods missing or not array"); }
  config.similarity_threshold = require_value<double>(*tbl, "similarity_threshold");
  config.vector_dim = require_value<int>(*tbl, "vector_dim");
  // Note: time_interval is handled in dynamic perf config elsewhere; no field here.
  try { config.records_count = require_value<int>(*tbl, "records_count"); } catch(...) { config.records_count = 0; }
  if (auto* arr = tbl->get_as<toml::array>("sizes")) { config.sizes.clear(); for (auto &n : *arr) if (auto v = n.value<int>()) config.sizes.push_back(*v); }
  if (config.sizes.empty() && config.records_count <= 0) throw std::runtime_error("performance_test missing both sizes[] and valid records_count");
  if (auto* arr = tbl->get_as<toml::array>("parallelism")) { config.parallelism.clear(); for (auto& n : *arr) if (auto v = n.value<int>()) config.parallelism.emplace_back(*v); }
  else { throw std::runtime_error("performance_test.parallelism missing or not array"); }
  config.window_time_ms = require_value<int64_t>(*tbl, "window_time_ms");
  config.window_trigger_ms = optional_value<int64_t>(*tbl, "window_trigger_ms", config.window_trigger_ms);
  config.seed = require_value<uint32_t>(*tbl, "seed");
  std::cerr << "[perf_config] name=" << config.name << " sizes="; if (!config.sizes.empty()) { for (size_t i=0;i<config.sizes.size();++i){ if(i) std::cerr<<','; std::cerr<<config.sizes[i]; } }
  std::cerr << " records_count=" << config.records_count << " parallelism="; for (size_t i=0;i<config.parallelism.size();++i){ if(i) std::cerr<<','; std::cerr<<config.parallelism[i]; }
  std::cerr << " threshold=" << config.similarity_threshold << " win_ms=" << config.window_time_ms << " trig_ms=" << config.window_trigger_ms << std::endl;
}

void TestConfigManager::extractPipelineConfig(const toml::table& root, PipelineConfig& config) {
  const auto* pipeline = root["pipeline"].as_table(); if (!pipeline) throw std::runtime_error("[pipeline] section missing or invalid");
  config.parallelism_source = require_value<int>(*pipeline, "parallelism_source");
  config.parallelism_join = require_value<int>(*pipeline, "parallelism_join");
  config.parallelism_sink = require_value<int>(*pipeline, "parallelism_sink");
  config.records_per_source = require_value<int>(*pipeline, "records_per_source");
  config.vector_dim = require_value<int>(*pipeline, "vector_dim");
  config.similarity_threshold = require_value<double>(*pipeline, "similarity_threshold");
  config.join_method = require_value<std::string>(*pipeline, "join_method");
  const auto* window = root["window"].as_table(); if (!window) throw std::runtime_error("[window] section missing or invalid");
  config.window.time_ms = require_value<int64_t>(*window, "time_ms");
  config.window.trigger_interval_ms = require_value<int64_t>(*window, "trigger_interval_ms");
  const auto* index = root["index"].as_table(); if (!index) throw std::runtime_error("[index] section missing or invalid");
  const auto* ivf = (*index)["ivf"].as_table(); if (!ivf) throw std::runtime_error("[index.ivf] section missing or invalid");
  config.ivf.nlist = require_value<int>(*ivf, "nlist");
  config.ivf.nprobes = require_value<int>(*ivf, "nprobes");
  config.ivf.rebuild_threshold = require_value<double>(*ivf, "rebuild_threshold");
  const auto* data = root["data"].as_table(); if (!data) throw std::runtime_error("[data] section missing or invalid");
  const auto* pattern = (*data)["pattern"].as_table(); if (!pattern) throw std::runtime_error("[data.pattern] section missing or invalid");
  config.data_pattern.positive_pairs = require_value<int>(*pattern, "positive_pairs");
  config.data_pattern.near_threshold_pairs = require_value<int>(*pattern, "near_threshold_pairs");
  config.data_pattern.negative_pairs = require_value<int>(*pattern, "negative_pairs");
  config.data_pattern.random_tail = require_value<int>(*pattern, "random_tail");
  config.data_pattern.seed = require_value<uint32_t>(*pattern, "seed");
}

}} // namespace

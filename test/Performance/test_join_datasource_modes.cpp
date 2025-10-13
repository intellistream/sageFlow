#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <filesystem>
#include <atomic>
#include "operator/join_operator.h"
#include "stream/stream_environment.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "function/sink_function.h"
#include "function/join_function.h"
#include "test_utils/test_data_generator.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"
#include "execution/collector.h"
#include "utils/logger.h"
#include "test_utils/dynamic_config.h"
#include "utils/log_config.h"
#include "test_utils/join_test_helper.h"
#include "test_utils/data_source/data_source_factory.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/data_writer/fvecs_writer.h"
#include "test_utils/data_writer/json_writer.h"
#include <fstream>
#include <set>
#include <sstream>
#include <algorithm>
#include <cmath>

namespace sageFlow {
namespace test {

// TestVectorStreamSource for feeding records into the pipeline
class TestVectorStreamSource : public DataStreamSource {
 public:
  explicit TestVectorStreamSource(std::string name, std::vector<std::unique_ptr<VectorRecord>> records)
      : DataStreamSource(std::move(name), DataStreamSourceType::None), records_(std::move(records)) {}
  void Init() override { idx_=0; }
  auto Next() -> std::unique_ptr<VectorRecord> override {
    if (idx_ >= records_.size()) return nullptr;
    return std::move(records_[idx_++]);
  }
 private:
  std::vector<std::unique_ptr<VectorRecord>> records_;
  size_t idx_{0};
};

// Configuration structure for data source modes tests
struct DataSourceModeConfig {
  std::string name;
  std::string mode;  // "generate_save_load", "direct_load", "generate_direct_use"
  std::vector<std::string> methods;
  std::vector<int> sizes;
  std::vector<int> parallelism;
  double threshold{0.8};
  std::vector<uint64_t> win_ms_list{10000};
  uint64_t trig_ms{50};
  int vector_dim{128};
  int64_t time_interval_ms{10};
  uint32_t seed{42};
  
  // Data source config
  std::string data_source_type;  // "random", "dataset", "json"
  std::string data_source_file_path;
  int data_source_expected_dim{128};
  bool data_source_loop{true};
  
  // Storage config (for generate_save_load mode)
  std::string storage_format;  // "fvecs", "json"
  std::string storage_file_path;
};

// Load configuration from TOML file
static std::vector<DataSourceModeConfig> loadDataSourceModeConfigs() {
  std::vector<DataSourceModeConfig> configs;
  std::vector<DynamicConfig> perf_configs;
  
  if (!DynamicConfigManager::loadConfigs("config/perf_join_datasource_modes.toml", "performance_test", perf_configs)) {
    SAGEFLOW_LOG_WARN("TEST", "Failed to load config from perf_join_datasource_modes.toml");
    return configs;
  }
  
  // Set global log level if specified
  DynamicConfig global_config;
  if (DynamicConfigManager::loadConfig("config/perf_join_datasource_modes.toml", "", global_config)) {
    auto log_level = global_config.get<std::string>("log.level", "info");
    SAGEFLOW_LOG_INFO("TEST", "Setting log level to: {}", log_level);
    sageFlow::init_log_level(log_level);
  }
  
  for (const auto& config : perf_configs) {
    DataSourceModeConfig mode_config;
    mode_config.name = config.get<std::string>("name", "unnamed_test");
    mode_config.mode = config.get<std::string>("mode", "generate_direct_use");
    mode_config.methods = config.get<std::vector<std::string>>("methods", std::vector<std::string>{"bruteforce_eager"});
    
    auto sizes = config.get<std::vector<int>>("sizes", std::vector<int>{});
    if (!sizes.empty()) {
      mode_config.sizes = sizes;
    } else {
      auto records_count = config.get<int>("records_count", 1000);
      mode_config.sizes = {records_count};
    }
    
    mode_config.parallelism = config.get<std::vector<int>>("parallelism", std::vector<int>{1});
    mode_config.threshold = config.get<double>("similarity_threshold", 0.8);
    
    auto win_list = config.get<std::vector<int>>("window_time_ms", std::vector<int>{});
    if (!win_list.empty()) {
      mode_config.win_ms_list.clear();
      for (int v : win_list) mode_config.win_ms_list.push_back(static_cast<uint64_t>(v));
    } else {
      int win_single = config.get<int>("window_time_ms", 10000);
      mode_config.win_ms_list = {static_cast<uint64_t>(win_single)};
    }
    
    mode_config.trig_ms = config.get<int>("window_trigger_ms", 50);
    mode_config.vector_dim = config.get<int>("vector_dim", 128);
    mode_config.time_interval_ms = config.get<int>("time_interval", 10);
    mode_config.seed = config.get<int>("seed", 42);
    
    // Data source configuration
  auto ds_type = config.get<std::string>("data_source.type", "random");
  mode_config.data_source_type = ds_type;
  mode_config.data_source_file_path = DynamicConfigManager::resolveProjectRelativePath(
    config.get<std::string>("data_source.file_path", ""));

  if (ds_type == "dataset") {
      mode_config.data_source_expected_dim = config.get<int>("data_source.expected_dim", 128);
      int loop_val = config.get<int>("data_source.loop", 1);
      mode_config.data_source_loop = (loop_val != 0);
    }
    
    // Storage configuration (for generate_save_load mode)
    if (mode_config.mode == "generate_save_load") {
    mode_config.storage_format = config.get<std::string>("storage.format", "fvecs");
    mode_config.storage_file_path = DynamicConfigManager::resolveProjectRelativePath(
      config.get<std::string>("storage.file_path", "test/data/temp_generated.fvecs"));
    }
    
    configs.push_back(mode_config);
    
    SAGEFLOW_LOG_INFO("TEST", "[CONFIG] Loaded test: name={} mode={} methods={} sizes={} vector_dim={}", 
                     mode_config.name, mode_config.mode, mode_config.methods.size(), mode_config.sizes.size(), mode_config.vector_dim);
  }
  
  return configs;
}

// Compute expected matches using L2 distance and similarity threshold
static inline double l2_distance(const std::vector<float>& a, const std::vector<float>& b) {
  double acc = 0.0;
  const size_t n = std::min(a.size(), b.size());
  for (size_t i = 0; i < n; ++i) {
    const double d = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    acc += d * d;
  }
  return std::sqrt(acc);
}

static std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash>
  computeExpectedPairsByTraversal(
    const std::vector<std::unique_ptr<VectorRecord>>& left_records,
    const std::vector<std::unique_ptr<VectorRecord>>& right_records,
    double similarity_threshold,
    uint64_t window_ms,
    double alpha = 0.1,
    uint64_t modulo_base = 1000000ULL) {
  std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash> expected;
  expected.reserve(left_records.size());

  const int64_t w = static_cast<int64_t>(window_ms);
  size_t j_low = 0;
  size_t j_high = 0;

  const size_t R = right_records.size();
  for (const auto& l : left_records) {
    if (!l) continue;
    const int64_t tl = l->timestamp_;

    while (j_low < R) {
      const auto& rr = right_records[j_low];
      if (!rr) { ++j_low; continue; }
      if (rr->timestamp_ >= tl - w) break;
      ++j_low;
    }

    if (j_high < j_low) j_high = j_low;

    while (j_high < R) {
      const auto& rr = right_records[j_high];
      if (!rr) { ++j_high; continue; }
      if (rr->timestamp_ > tl + w) break;
      ++j_high;
    }

    const auto lv = extractFloatVector(*l);
    for (size_t j = j_low; j < j_high; ++j) {
      const auto& r = right_records[j];
      if (!r) continue;
      const auto rv = extractFloatVector(*r);
      const double dist = l2_distance(lv, rv);
      const double sim = std::exp(-alpha * dist);
      if (sim >= similarity_threshold) {
        expected.insert({l->uid_, r->uid_ % modulo_base});
      }
    }
  }

  return expected;
}

// Test class for data source modes
class JoinDataSourceModesTest : public ::testing::TestWithParam<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>> {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(std::make_shared<StorageManager>());
  }

  void TearDown() override {
    std::filesystem::create_directories("build/metrics");
    std::string metrics_path = "build/metrics/join_datasource_modes_" + 
          std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".tsv";
    JoinMetrics::instance().dump_tsv(metrics_path);
    SAGEFLOW_LOG_INFO("TEST", "Performance metrics saved to {}", metrics_path);
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_P(JoinDataSourceModesTest, DataSourceModePerformance) {
  auto [mode_config, method, data_size, parallelism, win_ms] = GetParam();
  
  SAGEFLOW_LOG_INFO("TEST", "===== Running test: {} mode={} method={} size={} parallelism={} win_ms={} =====",
                   mode_config.name, mode_config.mode, method, data_size, parallelism, win_ms);
  
  // Prepare data based on mode
  std::vector<std::unique_ptr<VectorRecord>> base_records;
  
  if (mode_config.mode == "generate_save_load") {
    // Mode 1: Generate -> Save -> Load
    SAGEFLOW_LOG_INFO("TEST", "[MODE1] Generate-Save-Load: format={} path={}", 
                      mode_config.storage_format, mode_config.storage_file_path);
    
    // Check if file already exists
    bool file_exists = std::filesystem::exists(mode_config.storage_file_path);
    
    if (!file_exists) {
      // Generate data
      SAGEFLOW_LOG_INFO("TEST", "[MODE1] File doesn't exist, generating data");
      TestDataGenerator::Config gen_config;
      gen_config.vector_dim = mode_config.vector_dim;
      gen_config.similarity_threshold = mode_config.threshold;
      gen_config.seed = mode_config.seed;
      gen_config.base_timestamp = 1000000;
      gen_config.time_interval = mode_config.time_interval_ms;
      
      int target_pos = static_cast<int>(data_size * 0.10);
      int target_neg = static_cast<int>(data_size * 0.60);
      int pos_pairs = target_pos / 2;
      int neg_pairs = target_neg / 2;
      int used = 2*pos_pairs + 2*neg_pairs;
      int tail = std::max(0, data_size - used);
      gen_config.positive_pairs = pos_pairs;
      gen_config.near_threshold_pairs = 0;
      gen_config.negative_pairs = neg_pairs;
      gen_config.random_tail = tail;
      
      TestDataGenerator generator(gen_config);
      auto [records, _] = generator.generateData();
      
      // Save to file
      std::filesystem::create_directories(std::filesystem::path(mode_config.storage_file_path).parent_path());
      std::shared_ptr<DataWriterBase> writer;
      if (mode_config.storage_format == "fvecs") {
        writer = std::make_shared<FvecsWriter>();
      } else {
        writer = std::make_shared<JsonWriter>();
      }
      generator.saveGeneratedVectors(mode_config.storage_file_path, writer);
      SAGEFLOW_LOG_INFO("TEST", "[MODE1] Saved {} records to {}", records.size(), mode_config.storage_file_path);
    } else {
      SAGEFLOW_LOG_INFO("TEST", "[MODE1] File exists, skipping generation");
    }
    
    // Load from file
    DatasetDataSource::Config ds_config;
    ds_config.file_path = mode_config.storage_file_path;
    ds_config.expected_dim = mode_config.vector_dim;
    ds_config.loop = true;
    
    DatasetDataSource data_source(ds_config);
    base_records.reserve(data_size);
    int64_t base_ts = 1000000;
    uint64_t uid = 1;
    while (data_source.hasMore() && base_records.size() < static_cast<size_t>(data_size)) {
      auto vec = data_source.getNextVector();
      auto record = createVectorRecord(uid++, base_ts, vec);
      base_ts += mode_config.time_interval_ms;
      base_records.push_back(std::move(record));
    }
    SAGEFLOW_LOG_INFO("TEST", "[MODE1] Loaded {} records from file", base_records.size());
    
  } else if (mode_config.mode == "direct_load") {
    // Mode 2: Direct Load from existing dataset
    SAGEFLOW_LOG_INFO("TEST", "[MODE2] Direct-Load from: {}", mode_config.data_source_file_path);
    
    DatasetDataSource::Config ds_config;
    ds_config.file_path = mode_config.data_source_file_path;
    ds_config.expected_dim = mode_config.data_source_expected_dim;
    ds_config.loop = mode_config.data_source_loop;
    
    DatasetDataSource data_source(ds_config);
    base_records.reserve(data_size);
    int64_t base_ts = 1000000;
    uint64_t uid = 1;
    while (data_source.hasMore() && base_records.size() < static_cast<size_t>(data_size)) {
      auto vec = data_source.getNextVector();
      auto record = createVectorRecord(uid++, base_ts, vec);
      base_ts += mode_config.time_interval_ms;
      base_records.push_back(std::move(record));
    }
    SAGEFLOW_LOG_INFO("TEST", "[MODE2] Loaded {} records directly from dataset", base_records.size());
    
  } else {
    // Mode 3: Generate and use directly (no file I/O)
    SAGEFLOW_LOG_INFO("TEST", "[MODE3] Generate-Direct-Use (no file I/O)");
    
    TestDataGenerator::Config gen_config;
    gen_config.vector_dim = mode_config.vector_dim;
    gen_config.similarity_threshold = mode_config.threshold;
    gen_config.seed = mode_config.seed;
    gen_config.base_timestamp = 1000000;
    gen_config.time_interval = mode_config.time_interval_ms;
    
    int target_pos = static_cast<int>(data_size * 0.10);
    int target_neg = static_cast<int>(data_size * 0.60);
    int pos_pairs = target_pos / 2;
    int neg_pairs = target_neg / 2;
    int used = 2*pos_pairs + 2*neg_pairs;
    int tail = std::max(0, data_size - used);
    gen_config.positive_pairs = pos_pairs;
    gen_config.near_threshold_pairs = 0;
    gen_config.negative_pairs = neg_pairs;
    gen_config.random_tail = tail;
    
    TestDataGenerator generator(gen_config);
    auto [records, _] = generator.generateData();
    base_records = std::move(records);
    SAGEFLOW_LOG_INFO("TEST", "[MODE3] Generated {} records directly", base_records.size());
  }
  
  // Split into left and right streams using JoinTestHelper (already refactored pattern)
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  left_records.reserve(base_records.size());
  for (auto& r : base_records) {
    left_records.push_back(std::move(r));
  }
  
  std::vector<std::unique_ptr<VectorRecord>> right_records;
  right_records.reserve(left_records.size());
  constexpr uint64_t kRightUidOffset = 500000;
  for (auto& lr : left_records) {
    right_records.push_back(std::make_unique<VectorRecord>(lr->uid_ + kRightUidOffset, lr->timestamp_, lr->data_));
  }
  
  const size_t expected_left = left_records.size();
  const size_t expected_right = right_records.size();
  
  // Compute expected matches
  auto expected_matches = computeExpectedPairsByTraversal(left_records, right_records, mode_config.threshold, win_ms);
  
  // Create stream sources
  auto left_source = std::make_shared<TestVectorStreamSource>("DataModeLeft", std::move(left_records));
  auto right_source = std::make_shared<TestVectorStreamSource>("DataModeRight", std::move(right_records));
  
  // Create join function
  auto join_func = std::make_unique<JoinFunction>(
      "DataModeJoin",
      [](std::unique_ptr<VectorRecord>& left,
         std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        auto lv = extractFloatVector(*left);
        auto rv = extractFloatVector(*right);
        std::vector<float> out;
        out.reserve(lv.size() + rv.size());
        out.insert(out.end(), lv.begin(), lv.end());
        out.insert(out.end(), rv.begin(), rv.end());
        uint64_t id = left->uid_ * 1000000 + right->uid_ % 1000000;
        int64_t ts = std::max(left->timestamp_, right->timestamp_);
        return createVectorRecord(id, ts, out);
    }, mode_config.vector_dim);
  join_func->setWindow(win_ms, mode_config.trig_ms);
  
  // Collect matches
  std::mutex match_mutex;
  std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash> actual_pairs;
  auto sink_func = std::make_unique<SinkFunction>("DataModeSink", [&](std::unique_ptr<VectorRecord>& rec){
      if (!rec) return;
      uint64_t cid = rec->uid_;
      uint64_t lid = cid / 1000000;
      uint64_t rid = cid % 1000000;
      std::lock_guard<std::mutex> lg(match_mutex);
      actual_pairs.insert({lid, rid});
  });
  
  // Build pipeline
  left_source->join(right_source, std::move(join_func), method, mode_config.threshold, (size_t)parallelism)
             ->writeSink(std::move(sink_func), 1);
  
  // Execute
  StreamEnvironment env;
  JoinMetrics::instance().reset();
  env.addStream(left_source);
  env.addStream(right_source);
  
  auto start_time = std::chrono::high_resolution_clock::now();
  env.execute();
  
  // Wait for completion
  {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(600);
    for (;;) {
      uint64_t l = JoinMetrics::instance().total_records_left.load();
      uint64_t r = JoinMetrics::instance().total_records_right.load();
      if (l >= expected_left && r >= expected_right) break;
      if (std::chrono::steady_clock::now() >= deadline) {
        SAGEFLOW_LOG_WARN("TEST", "Timeout waiting for processing: left={}/{} right={}/{}", l, expected_left, r, expected_right);
        break;
      }
      std::this_thread::sleep_for(5ms);
    }
    
    // Wait for output stabilization
    const auto stable_window = 50ms;
    const auto max_wait = std::chrono::seconds(5);
    uint64_t last = JoinMetrics::instance().total_emits.load();
    auto stable_since = std::chrono::steady_clock::now();
    auto end_by = std::chrono::steady_clock::now() + max_wait;
    while (std::chrono::steady_clock::now() < end_by) {
      std::this_thread::sleep_for(5ms);
      uint64_t cur = JoinMetrics::instance().total_emits.load();
      if (cur != last) { last = cur; stable_since = std::chrono::steady_clock::now(); }
      if (std::chrono::steady_clock::now() - stable_since >= stable_window) break;
    }
  }
  
  env.stop();
  env.awaitTermination();
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // Calculate metrics
  size_t match_count = 0;
  for (auto ap : actual_pairs) {
    if (expected_matches.count(ap)) match_count++;
  }
  
  double recall = expected_matches.empty() ? 1.0 : static_cast<double>(match_count) / static_cast<double>(expected_matches.size());
  double precision = actual_pairs.empty() ? 0.0 : static_cast<double>(match_count) / static_cast<double>(actual_pairs.size());
  double f1 = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0.0;
  
  SAGEFLOW_LOG_INFO("TEST", "Result: name={} mode={} method={} size={} parallelism={} time_ms={} matches={}/{} recall={:.3f} precision={:.3f} f1={:.3f}",
                   mode_config.name, mode_config.mode, method, data_size, parallelism, duration.count(), 
                   match_count, expected_matches.size(), recall, precision, f1);
  
  // Write to report file
  try {
    const auto report_dir =
#ifdef PROJECT_DIR
      std::filesystem::path(PROJECT_DIR) / "test" / "result"
#else
      std::filesystem::current_path() / "test" / "result"
#endif
    ;
    std::filesystem::create_directories(report_dir);
    const auto report_path_fs = report_dir / "datasource_modes_report.tsv";
    std::string report_path = report_path_fs.string();
    bool new_file = !std::filesystem::exists(report_path);
    std::ofstream ofs(report_path, std::ios::app);
    if (ofs.is_open()) {
      if (new_file) {
        ofs << "test_name\tmode\tmethod\tsize\tparallelism\twin_ms\ttime_ms\tmatches\texpected\trecall\tprecision\tf1\n";
      }
      ofs << mode_config.name << '\t' << mode_config.mode << '\t' << method << '\t' << data_size << '\t' 
          << parallelism << '\t' << win_ms << '\t' << duration.count() << '\t' << match_count << '\t' 
          << expected_matches.size() << '\t' << recall << '\t' << precision << '\t' << f1 << '\n';
      ofs.flush();
      SAGEFLOW_LOG_INFO("TEST", "Report written to {}", report_path);
    }
  } catch(const std::exception &e) {
    SAGEFLOW_LOG_WARN("TEST", "Failed to write report: {}", e.what());
  }
  
  // Assertions
  EXPECT_GE(recall, 0.85) << "Recall too low for " << mode_config.name;
  EXPECT_GE(precision, 0.85) << "Precision too low for " << mode_config.name;
}

// Generate test parameters
static std::vector<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>> buildTestParams() {
  std::vector<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>> params;
  
  auto configs = loadDataSourceModeConfigs();
  for (const auto& config : configs) {
    for (const auto& method : config.methods) {
      for (int size : config.sizes) {
        for (int par : config.parallelism) {
          for (uint64_t win : config.win_ms_list) {
            params.push_back({config, method, size, par, win});
            SAGEFLOW_LOG_INFO("TEST", "[PARAM] Generated test case: {} mode={} method={} size={} par={} win={}",
                             config.name, config.mode, method, size, par, win);
          }
        }
      }
    }
  }
  
  return params;
}

INSTANTIATE_TEST_SUITE_P(
    DataSourceModes,
    JoinDataSourceModesTest,
    ::testing::ValuesIn(buildTestParams()),
    [](const ::testing::TestParamInfo<std::tuple<DataSourceModeConfig, std::string, int, int, uint64_t>>& info) {
      const DataSourceModeConfig& config = std::get<0>(info.param);
      const std::string& method = std::get<1>(info.param);
      int size = std::get<2>(info.param);
      int parallelism = std::get<3>(info.param);
      uint64_t win_ms = std::get<4>(info.param);
      
      std::string name = config.name + "_" + method + "_" + std::to_string(size) + "_p" + 
                        std::to_string(parallelism) + "_w" + std::to_string(win_ms);
      // Replace invalid characters
      std::replace(name.begin(), name.end(), '/', '_');
      std::replace(name.begin(), name.end(), '.', '_');
      return name;
    }
);

} // namespace test
} // namespace sageFlow
